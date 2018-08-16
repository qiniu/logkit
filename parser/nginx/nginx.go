package nginx

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(parser.TypeNginx, NewParser)
}

type Parser struct {
	name                 string
	regexp               *regexp.Regexp
	schema               map[string]string
	labels               []parser.Label
	disableRecordErrData bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	nginxParser, err := NewNginxAccParser(c)
	return nginxParser, err
}

func NewNginxAccParser(c conf.MapConf) (p *Parser, err error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")

	schema, _ := c.GetStringOr(parser.NginxSchema, "")
	nginxRegexStr, _ := c.GetStringOr(parser.NginxFormatRegex, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	nameMap := make(map[string]struct{})
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	p = &Parser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
	}
	p.schema, err = p.parseSchemaFields(schema)
	if err != nil {
		return
	}
	if nginxRegexStr == "" {
		nginxConfPath, err := c.GetString(parser.NginxConfPath)
		if err != nil {
			return nil, err
		}
		formatName, err := c.GetString(parser.NginxLogFormat)
		if err != nil {
			return nil, err
		}
		re, err := getRegexp(nginxConfPath, formatName)
		if err != nil {
			return nil, err
		}
		p.regexp = re
	} else {
		re, err := regexp.Compile(nginxRegexStr)
		if err != nil {
			return nil, fmt.Errorf("Compile nginx_log_format_regex %v error %v", nginxRegexStr, err)
		}
		p.regexp = re
	}
	return p, nil
}

func (p *Parser) parseSchemaFields(schema string) (m map[string]string, err error) {
	fieldMap := make(map[string]string)
	if schema == "" {
		return fieldMap, nil
	}
	schema = strings.Replace(schema, ":", " ", -1)
	schemas := strings.Split(schema, ",")

	for _, s := range schemas {
		parts := strings.Fields(s)
		if len(parts) < 2 {
			err = errors.New("column conf error: " + s + ", format should be \"columnName dataType\"")
			return
		}
		fieldMap[parts[0]] = parts[1]
	}
	return fieldMap, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return parser.TypeNginx
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	var ret []Data
	se := &StatsError{}
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		data, err := p.parseline(line)
		if err != nil {
			se.ErrorDetail = err
			se.AddErrors()
			if !p.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				ret = append(ret, errData)
			} else {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			}
			continue
		}
		if len(data) < 1 { //数据不为空的时候发送
			se.ErrorDetail = fmt.Errorf("parsed no data by line [%v]", line)
			se.AddErrors()
			continue
		}
		se.AddSuccess()
		log.Debugf("D! parse result(%v)", data)
		ret = append(ret, data)
	}
	return ret, se
}

func (p *Parser) parseline(line string) (Data, error) {
	line = strings.Trim(line, "\n")
	re := p.regexp
	fields := re.FindStringSubmatch(line)
	if fields == nil {
		return nil, fmt.Errorf("NginxParser fail to parse log line [%v], given format is [%v]", TruncateStrSize(line), re)
	}
	entry := make(Data)
	// Iterate over subexp group and fill the map record
	for i, name := range re.SubexpNames() {
		if i == 0 {
			continue
		}
		data, err := p.makeValue(name, fields[i])
		if err != nil {
			log.Warnf("Error %v, ignore this key %v ...", err, name)
			continue
		}
		entry[name] = data
	}
	for _, l := range p.labels {
		entry[l.Name] = l.Value
	}
	return entry, nil
}

func (p *Parser) makeValue(name, raw string) (data interface{}, err error) {
	valueType := p.schema[name]
	switch parser.DataType(valueType) {
	case parser.TypeFloat:
		if raw == "-" {
			return 0.0, nil
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			err = fmt.Errorf("convet for %q to float64 failed: %q", name, raw)
		}
		return v, err
	case parser.TypeLong:
		if raw == "-" {
			return 0, nil
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			err = fmt.Errorf("convet for %q to int64 failed, %q", name, raw)
		}
		return v, err
	case parser.TypeString:
		return raw, nil
	case parser.TypeDate:
		tm, nerr := times.StrToTime(raw)
		if nerr != nil {
			return tm, nerr
		}
		data = tm.Format(time.RFC3339Nano)
		return
	default:
		return strings.TrimSpace(raw), nil
	}
}

func getRegexp(conf, name string) (r *regexp.Regexp, err error) {
	f, err := os.Open(conf)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	reTmp := regexp.MustCompile(fmt.Sprintf(`^\s*log_format\s+%v\s+(.+)\s*$`, name))
	found := false
	var format string
	for scanner.Scan() {
		var line string
		if !found {
			// Find a log_format definition
			line = scanner.Text()
			formatDef := reTmp.FindStringSubmatch(line)
			if formatDef == nil {
				continue
			}
			found = true
			line = formatDef[1]
		} else {
			line = scanner.Text()
		}
		// Look for a definition end
		reTmp = regexp.MustCompile(`^\s*(.*?)\s*(;|$)`)
		lineSplit := reTmp.FindStringSubmatch(line)
		if l := len(lineSplit[1]); l > 2 {
			format += lineSplit[1][1 : l-1]
		}
		if lineSplit[2] == ";" {
			break
		}
	}
	if !found {
		err = fmt.Errorf("`log_format %v` not found in given config", name)
	} else {
		err = scanner.Err()
	}
	if err != nil {
		return
	}
	re, err := regexp.Compile(`\\\$([a-z_]+)(\\?(.))`)
	if err != nil {
		return
	}
	restr := re.ReplaceAllString(
		regexp.QuoteMeta(format+" "), "(?P<$1>[^$3]*)$2")
	return regexp.Compile(fmt.Sprintf("^%v$", strings.Trim(restr, " ")))
}
