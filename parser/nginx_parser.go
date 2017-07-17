package parser

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"
)

const (
	NginxSchema    string = "nginx_schema"
	NginxConfPath         = "nginx_log_format_path"
	NginxLogFormat        = "nginx_log_format_name"
)

type NginxParser struct {
	name   string
	conf   string
	format string
	regexp *regexp.Regexp
	schema map[string]string
	labels []Label
}

func NewNginxParser(c conf.MapConf) (LogParser, error) {
	nginxParser, err := NewNginxAccParser(c)
	return nginxParser, err
}

func NewNginxAccParser(c conf.MapConf) (*NginxParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")

	nginxConfPath, err := c.GetString(NginxConfPath)
	if err != nil {
		return nil, err
	}
	formatName, err := c.GetString(NginxLogFormat)
	if err != nil {
		return nil, err
	}
	schema, _ := c.GetString(NginxSchema)
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	nameMap := make(map[string]struct{})
	labels := GetLabels(labelList, nameMap)

	p := &NginxParser{
		name:   name,
		conf:   nginxConfPath,
		format: formatName,
		labels: labels,
	}
	p.schema = p.parseSchemaFields(schema)
	re, err := getRegexp(nginxConfPath, formatName)
	if err != nil {
		return nil, err
	}
	p.regexp = re
	return p, nil
}

func (p *NginxParser) parseSchemaFields(schema string) (m map[string]string) {
	fieldMap := make(map[string]string)
	if schema == "" {
		return fieldMap
	}
	schema = strings.Replace(schema, " ", "", -1)
	schemas := strings.Split(schema, ",")

	for _, s := range schemas {
		kTpye := strings.Split(s, ":")
		fieldMap[kTpye[0]] = kTpye[1]
	}
	return fieldMap
}

func (p *NginxParser) Name() string {
	return p.name
}

func (p *NginxParser) Parse(lines []string) ([]sender.Data, error) {
	var ret []sender.Data
	se := &utils.StatsError{}
	for _, line := range lines {
		data, err := p.parseline(line)
		if err != nil {
			se.ErrorDetail = err
			se.AddErrors()
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

func (p *NginxParser) parseline(line string) (sender.Data, error) {
	line = strings.Trim(line, "\n")
	re := p.regexp
	fields := re.FindStringSubmatch(line)
	if fields == nil {
		return nil, fmt.Errorf("NginxParser fail to parse log line [%v], given format is [%v]", line, re)
	}
	entry := make(sender.Data)
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

func (p *NginxParser) makeValue(name, raw string) (data interface{}, err error) {
	valueType := p.schema[name]
	switch CsvType(valueType) {
	case TypeFloat:
		if raw == "-" {
			return 0.0, nil
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			err = fmt.Errorf("convet for %q to float64 failed: %q", name, raw)
		}
		return v, err
	case TypeLong:
		if raw == "-" {
			return 0, nil
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			err = fmt.Errorf("convet for %q to int64 failed, %q", name, raw)
		}
		return v, err
	case TypeString:
		return raw, nil
	case TypeDate:
		tm, nerr := times.StrToTime(raw)
		if nerr != nil {
			return tm, nerr
		}
		data = tm.Format(time.RFC3339Nano)
		return
	default:
		return raw, nil
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
	re := regexp.MustCompile(`\\\$([a-z_]+)(\\?(.))`).ReplaceAllString(
		regexp.QuoteMeta(format+" "), "(?P<$1>[^$3]*)$2")
	return regexp.MustCompile(fmt.Sprintf("^%v$", strings.Trim(re, " "))), nil
}
