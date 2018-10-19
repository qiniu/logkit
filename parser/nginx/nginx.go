package nginx

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	numRoutine           int
	keepRawData          bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	return NewNginxAccParser(c)
}

func NewNginxAccParser(c conf.MapConf) (p *Parser, err error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	schema, _ := c.GetStringOr(parser.NginxSchema, "")
	nginxRegexStr, _ := c.GetStringOr(parser.NginxFormatRegex, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	keepRawData, _ := c.GetBoolOr(parser.KeyKeepRawData, false)
	nameMap := make(map[string]struct{})
	labels := parser.GetLabels(labelList, nameMap)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	p = &Parser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
		numRoutine:           numRoutine,
		keepRawData:          keepRawData,
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
		re, err := ResolveRegexpFromConf(nginxConfPath, formatName)
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
	datas := make([]Data, 0, len(lines))
	se := &StatsError{}

	numRoutine := p.numRoutine
	if len(lines) < numRoutine {
		numRoutine = len(lines)
	}
	sendChan := make(chan parser.ParseInfo)
	resultChan := make(chan parser.ParseResult)

	wg := new(sync.WaitGroup)
	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go parser.ParseLine(sendChan, resultChan, wg, true, p.parse)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, line := range lines {
			sendChan <- parser.ParseInfo{
				Line:  line,
				Index: idx,
			}
		}
		close(sendChan)
	}()

	var parseResultSlice = make(parser.ParseResultSlice, 0, len(lines))
	for resultInfo := range resultChan {
		parseResultSlice = append(parseResultSlice, resultInfo)
	}
	if numRoutine > 1 {
		sort.Stable(parseResultSlice)
	}

	for _, parseResult := range parseResultSlice {
		if len(parseResult.Line) == 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, parseResult.Index)
			continue
		}

		if parseResult.Err != nil {
			se.AddErrors()
			se.LastError = parseResult.Err.Error()
			errData := make(Data)
			if !p.disableRecordErrData {
				errData[KeyPandoraStash] = parseResult.Line
			} else if !p.keepRawData {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, parseResult.Index)
			}
			if p.keepRawData {
				errData[parser.KeyRawData] = parseResult.Line
			}
			if !p.disableRecordErrData || p.keepRawData {
				datas = append(datas, errData)
			}
			continue
		}
		if len(parseResult.Data) < 1 { //数据为空时不发送
			se.LastError = fmt.Sprintf("parsed no data by line [%v]", parseResult.Line)
			se.AddErrors()
			continue
		}
		se.AddSuccess()
		log.Debugf("D! parse result(%v)", parseResult.Data)
		if p.keepRawData {
			parseResult.Data[parser.KeyRawData] = parseResult.Line
		}
		datas = append(datas, parseResult.Data)
	}

	if se.Errors == 0 {
		return datas, nil
	}
	return datas, se
}

func (p *Parser) parse(line string) (Data, error) {
	line = strings.Trim(line, "\n")
	re := p.regexp
	fields := re.FindStringSubmatch(line)
	if fields == nil {
		return nil, fmt.Errorf("NginxParser fail to parse log line [%v], given format is [%v]", TruncateStrSize(line, DefaultTruncateMaxSize), re)
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

var (
	formatRegexp    = regexp.MustCompile(`^\s*log_format\s+(\S+)+\s+(.+)\s*$`)
	formatEndRegexp = regexp.MustCompile(`^\s*(.*?)\s*(;|$)`)
	replaceRegexp   = regexp.MustCompile(`\\\$([a-z_]+)(\\?(.))`)
)

// ResolveRegexpFromConf 根据给定配置文件和日志格式名称返回自动生成的匹配正则表达式
func ResolveRegexpFromConf(confPath, name string) (*regexp.Regexp, error) {
	f, err := os.Open(confPath)
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	reTmp, err := regexp.Compile(fmt.Sprintf(`^\s*log_format\s+%v\s+(.+)\s*$`, name))
	if err != nil {
		return nil, fmt.Errorf("compile log format regexp: %v", err)
	}

	found := false
	var format string
	for scanner.Scan() {
		line := scanner.Text()
		if !found {
			// Find a log_format definition
			formatDef := reTmp.FindStringSubmatch(line)
			if formatDef == nil {
				continue
			}
			found = true
			line = formatDef[1]
		}

		// Look for a definition end
		lineSplit := formatEndRegexp.FindStringSubmatch(line)
		if l := len(lineSplit[1]); l > 2 {
			format += lineSplit[1][1 : l-1]
		}
		if lineSplit[2] == ";" {
			break
		}
	}
	if scanner.Err() != nil {
		return nil, scanner.Err()
	} else if !found {
		return nil, fmt.Errorf("`log_format %v` not found in given config", name)
	}

	restr := replaceRegexp.ReplaceAllString(regexp.QuoteMeta(format+" "), "(?P<$1>[^$3]*)$2")
	return regexp.Compile(fmt.Sprintf("^%v$", strings.Trim(restr, " ")))
}

// FindAllRegexpsFromConf 根据给定配置文件返回所有可能的日志格式与匹配正则表达式的组合
func FindAllRegexpsFromConf(confPath string) (map[string]*regexp.Regexp, error) {
	f, err := os.Open(confPath)
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	defer f.Close()

	patterns := make(map[string]*regexp.Regexp)
	found := false

	var name, format string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// 如果当前并未找到任何匹配，则尝试匹配
		if !found {
			formatDef := formatRegexp.FindStringSubmatch(line)
			if formatDef == nil {
				continue
			}
			found = true
			name = formatDef[1]
			line = formatDef[2]
		}

		lineSplit := formatEndRegexp.FindStringSubmatch(line)
		if l := len(lineSplit[1]); l > 2 {
			format += lineSplit[1][1 : l-1]
		}
		if lineSplit[2] == ";" {
			restr := replaceRegexp.ReplaceAllString(regexp.QuoteMeta(format+" "), "(?P<$1>[^$3]*)$2")
			re, err := regexp.Compile(fmt.Sprintf("^%v$", strings.Trim(restr, " ")))
			if err != nil {
				return nil, fmt.Errorf("compile log format regexp: %v", err)
			}
			patterns[name] = re
			found = false
			name = ""
			format = ""
			continue
		}
	}

	return patterns, nil
}
