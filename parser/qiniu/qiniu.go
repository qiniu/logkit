package qiniu

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

/*
理论上这个parser可以解析 七牛的 xlog，log，teapot这三类日志
*/

const (
	LogHeadPrefix        = "prefix"
	LogHeadDate          = "date"
	LogHeadTime          = "time"
	LogHeadReqid  string = "reqid"
	LogHeadLevel         = "level"
	LogHeadModule        = "module"
	LogHeadFile          = "file"
	LogHeadLog           = "log" //默认在最后，不能改变顺序

	LogCombinedReqidLevel = "combinedReqidLevel" //reqid和level组合，代表reqid可能有、可能没有，但是如果都有，前者被认定为一定reqid
	LogCombinedModuleFile = "combinedModuleFile" //module和file的组合，代表可能有module，可能没有，如果存在中括号开头就认为是module
)

const (
	LogFilePattern = ":\\d+:$"

	HeadPatthern = `[1-9]\d{3}\/[0-1]\d\/[0-3]\d [0-2]\d:[0-6]\d:[0-6]\d(\.\d{6})? \[`
	KeyPrefix    = "qiniulog_prefix" //qiniulog的日志前缀
)

var (
	defaultLogHeads = []string{LogHeadDate, LogHeadTime, LogCombinedReqidLevel, LogCombinedModuleFile}
	logFilePattern  = regexp.MustCompile(LogFilePattern)
)

func init() {
	parser.RegisterConstructor(parser.TypeLogv1, NewParser)
}

type Parser struct {
	name                 string
	headers              []string
	labels               []parser.Label
	disableRecordErrData bool
}

func checkLevel(str string) bool {
	str = strings.ToUpper(str)
	switch str {
	case "INFO", "DEBUG", "WARN", "ERROR", "PANIC", "FATAL":
		return true
	}
	return false
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	logHeaders, _ := c.GetStringListOr(parser.KeyLogHeaders, defaultLogHeads)
	if len(logHeaders) < 1 {
		return nil, fmt.Errorf("no log headers was configured to parse")
	}

	//兼容老的配置，以前的配置必须要配 KeyPrefix 才能匹配 prefix
	prefix, _ := c.GetStringOr(KeyPrefix, "")
	if len(prefix) > 0 {
		if logHeaders[0] != LogHeadPrefix {
			logHeaders = append([]string{LogHeadPrefix}, logHeaders...)
		}
	}

	nameMap := make(map[string]struct{})
	for k, _ := range logHeaders {
		nameMap[string(k)] = struct{}{}
	}
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		headers:              logHeaders,
		disableRecordErrData: disableRecordErrData,
	}, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return parser.TypeLogv1
}

func (p *Parser) GetParser(head string) (func(string) (string, map[string]string, error), error) {
	switch head {
	case LogHeadPrefix:
		return p.parsePrefix, nil
	case LogHeadDate:
		return p.parseDate, nil
	case LogHeadTime:
		return p.parseTime, nil
	case LogHeadReqid:
		return p.parseReqid, nil
	case LogHeadLevel:
		return p.parseLogLevel, nil
	case LogHeadModule:
		return p.parseModule, nil
	case LogHeadFile:
		//以前的file，就是组合式的解析
		return p.parseCombinedModuleFile, nil
	case LogCombinedReqidLevel:
		return p.parseCombinedReqidLevel, nil
	case LogCombinedModuleFile:
		return p.parseCombinedModuleFile, nil

	}
	return nil, fmt.Errorf("QiniulogParser Loghead <%v> not exist", head)
}

func getSplitByFirstSpace(line string) (firstPart, left string) {
	space := strings.Index(line, " ")
	if space < 0 {
		return line, ""
	}
	return line[0:space], line[space+1:]
}

func (p *Parser) parsePrefix(line string) (string, map[string]string, error) {
	prefix, leftline := getSplitByFirstSpace(line)
	result := map[string]string{LogHeadPrefix: prefix}
	return leftline, result, nil
}

func (p *Parser) parseDate(line string) (string, map[string]string, error) {
	date, leftline := getSplitByFirstSpace(line)
	result := map[string]string{LogHeadDate: date}
	return leftline, result, nil
}

func (p *Parser) parseTime(line string) (string, map[string]string, error) {
	time, leftline := getSplitByFirstSpace(line)
	result := map[string]string{LogHeadTime: time}
	return leftline, result, nil
}

func parseFromBracket(line, leftBracket, rightBracket string) (leftline, thing string, err error) {
	if !strings.HasPrefix(line, leftBracket) {
		err = fmt.Errorf("can not find left bracket %v %s", leftBracket, line)
		return
	}
	index := strings.Index(line, rightBracket)
	if index < 0 {
		err = fmt.Errorf("can not find right bracket %v from log %s", leftBracket, line)
		return
	}
	thing = line[len(leftBracket):index]
	if index+1 >= len(line) {
		leftline = ""
	} else {
		leftline = line[index+1:]
	}
	return
}

func (p *Parser) parseReqid(line string) (string, map[string]string, error) {

	//reqid可以不存在
	if !strings.HasPrefix(line, "[") {
		return line, nil, nil
	}
	//reqid不含双引号，是teapot的file
	if strings.HasPrefix(line, `["`) {
		return line, nil, nil
	}
	leftline, reqid, err := parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogHeadReqid, line, err)
		return line, nil, err
	}
	//确保不是level
	if checkLevel(reqid) {
		return line, nil, nil
	}

	result := map[string]string{LogHeadReqid: reqid}
	return leftline, result, nil
}

func (p *Parser) parseCombinedReqidLevel(line string) (string, map[string]string, error) {
	leftline, firstRes, err := parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogCombinedReqidLevel, line, err)
		return line, nil, err
	}
	result := make(map[string]string)
	var secondRes string
	if strings.HasPrefix(leftline, "[") {
		leftline, secondRes, err = parseFromBracket(leftline, "[", "]")
		if err != nil {
			err = errorCanNotParse(LogCombinedReqidLevel, leftline, err)
			return line, nil, err
		}
		result[LogHeadReqid] = firstRes
		result[LogHeadLevel] = secondRes
	} else {
		result[LogHeadLevel] = firstRes
	}
	return leftline, result, nil
}

func (p *Parser) parseLogLevel(line string) (string, map[string]string, error) {
	leftline, loglevel, err := parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogHeadLevel, line, err)
		return line, nil, err
	}
	result := map[string]string{LogHeadLevel: loglevel}
	return leftline, result, nil
}

func (p *Parser) parseModule(line string) (string, map[string]string, error) {
	if !strings.HasPrefix(line, "[") {
		return line, nil, nil
	}
	leftline, module, err := parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogHeadModule, line, err)
		return line, nil, err
	}
	result := map[string]string{LogHeadModule: module}
	return leftline, result, err
}

func (p *Parser) parseLogFile(line string) (string, map[string]string, error) {
	logFile, leftline := getSplitByFirstSpace(line)
	match := isMatch(logFilePattern, logFile)
	if match {
		result := map[string]string{LogHeadFile: logFile}
		return leftline, result, nil
	}
	if len(leftline) < 1 {
		err := errorCanNotParse(LogHeadFile, line, fmt.Errorf("no left log to parse"))
		return line, nil, err
	}
	leftline = strings.TrimSpace(leftline)
	nextfile, leftline := getSplitByFirstSpace(leftline)
	match = isMatch(logFilePattern, nextfile)
	if !match {
		err := errorCanNotParse(LogHeadFile, nextfile, fmt.Errorf("pattern <%v> not match %v", LogFilePattern, nextfile))
		return line, nil, err
	}
	logFile += " " + nextfile
	result := map[string]string{LogHeadFile: logFile}
	return leftline, result, nil
}

func (p *Parser) parseCombinedModuleFile(line string) (string, map[string]string, error) {
	result := make(map[string]string)
	if strings.HasPrefix(line, "[") {
		leftLine, moduleResult, err := p.parseModule(line)
		if err != nil {
			return leftLine, result, err
		}
		line = strings.TrimSpace(leftLine)
		module := moduleResult[LogHeadModule]
		if strings.HasPrefix(module, `"`) && strings.HasSuffix(module, `"`) {
			//适配teapot日志
			result[LogHeadFile] = strings.Trim(module, `"`)
			return line, result, nil
		} else {
			result[LogHeadModule] = module
		}
	}
	leftLine, logRes, err := p.parseLogFile(line)
	if err != nil {
		if len(result) > 0 {
			return leftLine, result, nil
		}
		return leftLine, result, err
	}
	result[LogHeadFile] = logRes[LogHeadFile]
	return leftLine, result, nil
}

func isMatch(pattern *regexp.Regexp, raw string) bool {
	return pattern.MatchString(raw)
}

func errorCanNotParse(s string, line string, err error) error {
	return fmt.Errorf("can not parse %v from %v %v", s, line, err)
}

func (p *Parser) parse(line string) (d Data, err error) {
	d = make(Data, len(p.headers)+len(p.labels))
	// 不明白为什么之前要把换行和\t干掉，现在注释
	//line = strings.Replace(line, "\n", " ", -1)
	//line = strings.Replace(line, "\t", "\\t", -1)
	var logdate string
	var result map[string]string
	for _, head := range p.headers {
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			break
		}
		// LogHeadLog不需要使用解析器，最后剩下的就是
		if line == LogHeadLog {
			continue
		}
		parser, err := p.GetParser(head)
		if err != nil {
			return nil, err
		}
		line, result, err = parser(line)
		if err != nil {
			return nil, err
		}
		if head == LogHeadDate {
			logdate = result[LogHeadDate]
			continue
		}
		if head == LogHeadTime {
			logTime := result[LogHeadTime]
			if len(logdate) > 0 {
				_, zoneValue := times.GetTimeZone()
				result[LogHeadTime] = logdate + " " + logTime + zoneValue
			}
		}
		for k, v := range result {
			d[k] = v
		}
	}
	line = strings.TrimSpace(line)
	d[LogHeadLog] = line
	for _, l := range p.labels {
		d[l.Name] = l.Value
	}
	return d, nil
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	datas := []Data{}
	se := &StatsError{}
	for idx, line := range lines {
		if len(strings.TrimSpace(line)) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		d, err := p.parse(line)
		if err != nil {
			se.AddErrors()
			se.ErrorDetail = err
			if !p.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				datas = append(datas, errData)
			} else {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			}
			continue
		}
		se.AddSuccess()
		datas = append(datas, d)
	}
	return datas, se
}
