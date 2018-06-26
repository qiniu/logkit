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

const (
	LogHeadPrefix        = "prefix"
	LogHeadDate          = "date"
	LogHeadTime          = "time"
	LogHeadLevel         = "level"
	LogHeadReqid  string = "reqid"
	LogHeadFile          = "file"
	LogHeadLog           = "log" //默认在最后，不能改变顺序
)

var (
	defaultLogHeads = []string{LogHeadPrefix, LogHeadDate, LogHeadTime, LogHeadReqid, LogHeadLevel, LogHeadFile}
	HeaderPattern   = map[string]string{
		LogHeadDate:  "^[1-9]\\d{3}/[0-1]\\d/[0-3]\\d$",
		LogHeadTime:  "^[0-2]\\d:[0-6]\\d:[0-6]\\d(\\.\\d{6})?$",
		LogHeadReqid: "^\\[\\w+\\]\\[\\w+\\]$",
		LogHeadLevel: "^\\[[A-Z]+\\]$",
		LogHeadFile:  ":\\d+:$",
	}
	CompliedPatterns map[string]*regexp.Regexp
)

func init() {
	CompliedPatterns = make(map[string]*regexp.Regexp)
	for k, v := range HeaderPattern {
		c, _ := regexp.Compile(v)
		CompliedPatterns[k] = c
	}

	parser.RegisterConstructor(parser.TypeLogv1, NewParser)
}

type Parser struct {
	name                 string
	prefix               string
	headers              []string
	labels               []parser.Label
	disableRecordErrData bool
}

func getAllLogv1Heads() map[string]bool {
	return map[string]bool{
		LogHeadDate:  true,
		LogHeadTime:  true,
		LogHeadReqid: true,
		LogHeadLevel: true,
		LogHeadFile:  true,
	}
}

func isExist(source []string, item string) bool {
	for _, v := range source {
		if v == string(item) {
			return true
		}
	}
	return false
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	prefix, _ := c.GetStringOr(parser.KeyQiniulogPrefix, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	logHeaders, _ := c.GetStringListOr(parser.KeyLogHeaders, defaultLogHeads)

	nameMap := make(map[string]struct{})
	for k, _ := range logHeaders {
		nameMap[string(k)] = struct{}{}
	}
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		prefix:               prefix,
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

func (p *Parser) GetParser(head string) (func(string) (string, string, error), error) {
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
	case LogHeadFile:
		return p.parseLogFile, nil
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

func (p *Parser) parsePrefix(line string) (leftline, prefix string, err error) {
	if !strings.HasPrefix(line, p.prefix) {
		err = fmt.Errorf("%v can not find prefix %v", line, p.prefix)
		return
	}
	return strings.TrimPrefix(line, p.prefix), p.prefix, nil
}
func (p *Parser) parseDate(line string) (leftline, date string, err error) {
	date, leftline = getSplitByFirstSpace(line)
	return
}
func (p *Parser) parseTime(line string) (leftline, time string, err error) {
	time, leftline = getSplitByFirstSpace(line)
	return
}

func parseFromBracket(line, leftBracket, rightBracket string) (leftline, thing string, err error) {
	if !strings.HasPrefix(line, leftBracket) {
		err = fmt.Errorf("can not find bracket %v", leftBracket)
		return
	}
	index := strings.Index(line, rightBracket)
	if index < 0 {
		err = fmt.Errorf("can not find bracket %v", leftBracket)
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

func (p *Parser) parseReqid(line string) (leftline, reqid string, err error) {
	req, _ := getSplitByFirstSpace(line)
	if strings.Count(req, "[") < 2 || strings.Count(req, "]") < 2 {
		return line, "", nil
	}
	leftline, reqid, err = parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogHeadReqid, line, err)
		return
	}
	return
}

func (p *Parser) parseLogLevel(line string) (leftline, loglevel string, err error) {
	leftline, loglevel, err = parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogHeadLevel, line, err)
		return
	}
	return
}
func (p *Parser) parseLogFile(line string) (leftline, logFile string, err error) {
	logFile, leftline = getSplitByFirstSpace(line)
	if strings.HasPrefix(logFile, "[") {
		leftline, logFile, err = parseFromBracket(line, "[", "]")
		if err != nil {
			err = errorCanNotParse(LogHeadFile, line, err)
			return
		}
		return
	}
	match := isMatch(CompliedPatterns[LogHeadFile], logFile)
	if match {
		return
	}
	if len(leftline) < 1 {
		err = errorCanNotParse(LogHeadFile, line, fmt.Errorf("no left to parse"))
		return
	}
	leftline = strings.TrimSpace(leftline)
	nextfile, leftline := getSplitByFirstSpace(leftline)
	match = isMatch(CompliedPatterns[LogHeadFile], nextfile)
	if !match {
		err = errorCanNotParse(LogHeadFile, nextfile, fmt.Errorf("pattern %v not match %v", HeaderPattern[LogHeadFile], nextfile))
		return
	}
	logFile += " " + nextfile
	return
}

func isMatch(pattern *regexp.Regexp, raw string) bool {
	return pattern.MatchString(raw)
}

func errorNothingParse(s string, d string) error {
	return fmt.Errorf("there is no left log to parse %v, have parsed %v", s, d)
}
func errorCanNotParse(s string, line string, err error) error {
	return fmt.Errorf("can not parse %v from %v %v", s, line, err)
}

func (p *Parser) parse(line string) (d Data, err error) {
	d = make(Data, len(p.headers)+len(p.labels))
	line = strings.Replace(line, "\n", " ", -1)
	line = strings.Replace(line, "\t", "\\t", -1)
	var result, logdate string
	for _, head := range p.headers {
		line = strings.TrimSpace(line)
		if head == LogHeadPrefix && len(p.prefix) < 1 {
			continue
		}
		if len(line) < 1 {
			return nil, errorNothingParse(head, line)
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
			logdate = result
			continue
		}
		if head == LogHeadTime {
			if len(logdate) > 0 {
				_, zoneValue := times.GetTimeZone()
				result = logdate + " " + result + zoneValue
			}
		}
		d[string(head)] = result
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
