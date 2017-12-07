package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"
)

const (
	LogHeadPrefix string = "prefix"
	LogHeadDate   string = "date"
	LogHeadTime   string = "time"
	LogHeadReqid  string = "reqid"
	LogHeadLevel  string = "level"
	LogHeadFile   string = "file"
	LogHeadLog    string = "log" //默认在最后，不能改变顺序
)

// conf 字段
const (
	KeyQiniulogPrefix = "qiniulog_prefix" //qiniulog的日志前缀
	KeyLogHeaders     = "qiniulog_log_headers"
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
}

type QiniulogParser struct {
	name    string
	prefix  string
	headers []string
	labels  []Label
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

func NewQiniulogParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	prefix, _ := c.GetStringOr(KeyQiniulogPrefix, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	logHeaders, _ := c.GetStringListOr(KeyLogHeaders, defaultLogHeads)

	nameMap := make(map[string]struct{})
	for k, _ := range logHeaders {
		nameMap[string(k)] = struct{}{}
	}
	labels := GetLabels(labelList, nameMap)

	return &QiniulogParser{
		name:    name,
		labels:  labels,
		prefix:  prefix,
		headers: logHeaders,
	}, nil
}

func (p *QiniulogParser) Name() string {
	return p.name
}

func (p *QiniulogParser) Type() string {
	return TypeLogv1
}

func (p *QiniulogParser) GetParser(head string) (func(string) (string, string, error), error) {
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

func (p *QiniulogParser) parsePrefix(line string) (leftline, prefix string, err error) {
	if !strings.HasPrefix(line, p.prefix) {
		err = fmt.Errorf("%v can not find prefix %v", line, p.prefix)
		return
	}
	return strings.TrimPrefix(line, p.prefix), p.prefix, nil
}
func (p *QiniulogParser) parseDate(line string) (leftline, date string, err error) {
	date, leftline = getSplitByFirstSpace(line)
	return
}
func (p *QiniulogParser) parseTime(line string) (leftline, time string, err error) {
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

func (p *QiniulogParser) parseReqid(line string) (leftline, reqid string, err error) {
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

func (p *QiniulogParser) parseLogLevel(line string) (leftline, loglevel string, err error) {
	leftline, loglevel, err = parseFromBracket(line, "[", "]")
	if err != nil {
		err = errorCanNotParse(LogHeadLevel, line, err)
		return
	}
	return
}
func (p *QiniulogParser) parseLogFile(line string) (leftline, logFile string, err error) {
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

func (p *QiniulogParser) parse(line string) (d sender.Data, err error) {
	d = make(sender.Data, len(p.headers)+len(p.labels))
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
func (p *QiniulogParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	se := &utils.StatsError{}
	for _, line := range lines {
		d, err := p.parse(line)
		if err != nil {
			se.AddErrors()
			se.ErrorDetail = err
			continue
		}
		se.AddSuccess()
		datas = append(datas, d)
	}
	return datas, se
}
