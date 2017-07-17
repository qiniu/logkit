package parser

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"

	"github.com/qiniu/log"
	"github.com/vjeantet/grok"
)

const (
	KeyGrokMode               = "grok_mode"     //是否替换\n以匹配多行
	KeyGrokPatterns           = "grok_patterns" // grok 模式串名
	KeyGrokCustomPatternFiles = "grok_custom_pattern_files"
	KeyGrokCustomPatterns     = "grok_custom_patterns"

	KeyTimeZoneOffset = "timezone_offset"
)

const (
	ModeMulti = "multi"
)

const (
	LONG   = "long"
	FLOAT  = "float"
	STRING = "string"
	DATE   = "date"
	DROP   = "drop"
)

const MaxGrokMultiLineBuffer = 64 * 1024 * 1024 // 64MB

const DEFAULT_PATTERNS = `LOGDATE %{DATESTAMP:logdate:date}
REQID %{WORD:reqid}
LOGLEVEL %{WORD:loglevel}
LOGDATA %{%DATA:log}
LOGKIT_LOG %{LOGDATE} \[%{REQID}\] \[%{LOGLEVEL}\] %{LOGDATA}

# 可见，合理的将grok pattern作为中间结果为我们利用，可以逐渐构筑复杂的grok pattern
# 但是使用grok pattern依旧建议使用社区成熟的grok pattern为主，不建议大量自定义grok pattern

DURATION %{NUMBER}[nuµm]?s
RESPONSE_CODE %{NUMBER:response_code:string}
RESPONSE_TIME %{DURATION:response_time_ns:string}
EXAMPLE_LOG \[%{HTTPDATE:ts:date}\] %{NUMBER:myfloat:float} %{RESPONSE_CODE} %{IPORHOST:clientip} %{RESPONSE_TIME}

# Wider-ranging username matching vs. logstash built-in %{USER}
NGUSERNAME [a-zA-Z0-9\.\@\-\+_%]+
NGUSER %{NGUSERNAME}
# Wider-ranging client IP matching
CLIENT (?:%{IPORHOST}|%{HOSTPORT}|::1)

##
## 常见日志匹配方式
##

# apache & nginx logs, this is also known as the "common log format"
#   see https://en.wikipedia.org/wiki/Common_Log_Format
COMMON_LOG_FORMAT %{CLIENT:client_ip} %{NOTSPACE:ident} %{NOTSPACE:auth} \[%{HTTPDATE:ts:date}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:http_version:float})?|%{DATA})" %{NUMBER:resp_code} (?:%{NUMBER:resp_bytes:long}|-)
NGINX_LOG %{IPORHOST:client_ip} %{USER:ident} %{USER:auth} \[%{HTTPDATE:ts:date}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:http_version:float})?|%{DATA})" %{NUMBER:resp_code} (?:%{NUMBER:resp_bytes:long}|-)

PANDORA_NGINX %{NOTSPACE:client_ip} %{USER:ident} %{USER:auth} \[%{HTTPDATE:ts:date}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:http_version:float})?|%{DATA})" %{NUMBER:resp_code} (?:%{NUMBER:resp_bytes:long}|-) (?:%{NUMBER:resp_body_bytes:long}|-) "(?:%{NOTSPACE:referrer}|-)" %{QUOTEDSTRING:agent} %{QUOTEDSTRING:forward_for} %{NOTSPACE:upstream_addr} (%{HOSTNAME:host}|-) (%{NOTSPACE:reqid}) %{NUMBER:resp_time:float}

# Combined log format is the same as the common log format but with the addition
# of two quoted strings at the end for "referrer" and "agent"
#   See Examples at http://httpd.apache.org/docs/current/mod/mod_log_config.html
COMBINED_LOG_FORMAT %{COMMON_LOG_FORMAT} %{QS:referrer} %{QS:agent}

# HTTPD log formats
HTTPD20_ERRORLOG \[%{HTTPDERROR_DATE:timestamp}\] \[%{LOGLEVEL:loglevel}\] (?:\[client %{IPORHOST:clientip}\] ){0,1}%{GREEDYDATA:errormsg}
HTTPD24_ERRORLOG \[%{HTTPDERROR_DATE:timestamp}\] \[%{WORD:module}:%{LOGLEVEL:loglevel}\] \[pid %{POSINT:pid:long}:tid %{NUMBER:tid:long}\]( \(%{POSINT:proxy_errorcode:long}\)%{DATA:proxy_errormessage}:)?( \[client %{IPORHOST:client}:%{POSINT:clientport}\])? %{DATA:errorcode}: %{GREEDYDATA:message}
HTTPD_ERRORLOG %{HTTPD20_ERRORLOG}|%{HTTPD24_ERRORLOG}`

var (
	// matches named captures that contain a modifier.
	//   ie,
	//     %{NUMBER:bytes:long}
	//     %{IPORHOST:clientip:date}
	//     %{HTTPDATE:ts1:float}
	modifierRe = regexp.MustCompile(`%{\w+:(\w+):(long|string|date|float|drop)}`)
	// matches a plain pattern name. ie, %{NUMBER}
	patternOnlyRe = regexp.MustCompile(`%{(\w+)}`)
)

type GrokParser struct {
	name   string
	labels []Label
	mode   string

	timeZoneOffset int

	schemaErr *schemaErr

	Patterns []string // 正式的pattern名称
	// namedPatterns is a list of internally-assigned names to the patterns
	// specified by the user in Patterns.
	// They will look like:
	//   GROK_INTERNAL_PATTERN_0, GROK_INTERNAL_PATTERN_1, etc.
	namedPatterns      []string
	CustomPatterns     string
	CustomPatternFiles []string

	// typeMap is a map of patterns -> 字段名 -> 类型,
	//   ie, {
	//          "%{TESTLOG}":
	//             {
	//                "bytes": "long",
	//                "clientip": "string"
	//             }
	//       }
	typeMap map[string]map[string]string

	// patterns is a map of all of the parsed patterns from CustomPatterns
	// and CustomPatternFiles.
	//   ie, {
	//          "DURATION":      "%{NUMBER}[nuµm]?s"
	//          "RESPONSE_CODE": "%{NUMBER:rc:date}"
	//       }
	patterns map[string]string
	g        *grok.Grok
}

func parseTimeZoneOffset(zoneoffset string) (ret int) {
	zoneoffset = strings.TrimSpace(zoneoffset)
	if zoneoffset == "" {
		return
	}
	mi := false
	if strings.HasPrefix(zoneoffset, "-") {
		mi = true
	}
	zoneoffset = strings.Trim(zoneoffset, "+-")
	i, err := strconv.ParseInt(zoneoffset, 10, 64)
	if err != nil {
		log.Errorf("parse %v error %v, ignore zoneoffset...", zoneoffset, err)
		return
	}
	ret = int(i)
	if mi {
		ret = 0 - ret
	}
	return
}

func NewGrokParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	patterns, err := c.GetStringList(KeyGrokPatterns)
	if err != nil {
		return nil, fmt.Errorf("parse key %v error %v", KeyGrokPatterns, err)
	}
	mode, _ := c.GetStringOr(KeyGrokMode, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	timeZoneOffsetRaw, _ := c.GetStringOr(KeyTimeZoneOffset, "")
	timeZoneOffset := parseTimeZoneOffset(timeZoneOffsetRaw)
	nameMap := make(map[string]struct{})
	labels := GetLabels(labelList, nameMap)

	customPatterns, _ := c.GetStringOr(KeyGrokCustomPatterns, "")
	customPatternFiles, _ := c.GetStringListOr(KeyGrokCustomPatternFiles, []string{})

	p := &GrokParser{
		name:               name,
		labels:             labels,
		mode:               mode,
		Patterns:           patterns,
		CustomPatterns:     customPatterns,
		CustomPatternFiles: customPatternFiles,
		timeZoneOffset:     timeZoneOffset,
		schemaErr: &schemaErr{
			number: 0,
			last:   time.Now(),
		},
	}
	err = p.compile()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *GrokParser) compile() error {
	p.typeMap = make(map[string]map[string]string)
	p.patterns = make(map[string]string)
	gk, err := grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
	if err != nil {
		return err
	}
	p.g = gk

	// Give Patterns fake names so that they can be treated as named
	// "custom patterns"
	p.namedPatterns = make([]string, len(p.Patterns))
	for i, pattern := range p.Patterns {
		name := fmt.Sprintf("GROK_INTERNAL_PATTERN_%d", i)
		p.CustomPatterns += "\n" + name + " " + pattern + "\n"
		p.namedPatterns[i] = "%{" + name + "}"
	}

	// Combine user-supplied CustomPatterns with DEFAULT_PATTERNS and parse
	// them together as the same type of pattern.
	p.CustomPatterns = DEFAULT_PATTERNS + p.CustomPatterns
	if len(p.CustomPatterns) != 0 {
		scanner := bufio.NewScanner(strings.NewReader(p.CustomPatterns))
		p.addCustomPatterns(scanner)
	}

	// Parse any custom pattern files supplied.
	for _, filename := range p.CustomPatternFiles {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(bufio.NewReader(file))
		p.addCustomPatterns(scanner)
	}

	return p.compileCustomPatterns()
}

func (gp *GrokParser) Name() string {
	return gp.name
}

func (gp *GrokParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	se := &utils.StatsError{}
	for idx, line := range lines {
		data, err := gp.parseLine(line)
		if err != nil {
			gp.schemaErr.Output(err)
			se.AddErrors()
			se.ErrorIndex = append(se.ErrorIndex, idx)
			continue
		}
		if len(data) < 1 { //数据不为空的时候发送
			continue
		}
		log.Debugf("D! parse result(%v)", data)
		datas = append(datas, data)
		se.AddSuccess()
	}
	return datas, se
}

func (p *GrokParser) parseLine(line string) (sender.Data, error) {
	if p.mode == ModeMulti {
		line = strings.Replace(line, "\n", " ", -1)
	}
	var err error
	var values map[string]string
	var patternName string
	for _, pattern := range p.namedPatterns {
		if values, err = p.g.Parse(pattern, line); err != nil {
			log.Debugf("E! %v", err)
			return nil, err
		}
		if len(values) != 0 {
			patternName = pattern
			break
		}
	}
	if len(values) < 1 {
		log.Errorf("%v no value was parsed after grok pattern %v", line, p.Patterns)
		return nil, nil
	}
	data := sender.Data{}
	for k, v := range values {
		if k == "" || v == "" {
			continue
		}

		// t is the modifier of the field
		var t string
		// check if pattern has some modifiers
		if types, ok := p.typeMap[patternName]; ok {
			t = types[k]
		}

		// if we didn't find a type OR timestamp modifier, assume string
		if t == "" {
			t = STRING
		}

		switch t {
		case LONG:
			iv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Warnf("E! Error parsing %s to long: %s, ignore this field...", v, err)
			} else {
				data[k] = iv
			}
		case FLOAT:
			fv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				log.Warnf("E! Error parsing %s to float: %s, ignore this field...", v, err)
			} else {
				data[k] = fv
			}
		case DATE:
			ts, err := times.StrToTime(v)
			if err == nil {
				ts = ts.Add(time.Duration(p.timeZoneOffset) * time.Hour)
				rfctime := ts.Format(time.RFC3339Nano)
				data[k] = rfctime
			} else {
				log.Warnf("E! Error parsing %s to time layout [%s]: %s, ignore this field...", v, t, err)
			}

		case DROP:
		// goodbye!
		default: //default is STRING
			data[k] = strings.Trim(v, `"`)
		}
	}

	if len(data) <= 0 {
		return data, fmt.Errorf("all data was ignored in this line? Check WARN log and fix your grok pattern")
	}

	for _, l := range p.labels {
		data[l.name] = l.dataValue
	}
	return data, nil
}

func (p *GrokParser) addCustomPatterns(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 && line[0] != '#' {
			names := strings.SplitN(line, " ", 2)
			p.patterns[names[0]] = names[1]
		}
	}
}

func (p *GrokParser) compileCustomPatterns() error {
	var err error
	// check if the pattern contains a subpattern that is already defined
	// replace it with the subpattern for modifier inheritance.
	for i := 0; i < 2; i++ {
		for name, pattern := range p.patterns {
			subNames := patternOnlyRe.FindAllStringSubmatch(pattern, -1)
			for _, subName := range subNames {
				if subPattern, ok := p.patterns[subName[1]]; ok {
					pattern = strings.Replace(pattern, subName[0], subPattern, 1)
				}
			}
			p.patterns[name] = pattern
		}
	}

	// check if pattern contains modifiers. Parse them out if it does.
	for name, pattern := range p.patterns {
		if modifierRe.MatchString(pattern) {
			// this pattern has modifiers, so parse out the modifiers
			pattern, err = p.parseTypedCaptures(name, pattern)
			if err != nil {
				return err
			}
			p.patterns[name] = pattern
		}
	}

	return p.g.AddPatternsFromMap(p.patterns)
}

// parseTypedCaptures parses the capture modifiers, and then deletes the
// modifier from the line so that it is a valid "grok" pattern again.
func (p *GrokParser) parseTypedCaptures(name, pattern string) (string, error) {
	matches := modifierRe.FindAllStringSubmatch(pattern, -1)

	// grab the name of the capture pattern
	patternName := "%{" + name + "}"
	// create type map for this pattern
	p.typeMap[patternName] = make(map[string]string)

	// boolean to verify that each pattern only has a single ts- data type.
	for _, match := range matches {

		p.typeMap[patternName][match[1]] = match[2]

		// the modifier is not a valid part of a "grok" pattern, so remove it
		// from the pattern.
		pattern = strings.Replace(pattern, ":"+match[2]+"}", "}", 1)
	}

	return pattern, nil
}
