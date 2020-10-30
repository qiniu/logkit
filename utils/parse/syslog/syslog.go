package syslog

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/jeromer/syslogparser"
	"github.com/jeromer/syslogparser/rfc3164"
	"github.com/jeromer/syslogparser/rfc5424"
)

const (
	DetectedRFC3164 = iota
	DetectedRFC5424 = iota
	DetectedRFC6587 = iota
	DetectedLeftLog = iota
)

var MessageFacilities = map[int]string{
	0:  "kernel messages",
	1:  "user-level messages",
	2:  "mail system",
	3:  "system daemons",
	4:  "security/authorization messages (note 1)",
	5:  "messages generated internally by syslogd",
	6:  "line printer subsystem",
	7:  "network news subsystem",
	8:  "UUCP subsystem",
	9:  "clock daemon (note 2)",
	10: "security/authorization messages (note 1)",
	11: "FTP daemon",
	12: "NTP subsystem",
	13: "log audit (note 1)",
	14: "log alert (note 1)",
	15: "clock daemon (note 2)",
	16: "local use 0  (local0)",
	17: "local use 1  (local1)",
	18: "local use 2  (local2)",
	19: "local use 3  (local3)",
	20: "local use 4  (local4)",
	21: "local use 5  (local5)",
	22: "local use 6  (local6)",
	23: "local use 7  (local7)",
}

var MessageSeverities = map[int]string{
	0: "Emergency: system is unusable",
	1: "Alert: action must be taken immediately",
	2: "Critical: critical conditions",
	3: "Error: error conditions",
	4: "Warning: warning conditions",
	5: "Notice: normal but significant condition",
	6: "Informational: informational messages",
	7: "Debug: debug-level messages",
}

type LogParts map[string]interface{}

type Parser interface {
	Parse() error
	Dump() LogParts
	Location(*time.Location)
	NeedModifyTime() bool
}

type Format interface {
	GetParser([]byte) Parser
	IsNewLine(data []byte) bool
}

type parserWrapper struct {
	syslogparser.LogParser
	bool
}

func (w *parserWrapper) Dump() LogParts {
	return LogParts(w.LogParser.Dump())
}

func (w *parserWrapper) NeedModifyTime() bool {
	return w.bool
}

func DetectType(data []byte) (detected int) {
	// all formats have a sapce somewhere
	if i := bytes.IndexByte(data, ' '); i > 0 {
		pLength := data[0:i]
		if _, err := strconv.Atoi(string(pLength)); err == nil {
			return DetectedRFC6587
		}
		if len(data) < 1 || data[0] != '<' {
			return DetectedLeftLog
		}
		// 开头由一对尖括号组成 <12>
		angle := bytes.IndexByte(data, '>')
		if (angle < 0) || (angle >= i) {
			return DetectedLeftLog
		}

		//中间是0-9
		for j := 1; j < angle; j++ {
			if data[j] < '0' || data[j] > '9' {
				return DetectedLeftLog
			}
		}

		// <1>1 尖括号后紧跟数字的是RFC5424
		// 否则是 RFC3164
		if (angle+2 == i) && (data[angle+1] >= '0') && (data[angle+1] <= '9') {
			return DetectedRFC5424
		} else {
			return DetectedRFC3164
		}
	}
	return DetectedLeftLog
}

func GetFormat(format string, parseYear bool) Format {
	switch strings.ToLower(format) {
	case "rfc3164":
		return &RFC3164{parseYear}
	case "rfc5424":
		return &RFC5424{}
	case "rfc6587":
		return &RFC6587{}
	}
	return &Automatic{parseYear}
}

type RFC6587 struct{}

func (f *RFC6587) GetParser(line []byte) Parser {
	return &parserWrapper{rfc5424.NewParser(line), false}
}

func (f *RFC6587) IsNewLine(data []byte) bool {
	if i := bytes.IndexByte(data, ' '); i > 0 {
		pLength := data[0:i]
		_, err := strconv.Atoi(string(pLength))
		if err != nil {
			if string(data[0:1]) == "<" {
				// Assume this frame uses non-transparent-framing
				return true
			}
			return false
		}
		return true
	}
	return false
}

type RFC5424 struct{}

func (f *RFC5424) GetParser(line []byte) Parser {
	return &parserWrapper{rfc5424.NewParser(line), false}
}

func (f *RFC5424) IsNewLine(data []byte) bool {
	// all formats have a sapce somewhere
	if i := bytes.IndexByte(data, ' '); i > 0 {
		if len(data) < 1 || data[0] != '<' {
			return false
		}
		// 开头由一对尖括号组成 <12>
		angle := bytes.IndexByte(data, '>')
		if (angle < 0) || (angle >= i) {
			return false
		}
		// <1>1 尖括号后紧跟数字的是RFC5424
		// 否则是 RFC3164
		if (angle+2 == i) && (data[angle+1] >= '0') && (data[angle+1] <= '9') {
			return true
		}
		return false
	}
	return false
}

type RFC3164 struct {
	parseYear bool
}

func (f *RFC3164) GetParser(line []byte) Parser {
	return &parserWrapper{rfc3164.NewParser(line, f.parseYear), true}
}

func (f *RFC3164) IsNewLine(data []byte) bool {
	// pri: 1.the first char must be '<'; 2. the third,fourth or fifth char must be '>'; 3. between'<' and '>' are numbers
	if 0 < len(data) && data[0] != '<' {
		return false
	}
	for j := 1; j < len(data) && j < 5; j++ {
		if j > 1 && data[j] == '>' {
			return true
		}
		if data[j] < '0' || data[j] > '9' {
			return false
		}
	}
	return false
}

type Automatic struct {
	parseYear bool
}

func (f *Automatic) GetParser(line []byte) Parser {
	switch format := DetectType(line); format {
	case DetectedRFC3164:
		return &parserWrapper{rfc3164.NewParser(line, f.parseYear), true}
	case DetectedRFC5424:
		return &parserWrapper{rfc5424.NewParser(line), false}
	case DetectedRFC6587:
		return &parserWrapper{rfc5424.NewParser(line), false}
	default:
		return &parserWrapper{rfc3164.NewParser(line, f.parseYear), false}
	}
}

func (f *Automatic) IsNewLine(data []byte) bool {
	switch format := DetectType(data); format {
	case DetectedRFC6587, DetectedRFC3164, DetectedRFC5424:
		return true
	}
	return false
}
