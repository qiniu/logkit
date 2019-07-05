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
	detectedRFC3164 = iota
	detectedRFC5424 = iota
	detectedRFC6587 = iota
	detectedLeftLog = iota
)

type LogParts map[string]interface{}

type Parser interface {
	Parse() error
	Dump() LogParts
	Location(*time.Location)
}

type Format interface {
	GetParser([]byte) Parser
	IsNewLine(data []byte) bool
}

type parserWrapper struct {
	syslogparser.LogParser
}

func (w *parserWrapper) Dump() LogParts {
	return LogParts(w.LogParser.Dump())
}

func DetectType(data []byte) (detected int) {
	// all formats have a sapce somewhere
	if i := bytes.IndexByte(data, ' '); i > 0 {
		pLength := data[0:i]
		if _, err := strconv.Atoi(string(pLength)); err == nil {
			return detectedRFC6587
		}
		if len(data) < 1 || data[0] != '<' {
			return detectedLeftLog
		}
		// 开头由一对尖括号组成 <12>
		angle := bytes.IndexByte(data, '>')
		if (angle < 0) || (angle >= i) {
			return detectedLeftLog
		}

		//中间是0-9
		for j := 1; j < angle; j++ {
			if data[j] < '0' || data[j] > '9' {
				return detectedLeftLog
			}
		}

		// <1>1 尖括号后紧跟数字的是RFC5424
		// 否则是 RFC3164
		if (angle+2 == i) && (data[angle+1] >= '0') && (data[angle+1] <= '9') {
			return detectedRFC5424
		} else {
			return detectedRFC3164
		}
	}
	return detectedLeftLog
}

func GetFormt(format string) Format {
	switch strings.ToLower(format) {
	case "rfc3164":
		return &RFC3164{}
	case "rfc5424":
		return &RFC5424{}
	case "rfc6587":
		return &RFC6587{}
	}
	return &Automatic{}
}

type RFC6587 struct{}

func (f *RFC6587) GetParser(line []byte) Parser {
	return &parserWrapper{rfc5424.NewParser(line)}
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
	return &parserWrapper{rfc5424.NewParser(line)}
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

type RFC3164 struct{}

func (f *RFC3164) GetParser(line []byte) Parser {
	return &parserWrapper{rfc3164.NewParser(line)}
}

func (f *RFC3164) IsNewLine(data []byte) bool {
	if i := bytes.IndexByte(data, ' '); i > 1 {
		if string(data[0:1]) != "<" || string(data[i-1:i]) != ">" {
			return false
		}
		return true
	}
	return false
}

type Automatic struct{}

func (f *Automatic) GetParser(line []byte) Parser {
	switch format := DetectType(line); format {
	case detectedRFC3164:
		return &parserWrapper{rfc3164.NewParser(line)}
	case detectedRFC5424:
		return &parserWrapper{rfc5424.NewParser(line)}
	default:
		return &parserWrapper{rfc3164.NewParser(line)}
	}
}

func (f *Automatic) IsNewLine(data []byte) bool {
	switch format := DetectType(data); format {
	case detectedRFC6587, detectedRFC3164, detectedRFC5424:
		return true
	}
	return false
}
