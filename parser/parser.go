package parser

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

type LogParser interface {
	Name() string
	// parse lines into structured datas
	Parse(lines []string) (datas []Data, err error)
}

type ParserType interface {
	Type() string
}

// conf 字段
const (
	KeyParserName           = GlobalKeyName
	KeyParserType           = "type"
	KeyRunnerName           = "runner_name"
	KeyLabels               = "labels" // 额外增加的标签信息，比如机器信息等
	KeyDisableRecordErrData = "disable_record_errdata"
)

// parser 的类型
const (
	TypeCSV        = "csv"
	TypeLogv1      = "qiniulog"
	TypeKafkaRest  = "kafkarest"
	TypeRaw        = "raw"
	TypeEmpty      = "empty"
	TypeGrok       = "grok"
	TypeInnerSQL   = "_sql"
	TypeInnerMysql = "_mysql"
	TypeJson       = "json"
	TypeNginx      = "nginx"
	TypeSyslog     = "syslog"
)

type Label struct {
	Name  string
	Value string
}

type ParserRegistry struct {
	parserTypeMap map[string]func(conf.MapConf) (LogParser, error)
}

func NewParserRegistry() *ParserRegistry {
	ps := &ParserRegistry{
		parserTypeMap: map[string]func(conf.MapConf) (LogParser, error){},
	}
	ps.RegisterParser(TypeCSV, NewCsvParser)
	ps.RegisterParser(TypeLogv1, NewQiniulogParser)
	ps.RegisterParser(TypeRaw, NewRawlogParser)
	ps.RegisterParser(TypeKafkaRest, NewKafaRestlogParser)
	ps.RegisterParser(TypeEmpty, NewEmptyParser)
	ps.RegisterParser(TypeGrok, NewGrokParser)
	ps.RegisterParser(TypeInnerSQL, NewJsonParser)   //兼容
	ps.RegisterParser(TypeInnerMysql, NewJsonParser) //兼容
	ps.RegisterParser(TypeJson, NewJsonParser)
	ps.RegisterParser(TypeNginx, NewNginxParser)
	ps.RegisterParser(TypeSyslog, NewSyslogParser)
	return ps
}

func (ps *ParserRegistry) RegisterParser(parserType string, constructor func(conf.MapConf) (LogParser, error)) error {
	_, exist := ps.parserTypeMap[parserType]
	if exist {
		return errors.New("parserType " + parserType + " has been existed")
	}
	ps.parserTypeMap[parserType] = constructor
	return nil
}

func (ps *ParserRegistry) NewLogParser(conf conf.MapConf) (p LogParser, err error) {
	t, err := conf.GetString(KeyParserType)
	if err != nil {
		return
	}
	f, exist := ps.parserTypeMap[t]
	if !exist {
		return nil, fmt.Errorf("parser type not supported: %v", t)
	}
	return f(conf)
}
