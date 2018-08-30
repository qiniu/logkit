package parser

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

type Parser interface {
	Name() string
	// parse lines into structured datas
	Parse(lines []string) (datas []Data, err error)
}

type ParserType interface {
	Type() string
}

type Flushable interface {
	Flush() (Data, error)
}

// conf 字段
const (
	KeyParserName           = GlobalKeyName
	KeyParserType           = "type"
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
	TypeInnerMySQL = "_mysql"
	TypeJSON       = "json"
	TypeNginx      = "nginx"
	TypeSyslog     = "syslog"
	TypeMySQL      = "mysqllog"
	TypeLogfmt     = "logfmt"
)

// 数据常量类型
type DataType string

const (
	TypeFloat   DataType = "float"
	TypeLong    DataType = "long"
	TypeString  DataType = "string"
	TypeDate    DataType = "date"
	TypeJSONMap DataType = "jsonmap"
)

type Label struct {
	Name  string
	Value string
}

type ParseInfo struct {
	Line  string
	Index int
}

type ParseResult struct {
	Line  string
	Index int
	Data  Data
	Datas []Data
	Err   error
}

type ParseResultSlice []ParseResult

func (slice ParseResultSlice) Len() int {
	return len(slice)
}

func (slice ParseResultSlice) Less(i, j int) bool {
	return slice[i].Index < slice[j].Index
}

func (slice ParseResultSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type Constructor func(conf.MapConf) (Parser, error)

// registeredConstructors keeps a list of all available reader constructors can be registered by Registry.
var registeredConstructors = map[string]Constructor{}

// RegisterConstructor adds a new constructor for a given type of reader.
func RegisterConstructor(typ string, c Constructor) {
	registeredConstructors[typ] = c
}

type Registry struct {
	parserTypeMap map[string]func(conf.MapConf) (Parser, error)
}

func NewRegistry() *Registry {
	ret := &Registry{
		parserTypeMap: map[string]func(conf.MapConf) (Parser, error){},
	}

	for typ, c := range registeredConstructors {
		ret.RegisterParser(typ, c)
	}

	return ret
}

func (ps *Registry) RegisterParser(parserType string, constructor func(conf.MapConf) (Parser, error)) error {
	_, exist := ps.parserTypeMap[parserType]
	if exist {
		return errors.New("parserType " + parserType + " has been existed")
	}
	ps.parserTypeMap[parserType] = constructor
	return nil
}

func (ps *Registry) NewLogParser(conf conf.MapConf) (p Parser, err error) {
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

func ParseLine(dataPipline <-chan ParseInfo, resultChan chan ParseResult, wg *sync.WaitGroup,
	trimSpace bool, handlerFunc func(string) (Data, error)) {
	for parseInfo := range dataPipline {
		if trimSpace {
			parseInfo.Line = strings.TrimSpace(parseInfo.Line)
		}
		if len(parseInfo.Line) <= 0 {
			resultChan <- ParseResult{
				Line:  parseInfo.Line,
				Index: parseInfo.Index,
			}
			continue
		}

		data, err := handlerFunc(parseInfo.Line)
		resultChan <- ParseResult{
			Line:  parseInfo.Line,
			Index: parseInfo.Index,
			Data:  data,
			Err:   err,
		}
	}
	wg.Done()
}

func ParseLineDataSlice(dataPipline <-chan ParseInfo, resultChan chan ParseResult, wg *sync.WaitGroup,
	trimSpace bool, handlerFunc func(string) ([]Data, error)) {
	for parseInfo := range dataPipline {
		if trimSpace {
			parseInfo.Line = strings.TrimSpace(parseInfo.Line)
		}
		if len(parseInfo.Line) <= 0 {
			resultChan <- ParseResult{
				Line:  parseInfo.Line,
				Index: parseInfo.Index,
			}
			continue
		}

		datas, err := handlerFunc(parseInfo.Line)
		resultChan <- ParseResult{
			Line:  parseInfo.Line,
			Index: parseInfo.Index,
			Datas: datas,
			Err:   err,
		}
	}
	wg.Done()
}
