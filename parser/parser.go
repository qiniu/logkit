package parser

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

type Parser interface {
	Name() string
	// parse lines into structured datas
	Parse(lines []string) (datas []Data, err error)
}

type ServerParser interface {
	ServerConfig() map[string]interface{}
}

type ParserType interface {
	Type() string
}

type Flushable interface {
	Flush() (Data, error)
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
		return nil, err
	}
	f, exist := ps.parserTypeMap[t]
	if !exist {
		return nil, fmt.Errorf("parser type unsupported: %v", t)
	}
	return f(conf)
}

func ParseLine(dataPipeline <-chan ParseInfo, resultChan chan ParseResult, wg *sync.WaitGroup,
	trimSpace bool, handlerFunc func(string) (Data, error)) {
	for parseInfo := range dataPipeline {
		if trimSpace {
			line := strings.TrimSpace(parseInfo.Line)
			if len(line) <= 0 {
				resultChan <- ParseResult{
					Line:  line,
					Index: parseInfo.Index,
				}
				continue
			}
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

func ParseLineDataSlice(dataPipeline <-chan ParseInfo, resultChan chan ParseResult, wg *sync.WaitGroup,
	trimSpace bool, handlerFunc func(string) ([]Data, error)) {
	for parseInfo := range dataPipeline {
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
