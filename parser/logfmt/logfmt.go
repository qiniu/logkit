package logfmt

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/go-logfmt/logfmt"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(parser.TypeLogfmt, NewParser)
}

// Parser decodes logfmt formatted messages into metrics.
type Parser struct {
	name                 string
	disableRecordErrData bool
	numRoutine           int
}

// NewParser creates a parser.
func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	return &Parser{
		name:                 name,
		disableRecordErrData: disableRecordErrData,
		numRoutine:           numRoutine,
	}, nil
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
		go parser.ParseLineDataSlice(sendChan, resultChan, wg, true, p.parse)
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
			se.ErrorDetail = parseResult.Err
			if !p.disableRecordErrData {
				datas = append(datas, Data{
					KeyPandoraStash: parseResult.Line,
				})
			} else {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, parseResult.Index)
			}
			continue
		}
		if len(parseResult.Datas) == 0 { //数据为空时不发送
			se.ErrorDetail = fmt.Errorf("parsed no data by line [%v]", parseResult.Line)
			se.AddErrors()
			continue
		}

		se.AddSuccess()
		datas = append(datas, parseResult.Datas...)
	}

	return datas, se
}

// Parse converts a slice of line in logfmt format to metrics.
func (p *Parser) parse(line string) ([]Data, error) {
	reader := bytes.NewReader([]byte(line))
	decoder := logfmt.NewDecoder(reader)
	datas := make([]Data, 0)
	for {
		ok := decoder.ScanRecord()
		if !ok {
			err := decoder.Err()
			if err != nil {
				return nil, err
			}
			break
		}
		fields := make(Data)
		for decoder.ScanKeyval() {
			if string(decoder.Value()) == "" {
				continue
			}

			//type conversions
			value := string(decoder.Value())
			if fValue, err := strconv.ParseFloat(value, 64); err == nil {
				fields[string(decoder.Key())] = fValue
			} else if bValue, err := strconv.ParseBool(value); err == nil {
				fields[string(decoder.Key())] = bValue
			} else {
				fields[string(decoder.Key())] = value
			}
		}
		if len(fields) == 0 {
			continue
		}

		datas = append(datas, fields)
	}
	return datas, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return parser.TypeLogfmt
}
