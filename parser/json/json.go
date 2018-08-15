package json

import (
	"fmt"

	"strings"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"sync"

	"sort"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(parser.TypeInnerSQL, NewParser)
	parser.RegisterConstructor(parser.TypeInnerMySQL, NewParser)
	parser.RegisterConstructor(parser.TypeJSON, NewParser)
}

type Parser struct {
	name                 string
	labels               []parser.Label
	disableRecordErrData bool
	jsontool             jsoniter.API
	routineNumber        int
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	nameMap := map[string]struct{}{}
	labels := parser.GetLabels(labelList, nameMap)
	jsontool := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)
	routineNumber := MaxProcs
	if routineNumber == 0 {
		routineNumber = NumCpu
	}

	return &Parser{
		name:                 name,
		labels:               labels,
		jsontool:             jsontool,
		disableRecordErrData: disableRecordErrData,
		routineNumber:        routineNumber,
	}, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return parser.TypeJSON
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	datas := make([]Data, 0, len(lines))
	se := &StatsError{}
	routineNumber := p.routineNumber
	if len(lines) < routineNumber {
		routineNumber = len(lines)
	}
	sendChan := make(chan parser.ParseInfo)
	resultChan := make(chan parser.ParseResult)

	wg := new(sync.WaitGroup)
	for i := 0; i < routineNumber; i++ {
		wg.Add(1)
		go p.parseLine(sendChan, resultChan, wg)
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

	sort.Stable(parseResultSlice)
	for _, parseResult := range parseResultSlice {
		if len(parseResult.Line) == 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, parseResult.Index)
			continue
		}

		if parseResult.Err != nil {
			log.Debug(parseResult.Err)
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
		if len(parseResult.Data) < 1 && len(parseResult.Datas) < 1 { //数据为空时不发送
			se.ErrorDetail = fmt.Errorf("parsed no data by line [%v]", parseResult.Line)
			se.AddErrors()
			continue
		}
		se.AddSuccess()
		if len(parseResult.Data) == 0 {
			datas = append(datas, parseResult.Datas...)
			continue
		}
	}

	return datas, se
}

func (p *Parser) parse(line string) (data Data, err error) {
	data = make(Data)
	if err = p.jsontool.Unmarshal([]byte(line), &data); err != nil {
		err = fmt.Errorf("parse json line error %v, raw data is: %v", err, TruncateStrSize(line, DefaultTruncateMaxSize))
	if err = p.jsontool.Unmarshal([]byte(line), &data); err != nil {
		err = fmt.Errorf("parse json line error %v, raw data is: %v", err, TruncateStrSize(line, DefaultTruncateMaxSize))
		log.Debug(err)
		return
	}
	for _, l := range p.labels {
		// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
		if _, ok := data[l.Name]; ok {
			continue
		}
		data[l.Name] = l.Value
	}
	return
}

func (p *Parser) parseLineMutiData(line string) (data []Data, err error) {
	data = make([]Data, 0)
	if err = p.jsontool.Unmarshal([]byte(line), &data); err != nil {
		err = fmt.Errorf("parse json line error %v, raw data is: %v", err, line)
		log.Debug(err)
		return
	}
	for i := range data {
		for _, l := range p.labels {
			// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
			if _, ok := data[i][l.Name]; ok {
				continue
			}
			data[i][l.Name] = l.Value
		}
	}
	return
}

func (p *Parser) parseLine(sendChan chan parser.ParseInfo, resultChan chan parser.ParseResult, wg *sync.WaitGroup) {
	for parseInfo := range sendChan {
		parseInfo.Line = strings.TrimSpace(parseInfo.Line)
		if len(parseInfo.Line) <= 0 {
			resultChan <- parser.ParseResult{
				Line:  parseInfo.Line,
				Index: parseInfo.Index,
			}
			continue
		}

		data, err1 := p.parse(parseInfo.Line)
		if err1 == nil {
			resultChan <- parser.ParseResult{
				Line:  parseInfo.Line,
				Index: parseInfo.Index,
				Data:  data,
			}
			continue
		}

		datas, err2 := p.parseLineMutiData(parseInfo.Line)
		resultChan <- parser.ParseResult{
			Line:  parseInfo.Line,
			Index: parseInfo.Index,
			Datas: datas,
			Err:   err2,
		}
	}
	wg.Done()
}
