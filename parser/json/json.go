package json

import (
	"fmt"
	"sort"
	"sync"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

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
	numRoutine           int
	keepRawData          bool
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
	keepRawData, _ := c.GetBoolOr(parser.KeyKeepRawData, false)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}

	return &Parser{
		name:                 name,
		labels:               labels,
		jsontool:             jsontool,
		disableRecordErrData: disableRecordErrData,
		numRoutine:           numRoutine,
		keepRawData:          keepRawData,
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
			errData := make(Data)
			if !p.disableRecordErrData {
				errData[KeyPandoraStash] = parseResult.Line
			} else if !p.keepRawData {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, parseResult.Index)
			}
			if p.keepRawData {
				errData[parser.KeyRawData] = parseResult.Line
			}
			if !p.disableRecordErrData || p.keepRawData {
				datas = append(datas, errData)
			}
			continue
		}
		if len(parseResult.Datas) == 0 { //数据为空时不发送
			se.ErrorDetail = fmt.Errorf("parsed no data by line [%v]", parseResult.Line)
			se.AddErrors()
			continue
		}

		se.AddSuccess()
		//一条Json格式的数据可能返回多个Data，只有当返回Data数组长度为1是raw_data才会生效
		if p.keepRawData && len(parseResult.Datas) == 1 {
			parseResult.Datas[0][parser.KeyRawData] = parseResult.Line
		}
		datas = append(datas, parseResult.Datas...)
	}

	return datas, se
}

func (p *Parser) parse(line string) (dataSlice []Data, err error) {
	data := make(Data)
	if err = p.jsontool.Unmarshal([]byte(line), &data); err == nil {
		for _, l := range p.labels {
			// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
			if _, ok := data[l.Name]; ok {
				continue
			}
			data[l.Name] = l.Value
		}
		return []Data{data}, nil
	}

	dataSlice = make([]Data, 0)
	if err = p.jsontool.Unmarshal([]byte(line), &dataSlice); err != nil {
		err = fmt.Errorf("parse json line error %v, raw data is: %v", err, line)
		log.Debug(err)
		return nil, err
	}

	for i := range dataSlice {
		for _, l := range p.labels {
			// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
			if _, ok := dataSlice[i][l.Name]; ok {
				continue
			}
			dataSlice[i][l.Name] = l.Value
		}
	}

	return dataSlice, nil
}
