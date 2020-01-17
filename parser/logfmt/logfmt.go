package logfmt

import (
	"fmt"
	"sync"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/parse/mutate"
)

func init() {
	parser.RegisterConstructor(TypeLogfmt, NewParser)
	parser.RegisterConstructor(TypeKeyValue, NewParser)
}

type Parser struct {
	name                 string
	keepString           bool
	disableRecordErrData bool
	numRoutine           int
	keepRawData          bool
	splitter             string
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)
	splitter, _ := c.GetStringOr(KeySplitter, "=")
	keepString, _ := c.GetBoolOr(KeyKeepString, false)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	return &Parser{
		name:                 name,
		keepString:           keepString,
		disableRecordErrData: disableRecordErrData,
		numRoutine:           numRoutine,
		keepRawData:          keepRawData,
		splitter:             splitter,
	}, nil
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	mp := mutate.Parser{
		KeepString: p.keepString,
		Splitter:   p.splitter,
	}
	if mp.Splitter == "" {
		mp.Splitter = "="
	}
	var (
		lineLen = len(lines)
		datas   = make([]Data, 0, lineLen)
		se      = &StatsError{}

		numRoutine = p.numRoutine
		sendChan   = make(chan parser.ParseInfo)
		resultChan = make(chan parser.ParseResult)
		wg         = new(sync.WaitGroup)
	)

	if lineLen < numRoutine {
		numRoutine = lineLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go parser.ParseLineDataSlice(sendChan, resultChan, wg, true, mp.Parse)
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
	var parseResultSlice = make(parser.ParseResultSlice, lineLen)
	for resultInfo := range resultChan {
		parseResultSlice[resultInfo.Index] = resultInfo
	}

	se.DatasourceSkipIndex = make([]int, lineLen)
	datasourceIndex := 0
	for _, parseResult := range parseResultSlice {
		if len(parseResult.Line) == 0 {
			se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
			datasourceIndex++
			continue
		}

		if parseResult.Err != nil {
			se.AddErrors()
			se.LastError = parseResult.Err.Error()
			errData := make(Data)
			if !p.disableRecordErrData {
				errData[KeyPandoraStash] = parseResult.Line
			} else if !p.keepRawData {
				se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
				datasourceIndex++
			}
			if p.keepRawData {
				errData[KeyRawData] = parseResult.Line
			}
			if !p.disableRecordErrData || p.keepRawData {
				datas = append(datas, errData)
			}
			continue
		}
		if len(parseResult.Datas) == 0 { //数据为空时不发送
			se.LastError = fmt.Sprintf("parsed no data by line [%s]", parseResult.Line)
			se.AddErrors()
			continue
		}

		se.AddSuccess()
		if p.keepRawData {
			//解析后的部分数据对应全部的原始数据会造成比较严重的数据膨胀
			//TODO 减少膨胀的数据
			for i := range parseResult.Datas {
				parseResult.Datas[i][KeyRawData] = parseResult.Line
			}
		}
		datas = append(datas, parseResult.Datas...)
	}

	se.DatasourceSkipIndex = se.DatasourceSkipIndex[:datasourceIndex]
	if se.Errors == 0 && len(se.DatasourceSkipIndex) == 0 {
		return datas, nil
	}
	return datas, se
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeKeyValue
}
