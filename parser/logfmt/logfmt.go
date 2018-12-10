package logfmt

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/go-logfmt/logfmt"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(TypeLogfmt, NewParser)
}

type Parser struct {
	name                 string
	disableRecordErrData bool
	numRoutine           int
	keepRawData          bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	return &Parser{
		name:                 name,
		disableRecordErrData: disableRecordErrData,
		numRoutine:           numRoutine,
		keepRawData:          keepRawData,
	}, nil
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
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
	if se.Errors == 0 {
		return datas, nil
	}
	return datas, se
}

func (p *Parser) parse(line string) ([]Data, error) {
	reader := bytes.NewReader([]byte(line))
	decoder := logfmt.NewDecoder(reader)
	datas := make([]Data, 0, 100)
	var fields Data
	for {
		ok := decoder.ScanRecord()
		if !ok {
			err := decoder.Err()
			if err != nil {
				return nil, err
			}
			//此错误仅用于当原始数据解析成功但无解析数据时，保留原始数据之用
			if len(fields) == 0 {
				return nil, errors.New("no value was parsed after logfmt, will keep origin data in pandora_stash if disable_record_errdata field is false")
			}
			break
		}
		fields = make(Data)
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
	return TypeLogfmt
}
