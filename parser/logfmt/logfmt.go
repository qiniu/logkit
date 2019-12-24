package logfmt

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
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
	if p.splitter == "" {
		p.splitter = "="
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
	if se.Errors == 0 && len(se.DatasourceSkipIndex) == 0 {
		return datas, nil
	}
	return datas, se
}

func (p *Parser) parse(line string) ([]Data, error) {

	pairs, err := splitKV(line, p.splitter)
	if err != nil {
		return nil, err
	}
	datas := make([]Data, 0, 100)

	// 调整数据类型
	for _, pair := range pairs {
		field := make(Data)
		for i := 0; i < len(pair); i += 2 {
			if len(pair[i]) == 0 || len(pair[i+1]) == 0 {
				return nil, errors.New("no value was parsed after logfmt, will keep origin data in pandora_stash if disable_record_errdata field is false")
			}

			value := pair[i+1]
			if !p.keepString {
				if fValue, err := strconv.ParseFloat(value, 64); err == nil {
					field[pair[i]] = fValue
					continue
				}
				if bValue, err := strconv.ParseBool(value); err == nil {
					field[pair[i]] = bValue
					continue
				}

			}
			field[pair[i]] = strings.Trim(value, "\"")
		}
		if len(field) == 0 {
			continue
		}
		datas = append(datas, field)
	}

	return datas, nil
}

func splitKV(line string, sep string) ([][]string, error) {
	line = strings.ReplaceAll(line, "\\\"", "")

	const space = " "
	data := make([][]string, 0, 100)

	// contain /n
	nl := strings.Index(line, "\n")
	for nl != -1 {
		if nl >= len(line)-1 {
			line = line[:len(line)-1]
			break
		}
		next := line[nl+1:]
		nextResult, err := splitKV(next, sep)
		if err != nil {
			return nil, err
		}
		data = append(data, nextResult...)
		line = line[:nl]
		nl = strings.Index(line, "\n")
	}

	if !strings.Contains(line, sep) {
		return nil, errors.New("no value was parsed after logfmt, will keep origin data in pandora_stash if disable_record_errdata field is false")
	}

	fields := strings.Split(line, sep)

	kvArr := make([]string, 0, 100)
	kvArr = append(kvArr, strings.TrimSpace(fields[0]))
	for i := 1; i < len(fields)-1; i++ {
		spaceIndex := strings.LastIndex(fields[i], space)
		if spaceIndex == -1 {
			return nil, errors.New("not correct key-value pair")
		}
		// split
		preV := strings.TrimSpace(fields[i][:spaceIndex])
		nextK := strings.TrimSpace(fields[i][spaceIndex:])

		kvArr = append(kvArr, preV)
		kvArr = append(kvArr, nextK)

	}

	kvArr = append(kvArr, strings.TrimSpace(fields[len(fields)-1]))
	data = append(data, kvArr)
	return data, nil

}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeKeyValue
}
