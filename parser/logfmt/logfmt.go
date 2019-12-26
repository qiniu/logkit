package logfmt

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	errMsg = "will keep origin data in pandora_stash if disable_record_errdata field is false"
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

	// 调整数据类型
	if len(pairs)%2 == 1 {
		return nil, errors.New(fmt.Sprintf("key value not match, %s", errMsg))
	}

	data := make([]Data, 0, 1)
	field := make(Data)
	for i := 0; i < len(pairs); i += 2 {
		// 消除双引号； 针对foo="" ,"foo=" 情况；其他情况如 a"b"c=d"e"f等首尾不出现引号的情况视作合法。
		kNum := strings.Count(pairs[i], "\"")
		vNum := strings.Count(pairs[i+1], "\"")
		if kNum%2 == 1 && vNum%2 == 1 {
			if strings.HasPrefix(pairs[i], "\"") && strings.HasSuffix(pairs[i+1], "\"") {
				pairs[i] = pairs[i][1:]
				pairs[i+1] = pairs[i+1][:len(pairs[i+1])-1]
			}
		}
		if kNum%2 == 0 && len(pairs[i]) > 1 {
			if strings.HasPrefix(pairs[i], "\"") && strings.HasSuffix(pairs[i], "\"") {
				pairs[i] = pairs[i][1 : len(pairs[i])-1]
			}
		}
		if vNum%2 == 0 && len(pairs[i+1]) > 1 {
			if strings.HasPrefix(pairs[i+1], "\"") && strings.HasSuffix(pairs[i+1], "\"") {
				pairs[i+1] = pairs[i+1][1 : len(pairs[i+1])-1]
			}
		}

		if len(pairs[i]) == 0 || len(pairs[i+1]) == 0 {
			return nil, fmt.Errorf("no value or key was parsed after logfmt, %s", errMsg)
		}

		value := pairs[i+1]
		if !p.keepString {
			if fValue, err := strconv.ParseFloat(value, 64); err == nil {
				field[pairs[i]] = fValue
				continue
			}
			if bValue, err := strconv.ParseBool(value); err == nil {
				field[pairs[i]] = bValue
				continue
			}

		}
		field[pairs[i]] = value
	}
	if len(field) == 0 {
		return nil, fmt.Errorf("data is empty after parse, %s", errMsg)
	}

	data = append(data, field)
	return data, nil
}

func splitKV(line string, sep string) ([]string, error) {
	data := make([]string, 0, 100)

	if !strings.Contains(line, sep) {
		return nil, errors.New(fmt.Sprintf("no splitter exist, %s", errMsg))
	}

	kvArr := make([]string, 0, 100)
	isKey := true
	vhead := 0
	lastSpace := 0
	pos := 0
	sepLen := len(sep)

	// key或value值中包含sep的情况；默认key中不包含sep；导致algorithm = 1+1=2会变成合法
	for pos+sepLen <= len(line) {
		if unicode.IsSpace(rune(line[pos : pos+1][0])) {
			nextSep := strings.Index(line[pos+1:], sep)
			if nextSep == -1 {
				break
			}
			if strings.TrimSpace(line[pos+1:pos+1+nextSep]) != "" {
				lastSpace = pos
				pos++
				continue
			}
		}
		if line[pos:pos+sepLen] == sep {
			if isKey {
				kvArr = append(kvArr, strings.TrimSpace(line[vhead:pos]))
				isKey = false
			} else {
				if lastSpace <= vhead {
					pos++
					continue
				}
				kvArr = append(kvArr, strings.TrimSpace(line[vhead:lastSpace]))
				kvArr = append(kvArr, strings.TrimSpace(line[lastSpace:pos]))
			}
			vhead = pos + sepLen
			pos = pos + sepLen - 1
		}
		pos++
	}
	if vhead < len(line) {
		kvArr = append(kvArr, strings.TrimSpace(line[vhead:]))
	}
	data = append(data, kvArr...)
	return data, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeKeyValue
}
