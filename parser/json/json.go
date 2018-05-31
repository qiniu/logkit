package json

import (
	"fmt"

	"strings"

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

	return &Parser{
		name:                 name,
		labels:               labels,
		jsontool:             jsontool,
		disableRecordErrData: disableRecordErrData,
	}, nil
}

func (im *Parser) Name() string {
	return im.name
}

func (im *Parser) Type() string {
	return parser.TypeJSON
}

func (im *Parser) Parse(lines []string) ([]Data, error) {
	datas := []Data{}
	se := &StatsError{}
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		data, err1 := im.parseLine(line)
		if err1 == nil {
			datas = append(datas, data)
			se.AddSuccess()
			continue
		}
		mutiData, err2 := im.parseLineMutiData(line)
		if err2 == nil {
			datas = append(datas, mutiData...)
			se.AddSuccess()
			continue
		}
		se.AddErrors()
		se.ErrorDetail = err1
		if !im.disableRecordErrData {
			errData := make(Data)
			errData[KeyPandoraStash] = line
			datas = append(datas, errData)
		} else {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
		}
	}
	return datas, se
}

func (im *Parser) parseLine(line string) (data Data, err error) {
	data = make(Data)
	if err = im.jsontool.Unmarshal([]byte(line), &data); err != nil {
		err = fmt.Errorf("parse json line error %v, raw data is: %v", err, line)
		log.Debug(err)
		return
	}
	for _, l := range im.labels {
		// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
		if _, ok := data[l.Name]; ok {
			continue
		}
		data[l.Name] = l.Value
	}
	return
}

func (im *Parser) parseLineMutiData(line string) (data []Data, err error) {
	data = make([]Data, 0)
	if err = im.jsontool.Unmarshal([]byte(line), &data); err != nil {
		err = fmt.Errorf("parse json line error %v, raw data is: %v", err, line)
		log.Debug(err)
		return
	}
	for i := range data {
		for _, l := range im.labels {
			// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
			if _, ok := data[i][l.Name]; ok {
				continue
			}
			data[i][l.Name] = l.Value
		}
	}
	return
}
