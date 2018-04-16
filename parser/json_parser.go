package parser

import (
	"fmt"

	"strings"

	"github.com/json-iterator/go"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

type JsonParser struct {
	name                 string
	labels               []Label
	disableRecordErrData bool
	jsontool             jsoniter.API
}

func NewJsonParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	nameMap := map[string]struct{}{}
	labels := GetLabels(labelList, nameMap)
	jsontool := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)

	return &JsonParser{
		name:                 name,
		labels:               labels,
		jsontool:             jsontool,
		disableRecordErrData: disableRecordErrData,
	}, nil
}

func (im *JsonParser) Name() string {
	return im.name
}

func (im *JsonParser) Type() string {
	return TypeJson
}

func (im *JsonParser) Parse(lines []string) ([]Data, error) {
	datas := []Data{}
	se := &StatsError{}
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
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
		se.ErrorIndex = append(se.ErrorIndex, idx)
		se.ErrorDetail = err1
		if !im.disableRecordErrData {
			errData := make(Data)
			errData[KeyPandoraStash] = line
			datas = append(datas, errData)
		}
	}
	return datas, se
}

func (im *JsonParser) parseLine(line string) (data Data, err error) {
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

func (im *JsonParser) parseLineMutiData(line string) (data []Data, err error) {
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
