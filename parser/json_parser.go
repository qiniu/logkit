package parser

import (
	"fmt"

	"github.com/json-iterator/go"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

type JsonParser struct {
	name     string
	labels   []Label
	jsontool jsoniter.API
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

	return &JsonParser{
		name:     name,
		labels:   labels,
		jsontool: jsontool,
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
	se := &utils.StatsError{}
	for idx, line := range lines {
		data, err := im.parseLine(line)
		if err != nil {
			se.AddErrors()
			se.ErrorIndex = append(se.ErrorIndex, idx)
			se.ErrorDetail = err
			continue
		}
		datas = append(datas, data)
		se.AddSuccess()
	}
	return datas, se
}

func (im *JsonParser) parseLine(line string) (data Data, err error) {
	data = Data{}
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
