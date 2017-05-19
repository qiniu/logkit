package parser

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
)

type JsonParser struct {
	name      string
	labels    []label
	schemaErr *schemaErr
}

func NewJsonParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	nameMap := map[string]struct{}{}
	labels := getLabels(labelList, nameMap)

	return &JsonParser{
		name:   name,
		labels: labels,
		schemaErr: &schemaErr{
			number: 0,
			last:   time.Now(),
		},
	}, nil
}

func (im *JsonParser) Name() string {
	return im.name
}

func (im *JsonParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	se := &utils.StatsError{}
	for idx, line := range lines {
		data, err := im.parseLine(line)
		if err != nil {
			im.schemaErr.Output(err)
			se.AddErrors()
			se.ErrorIndex = append(se.ErrorIndex, idx)
			continue
		}
		datas = append(datas, data)
		se.AddSuccess()
	}
	return datas, se
}

func (im *JsonParser) parseLine(line string) (data sender.Data, err error) {
	data = sender.Data{}
	decoder := json.NewDecoder(bytes.NewReader([]byte(line)))
	decoder.UseNumber()
	if err = decoder.Decode(&data); err != nil {
		return
	}
	for _, l := range im.labels {
		// label 不覆盖数据，其他parser不需要这么一步检验，因为Schema固定，json的Schema不固定
		if _, ok := data[l.name]; ok {
			continue
		}
		data[l.name] = l.dataValue
	}
	return
}
