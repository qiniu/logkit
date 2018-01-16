package mutate

import (
	"errors"
	"fmt"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Json struct {
	Key        string `json:"key"`
	New        string `json:"new"`
	stats      utils.StatsInfo
	jsonTool   jsoniter.API
}

func (g *Json) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errCount := 0
	keys := utils.GetKeys(g.Key)
	news := utils.GetKeys(g.New)

	for i := range datas {
		val, gerr := utils.GetMapValue(datas[i], keys...)
		if gerr != nil {
			errCount++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errCount++
			err = fmt.Errorf("transform key %v data type is not string", g.Key)
			continue
		}
		jsonVal, perr := parseJson(g.jsonTool, strval)
		if perr != nil {
			errCount++
			err = perr
			continue
		}

		if len(news) == 0 {
			utils.DeleteMapValue(datas[i], keys...)
			news = keys
		}
		serr := utils.SetMapValue(datas[i], jsonVal, false, news...)
		if serr != nil {
			errCount++
			err = fmt.Errorf("the new key %v already exists ", g.New)
		}
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform json, last error info is %v", errCount, err)
	}
	g.stats.Errors += int64(errCount)
	g.stats.Success += int64(len(datas) - errCount)
	return datas, ferr
}

func (g *Json) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("json transformer not support rawTransform")
}

func parseJson(jsonTool jsoniter.API, jsonStr string) (data interface{}, err error) {
	data = sender.Data{}
	err = jsonTool.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		err = fmt.Errorf("parse json str error %v, jsonStr is: %v", err, jsonStr)
		log.Debug(err)
	}

	return
}

func (g *Json) Description() string {
	return "parse jsonStr to json"
}

func (g *Json) Type() string {
	return "json"
}

func (g *Json) SampleConfig() string {
	return `{
       "type":"json",
       "key":"myParseKey",
       "new":"myNewKey"
    }`
}

func (g *Json) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyFieldName,
		{
			KeyName:      "new",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "新的字段名",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Json) Stage() string {
	return transforms.StageAfterParser
}

func (g *Json) Stats() utils.StatsInfo {
	return g.stats
}

func Init() {
	transforms.Add("json", func() transforms.Transformer {
		return &Json{
			jsonTool: jsoniter.Config{
				EscapeHTML: true,
				UseNumber:  true,
			}.Froze(),
		}
	})
}
