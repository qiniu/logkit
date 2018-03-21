package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
)

type Json struct {
	Key      string `json:"key"`
	New      string `json:"new"`
	stats    StatsInfo
	jsonTool jsoniter.API
}

func (g *Json) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errCount := 0
	keys := GetKeys(g.Key)
	news := GetKeys(g.New)

	for i := range datas {
		val, gerr := GetMapValue(datas[i], keys...)
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
			DeleteMapValue(datas[i], keys...)
			news = keys
		}
		serr := SetMapValue(datas[i], jsonVal, false, news...)
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
	err = jsonTool.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		err = fmt.Errorf("parse json str error %v, jsonStr is: %v", err, jsonStr)
		log.Debug(err)
	}
	return
}

func (g *Json) Description() string {
	//return "parse jsonStr to json"
	return "解析json"
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

func (g *Json) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
	}
}

func (g *Json) Stage() string {
	return transforms.StageAfterParser
}

func (g *Json) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("json", func() transforms.Transformer {
		return &Json{
			jsonTool: jsoniter.Config{
				EscapeHTML: true,
				UseNumber:  true,
			}.Froze(),
		}
	})
}
