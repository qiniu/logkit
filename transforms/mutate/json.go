package mutate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
)

var (
	_ transforms.StatsTransformer = &Json{}
	_ transforms.Transformer      = &Json{}
	_ transforms.Initializer      = &Json{}
)

type Json struct {
	Key      string `json:"key"`
	New      string `json:"new"`
	stats    StatsInfo
	jsonTool jsoniter.API

	keys []string
	news []string
}

func (g *Json) Init() error {
	g.keys = GetKeys(g.Key)
	g.news = GetKeys(g.New)
	return nil
}

func (g *Json) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	if g.keys == nil {
		g.Init()
	}
	for i := range datas {
		val, getErr := GetMapValue(datas[i], g.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", g.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		strVal = strings.TrimSpace(strVal)
		if strVal == "" {
			continue
		}
		jsonVal, parseErr := parseJson(g.jsonTool, strVal)
		if parseErr != nil {
			errNum, err = transforms.SetError(errNum, parseErr, transforms.General, "")
			continue
		}

		if len(g.news) == 0 {
			g.news = g.keys
		}
		setErr := SetMapValue(datas[i], jsonVal, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
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
	return `解析json并加入到数据中，如json为 {"a":123}，加入后变为{"myNewKey":{"a":123}}`
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

func (g *Json) SetStats(err string) StatsInfo {
	g.stats.LastError = err
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
