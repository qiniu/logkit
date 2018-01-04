package mutate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Json struct {
	OldKey  string `json:"key"`
	NewKey  string `json:"newKey"`
	stats   utils.StatsInfo
	oldKeys []string
	newKeys []string
}

func (g *Json) Init() error {
	g.oldKeys = utils.GetKeys(g.OldKey)
	g.newKeys = utils.GetKeys(g.NewKey)
	return nil
}

func (g *Json) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0

	for i := range datas {
		val, gerr := utils.GetMapValue(datas[i], g.oldKeys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.OldKey)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", g.OldKey)
			continue
		}
		jsonVal, perr := parseJson(strval)
		if perr != nil {
			errnums++
			err = perr
			continue
		}
		if len(g.newKeys) > 0 {
			utils.SetMapValue(datas[i], jsonVal, false, g.newKeys...)
		} else {
			for k, v := range jsonVal {
				utils.SetMapValue(datas[i], v, false, k)
			}
		}
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform json, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Json) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("json transformer not support rawTransform")
}

func parseJson(jsonStr string) (data map[string]interface{}, err error) {
	data = sender.Data{}
	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()
	if err = decoder.Decode(&data); err != nil {
		err = fmt.Errorf("parse json str error %v, jsonStr is: %v", err, jsonStr)
		log.Debug(err)
	}

	return
}

func (g *Json) Description() string {
	return "pase jsonStr to json"
}

func (g *Json) Type() string {
	return "json"
}

func (g *Json) SampleConfig() string {
	return `{
		"type":"json",
		"key":"myParseKey",
	}`
}

func (g *Json) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyFieldName,
		{
			KeyName:      "newKey",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
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

func init() {
	transforms.Add("json", func() transforms.Transformer {
		return &Json{}
	})
}
