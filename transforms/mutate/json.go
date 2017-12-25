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
	Key   string `json:"key"`
	stats utils.StatsInfo
}

func (g *Json) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	keys := utils.GetKeys(g.Key)
	for i := range datas {
		val, gerr := utils.GetMapValue(datas[i], keys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", g.Key)
			continue
		}
		val, perr := parseJson(strval)
		if perr != nil {
			errnums++
			err = perr
			continue
		}
		utils.SetMapValue(datas[i], val, false, keys...)
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform replace, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Json) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("json transformer not support rawTransform")
}

func parseJson(jsonStr string) (data sender.Data, err error) {
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
