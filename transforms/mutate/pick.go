package mutate

import (
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const TYPE = "pick"

var (
	_ transforms.StatsTransformer = &Pick{}
	_ transforms.Transformer      = &Pick{}
)

type Pick struct {
	Key       string `json:"key"`
	StageTime string `json:"stage"`
	stats     StatsInfo
}

func (g *Pick) RawTransform(datas []string) ([]string, error) {
	var ret []string
	for i := range datas {
		if !strings.Contains(datas[i], g.Key) {
			continue
		}
		ret = append(ret, datas[i])
	}
	g.stats.Success += int64(len(datas))
	return ret, nil
}

func (g *Pick) Transform(datas []Data) ([]Data, error) {
	pickKeys := strings.Split(g.Key, ",")
	var retDatas []Data
	for i := range datas {
		data := Data{}
		for _, v := range pickKeys {
			keys := GetKeys(v)
			PickMapValue(datas[i], data, keys...)
		}
		if len(data) != 0 {
			retDatas = append(retDatas, data)
		}
	}

	g.stats, _ = transforms.SetStatsInfo(nil, g.stats, 0, int64(len(datas)), g.Type())
	return retDatas, nil
}

func (g *Pick) Description() string {
	//return "pick onefield from data"
	return `保留指定的数据字段, 如数据{"a":123,"b":"xx"}, 指定a，变为{"a":"123"}, 可写多个用逗号分隔 a,b 数据变为空 {"a":123,"b":"xx"}`
}

func (g *Pick) Type() string {
	return TYPE
}

func (g *Pick) SampleConfig() string {
	return `{
		"type":"pick",
		"key":"PickFieldKey1,PickFieldKey2",
		"stage":"after_parser"
	}`
}

func (g *Pick) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
	}
}

func (g *Pick) Stage() string {
	if g.StageTime == "" {
		return transforms.StageAfterParser
	}
	return g.StageTime
}

func (g *Pick) Stats() StatsInfo {
	return g.stats
}

func (g *Pick) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add(TYPE, func() transforms.Transformer {
		return &Pick{}
	})
}
