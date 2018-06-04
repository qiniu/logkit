package mutate

import (
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

type Discarder struct {
	Key       string `json:"key"`
	StageTime string `json:"stage"`
	stats     StatsInfo
}

func (g *Discarder) RawTransform(datas []string) ([]string, error) {
	var ret []string
	for i := range datas {
		if strings.Contains(datas[i], g.Key) {
			continue
		}
		ret = append(ret, datas[i])
	}
	g.stats.Success += int64(len(datas))
	return ret, nil
}

func (g *Discarder) Transform(datas []Data) ([]Data, error) {
	var ferr error
	errnums := 0
	discardKeys := strings.Split(g.Key, ",")
	for _, v := range discardKeys {
		keys := GetKeys(v)
		for i := range datas {
			DeleteMapValue(datas[i], keys...)
		}
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Discarder) Description() string {
	//return "discard onefield from data"
	return `删除指定的数据字段, 如数据{"a":123,"b":"xx"}, 指定删除a，变为{"b":"xx"}, 可写多个用逗号分隔 a,b 数据变为空 {}`
}

func (g *Discarder) Type() string {
	return "discard"
}

func (g *Discarder) SampleConfig() string {
	return `{
		"type":"discard",
		"key":"DiscardFieldKey1,DiscardFieldKey2",
		"stage":"after_parser"
	}`
}

func (g *Discarder) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
	}
}

func (g *Discarder) Stage() string {
	if g.StageTime == "" {
		return transforms.StageAfterParser
	}
	return g.StageTime
}

func (g *Discarder) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("discard", func() transforms.Transformer {
		return &Discarder{}
	})
}
