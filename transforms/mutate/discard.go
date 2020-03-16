package mutate

import (
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Discarder{}
	_ transforms.Transformer      = &Discarder{}
	_ transforms.Initializer      = &Discarder{}
)

type Discarder struct {
	Key       string `json:"key"`
	StageTime string `json:"stage"`
	stats     StatsInfo

	discardKeys [][]string

	numRoutine int
}

func (g *Discarder) Init() error {
	discardKeys := strings.Split(g.Key, ",")
	g.discardKeys = make([][]string, len(discardKeys))
	for i := range g.discardKeys {
		g.discardKeys[i] = GetKeys(discardKeys[i])
	}

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Discarder) RawTransform(datas []string) ([]string, error) {
	if len(g.discardKeys) == 0 {
		g.Init()
	}

	res := make([]string, 0, len(datas))
	for _, data := range datas {
		if strings.Contains(data, g.Key) {
			continue
		}
		res = append(res, data)
	}

	return res, nil
}

func (g *Discarder) Transform(datas []Data) ([]Data, error) {
	if g.discardKeys == nil {
		g.Init()
	}

	for idx := range datas {
		for _, keys := range g.discardKeys {
			DeleteMapValue(datas[idx], keys...)
		}
	}

	return datas, nil
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

func (g *Discarder) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("discard", func() transforms.Transformer {
		return &Discarder{}
	})
}

func (g *Discarder) rawtransform(dataPipeline <-chan transforms.RawTransformInfo, resultChan chan transforms.RawTransformResult, wg *sync.WaitGroup) {
	for transformInfo := range dataPipeline {
		if strings.Contains(transformInfo.CurData, g.Key) {
			continue
		}

		resultChan <- transforms.RawTransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
		}
	}
	wg.Done()
}
