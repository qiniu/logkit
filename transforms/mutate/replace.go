package mutate

import (
	"fmt"
	"strings"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Replacer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	Old       string `json:"old"`
	New       string `json:"new"`
	stats     utils.StatsInfo
}

func (g *Replacer) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	for i := range datas {
		val, ok := datas[i][g.Key]
		if !ok {
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
		datas[i][g.Key] = strings.Replace(strval, g.Old, g.New, -1)
	}
	if err != nil {
		g.stats.LastError = err
		ferr = fmt.Errorf("find total %v erorrs in transform replace, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Replacer) RawTransform(datas []string) ([]string, error) {
	for i := range datas {
		datas[i] = strings.Replace(datas[i], g.Old, g.New, -1)
	}
	g.stats.Success += int64(len(datas))
	return datas, nil
}

func (g *Replacer) Description() string {
	return "transform replace can replace old string to new"
}

func (g *Replacer) SampleConfig() string {
	return `{
		"type":"replace",
		"stage":"before_parser",
		"key":"MyReplaceFieldKey",
		"old":"myOldString",
		"new":"myNewString"
	}`
}

func (g *Replacer) Stage() string {
	if g.StageTime == "" {
		return transforms.StageBeforeParser
	}
	return g.StageTime
}

func (g *Replacer) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("replace", func() transforms.Transformer {
		return &Replacer{}
	})
}
