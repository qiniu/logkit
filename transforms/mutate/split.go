package mutate

import (
	"errors"

	"strings"

	"fmt"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Spliter struct {
	Key         string `json:"key"`
	SeperateKey string `json:"sep"`
	ArraryName  string `json:"newfield"`
	stats       utils.StatsInfo
}

func (g *Spliter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("split transformer not support rawTransform")
}

func (g *Spliter) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	if g.ArraryName == "" {
		ferr = errors.New("array name is empty string,can't use as array field key name")
		g.stats.LastError = ferr.Error()
		errnums = len(datas)
	} else {
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
			datas[i][g.ArraryName] = strings.Split(strval, g.SeperateKey)
		}
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform split, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Spliter) Description() string {
	return "split one field into array from data"
}

func (g *Spliter) Type() string {
	return "split"
}

func (g *Spliter) SampleConfig() string {
	return `{
		"type":"split",
		"key":"SplitFieldKey",
		"sep":",",
		"newfield":"name"
	}`
}

func (g *Spliter) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
		{
			KeyName:      "newfield",
			ChooseOnly:   false,
			Default:      "newfieldname",
			DefaultNoUse: true,
			Description:  "split后生成的array字段名称(newfield)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "sep",
			ChooseOnly:   false,
			Default:      " ",
			DefaultNoUse: true,
			Description:  "split的分隔符(sep)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Spliter) Stage() string {
	return transforms.StageAfterParser
}

func (g *Spliter) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("split", func() transforms.Transformer {
		return &Spliter{}
	})
}
