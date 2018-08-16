package mutate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Spliter{}
	_ transforms.Transformer      = &Spliter{}
)

type Spliter struct {
	Key           string `json:"key"`
	SeperateKey   string `json:"sep"`
	ArraryName    string `json:"newfield"`
	ArraryNameNew string `json:"new"`
	stats         StatsInfo
}

func (g *Spliter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("split transformer not support rawTransform")
}

func (g *Spliter) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	if g.ArraryName == "" {
		g.ArraryName = g.ArraryNameNew
	}
	if g.ArraryName == "" {
		fmtErr = errors.New("array name is empty string,can't use as array field key name")
		g.stats.LastError = fmtErr.Error()
		errNum = len(datas)
	} else {
		keys := GetKeys(g.Key)
		newKeys := make([]string, len(keys))
		for i := range datas {
			copy(newKeys, keys)
			val, getErr := GetMapValue(datas[i], newKeys...)
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
			newKeys[len(newKeys)-1] = g.ArraryName
			setErr := SetMapValue(datas[i], strings.Split(strVal, g.SeperateKey), false, newKeys...)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, strings.Join(newKeys, "."))
			}
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func (g *Spliter) Description() string {
	//return "split one field into array from data"
	return `将指定的字段切割成数组, 例如 "a,b,c" 切割为 ["a","b","c"]`
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

func (g *Spliter) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "newfield",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "new_field_keyname",
			DefaultNoUse: true,
			Description:  "解析后数据的字段名(newfield)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "sep",
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  ",",
			Required:     true,
			DefaultNoUse: true,
			Description:  "分隔符(sep)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Spliter) Stage() string {
	return transforms.StageAfterParser
}

func (g *Spliter) Stats() StatsInfo {
	return g.stats
}

func (g *Spliter) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("split", func() transforms.Transformer {
		return &Spliter{}
	})
}
