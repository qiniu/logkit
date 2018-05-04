package mutate

import (
	"fmt"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	Prefix = "prefix"
	Suffix = "suffix"
	Both   = "both"
)

type Trim struct {
	Key        string `json:"key"`
	Characters string `json:"characters"`
	Place      string `json:"place"`
	stats      StatsInfo
}

func (g *Trim) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	keys := GetKeys(g.Key)
	for i := range datas {
		val, gerr := GetMapValue(datas[i], keys...)
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
		switch g.Place {
		case Prefix:
			strval = strings.TrimPrefix(strval, g.Characters)
		case Suffix:
			strval = strings.TrimSuffix(strval, g.Characters)
		default:
			strval = strings.Trim(strval, g.Characters)
		}
		SetMapValue(datas[i], strval, false, keys...)
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform trim, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Trim) RawTransform(datas []string) ([]string, error) {
	return datas, nil
}

func (g *Trim) Description() string {
	return "去掉字符串前后多余的字符，如 abc123, 设置trim的字符为abc，变化后为 123"
}

func (g *Trim) Type() string {
	return "trim"
}

func (g *Trim) SampleConfig() string {
	return `{
		"type":"trim",
		"key":"ToBeTrimedFieldKey",
		"characters":"trimCh",
		"place":"left"
	}`
}

func (g *Trim) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "characters",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "ToBeTrimedFieldKey",
			DefaultNoUse: true,
			Description:  "要修整掉的字符内容(characters)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "place",
			ChooseOnly:    true,
			ChooseOptions: []interface{}{Both, Prefix, Suffix},
			Default:       Both,
			DefaultNoUse:  false,
			Description:   "修整位置(place)",
			Type:          transforms.TransformTypeBoolean,
		},
	}
}

func (g *Trim) Stage() string {
	return transforms.StageAfterParser
}

func (g *Trim) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("trim", func() transforms.Transformer {
		return &Trim{}
	})
}
