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

var (
	_ transforms.StatsTransformer = &Trim{}
	_ transforms.Transformer      = &Trim{}
)

type Trim struct {
	Key        string `json:"key"`
	Characters string `json:"characters"`
	Place      string `json:"place"`
	stats      StatsInfo
}

func (g *Trim) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	keys := GetKeys(g.Key)
	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
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
		switch g.Place {
		case Prefix:
			strVal = strings.TrimPrefix(strVal, g.Characters)
		case Suffix:
			strVal = strings.TrimSuffix(strVal, g.Characters)
		default:
			strVal = strings.Trim(strVal, g.Characters)
		}
		setErr := SetMapValue(datas[i], strVal, false, keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.Key)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
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
			ToolTip:       "前缀后缀都去掉(both)，只去掉前缀(prefix)或者后缀(suffix)",
		},
	}
}

func (g *Trim) Stage() string {
	return transforms.StageAfterParser
}

func (g *Trim) Stats() StatsInfo {
	return g.stats
}

func (g *Trim) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("trim", func() transforms.Transformer {
		return &Trim{}
	})
}
