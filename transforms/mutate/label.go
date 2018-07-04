package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Label{}
	_ transforms.Transformer      = &Label{}
)

type Label struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Override bool   `json:"override"`
	stats    StatsInfo
}

func (g *Label) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("label transformer not support rawTransform")
}

func (g *Label) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	keySlice := GetKeys(g.Key)
	for i := range datas {
		_, getErr := GetMapValue(datas[i], keySlice...)
		if getErr == nil && !g.Override {
			existErr := fmt.Errorf("the key %v already exists", g.Key)
			errNum, err = transforms.SetError(errNum, existErr, transforms.General, "")
			continue
		}
		setErr := SetMapValue(datas[i], g.Value, false, keySlice...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.Key)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func (g *Label) Description() string {
	//return "label can add a field"
	return `增加标签, 如设置标签{key:a,value:b}, 则数据中加入 {"a":"b"}`
}

func (g *Label) Type() string {
	return "label"
}

func (g *Label) SampleConfig() string {
	return `{
		"type":"label",
		"key":"my_field_keyname",
		"value":"my_field_value",
		"override":false
	}`
}

func (g *Label) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "value",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "要添加的数据值[仅限string类型](value)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "override",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "要进行Transform变化的键已存在时，是否覆盖原有的值(override)",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
	}
}

func (g *Label) Stage() string {
	return transforms.StageAfterParser
}

func (g *Label) Stats() StatsInfo {
	return g.stats
}

func (g *Label) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("label", func() transforms.Transformer {
		return &Label{}
	})
}
