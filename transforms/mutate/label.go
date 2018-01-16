package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Label struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Override bool   `json:"override"`
	stats    utils.StatsInfo
}

func (g *Label) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("label transformer not support rawTransform")
}

func (g *Label) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	keySlice := utils.GetKeys(g.Key)
	for i := range datas {
		_, gerr := utils.GetMapValue(datas[i], keySlice...)
		if gerr == nil && !g.Override {
			errnums++
			err = fmt.Errorf("the key %v already exists", g.Key)
			continue
		}
		err = utils.SetMapValue(datas[i], g.Value, false, keySlice...)
		if err != nil {
			errnums++
			continue
		}
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform label, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Label) Description() string {
	return "增加标签"
}

func (g *Label) Type() string {
	return "标记"
}

func (g *Label) SampleConfig() string {
	return `{
		"type":"label",
		"key":"my_field_keyname",
		"value":"my_field_value",
		"override":false
	}`
}

func (g *Label) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyFieldName,
		transforms.KeyStageAfterOnly,
		{
			KeyName:      "value",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "要添加的数据值，仅限string类型(value)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "override",
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "要进行Transform变化的键已存在时，是否覆盖原有的值(override)",
			Type:          transforms.TransformTypeBoolean,
		},
	}
}

func (g *Label) Stage() string {
	return transforms.StageAfterParser
}

func (g *Label) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("label", func() transforms.Transformer {
		return &Label{}
	})
}
