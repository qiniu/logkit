package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Rename struct {
	Key        string `json:"key"`
	NewKeyName string `json:"new_key_name"`
	stats      utils.StatsInfo
}

func (g *Rename) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("rename transformer not support rawTransform")
}

func (g *Rename) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	keySlice := utils.GetKeys(g.Key)
	newKeySlice := utils.GetKeys(g.NewKeyName)
	for i := range datas {
		val, gerr := utils.GetMapValue(datas[i], keySlice...)
		if gerr != nil {
			errnums++
			fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		utils.DeleteMapValue(datas[i], keySlice...)
		utils.SetMapValue(datas[i], val, newKeySlice...)
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform replace, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Rename) Description() string {
	return "rename can mutate old field name to new field name"
}

func (g *Rename) Type() string {
	return "rename"
}

func (g *Rename) SampleConfig() string {
	return `{
		"type":"rename",
		"key":"old_key_name"
	    "new_key_name":"new_key_name"
	}`
}

func (g *Rename) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyFieldName,
		transforms.KeyStageAfterOnly,
		{
			KeyName:      "new_key_name",
			ChooseOnly:   false,
			Default:      "new_key_name",
			DefaultNoUse: true,
			Description:  "修改后的字段名",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Rename) Stage() string {
	return transforms.StageAfterParser
}

func (g *Rename) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("rename", func() transforms.Transformer {
		return &Rename{}
	})
}
