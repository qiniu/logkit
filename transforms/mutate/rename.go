package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)


type Rename struct {
	OldField  string `json:"old_field"`
	NewField  string `json:"new_field"`
	stats utils.StatsInfo
}

func (g *Rename) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("rename transformer not support rawTransform")
}

func (g *Rename) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	for i := range datas {
		val, exists := datas[i][g.OldField]
		if !exists {
			errnums ++
			fmt.Errorf("transform key %v not exist in data", g.OldField)
			continue
		}
		delete(datas[i], g.OldField)
		datas[i][g.NewField] = val
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
		"old_field":"old_field_name"
	    "new_field":"new_field_name"
	}`
}

func (g *Rename) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyStageAfterOnly,
		{
			KeyName:      "old_field",
			ChooseOnly:   false,
			Default:      "old_field_name",
			DefaultNoUse: true,
			Description:  "要修改的字段名",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "new_field",
			ChooseOnly:   false,
			Default:      "new_field_name",
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