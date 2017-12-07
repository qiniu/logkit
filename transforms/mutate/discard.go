package mutate

import (
	"errors"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Discarder struct {
	Key   string `json:"key"`
	stats utils.StatsInfo
}

func (g *Discarder) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("discard transformer not support rawTransform")
}

func (g *Discarder) Transform(datas []sender.Data) ([]sender.Data, error) {
	var ferr error
	errnums := 0
	keys := utils.GetKeys(g.Key)
	for i := range datas {
		utils.DeleteMapValue(datas[i], keys...)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Discarder) Description() string {
	return "discard onefield from data"
}

func (g *Discarder) Type() string {
	return "discard"
}

func (g *Discarder) SampleConfig() string {
	return `{
		"type":"discard",
		"key":"DiscardFieldKey"
	}`
}

func (g *Discarder) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
	}
}

func (g *Discarder) Stage() string {
	return transforms.StageAfterParser
}

func (g *Discarder) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("discard", func() transforms.Transformer {
		return &Discarder{}
	})
}
