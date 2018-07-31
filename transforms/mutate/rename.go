package mutate

import (
	"errors"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Rename{}
	_ transforms.Transformer      = &Rename{}
	_ transforms.Initializer      = &Rename{}
)

type Rename struct {
	Key        string `json:"key"`
	NewKeyName string `json:"new_key_name"`
	NewKey     string `json:"new"`
	stats      StatsInfo

	keys []string
	news []string
}

func (g *Rename) Init() error {
	g.keys = GetKeys(g.Key)
	if g.NewKey == "" {
		g.NewKey = g.NewKeyName
	}
	g.news = GetKeys(g.NewKey)
	return nil
}
func (g *Rename) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("rename transformer not support rawTransform")
}

func (g *Rename) Transform(datas []Data) ([]Data, error) {
	if g.keys == nil {
		g.Init()
	}
	var err, fmtErr error
	errNum := 0
	for i := range datas {
		val, getErr := GetMapValue(datas[i], g.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			continue
		}
		DeleteMapValue(datas[i], g.keys...)
		setErr := SetMapValue(datas[i], val, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.NewKeyName)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func (g *Rename) Description() string {
	//return "rename can mutate old field name to new field name"
	return "用新的字段重命名旧的字段, 如 {a:123} 改为 {b:123}"
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

func (g *Rename) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
	}
}

func (g *Rename) Stage() string {
	return transforms.StageAfterParser
}

func (g *Rename) Stats() StatsInfo {
	return g.stats
}

func (g *Rename) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("rename", func() transforms.Transformer {
		return &Rename{}
	})
}
