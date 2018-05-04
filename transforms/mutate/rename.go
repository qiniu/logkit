package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

type Rename struct {
	Key        string `json:"key"`
	NewKeyName string `json:"new_key_name"`
	stats      StatsInfo
}

func (g *Rename) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("rename transformer not support rawTransform")
}

func (g *Rename) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	keySlice := GetKeys(g.Key)
	newKeySlice := GetKeys(g.NewKeyName)
	for i := range datas {
		val, gerr := GetMapValue(datas[i], keySlice...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		DeleteMapValue(datas[i], keySlice...)
		err = SetMapValue(datas[i], val, false, newKeySlice...)
		if err != nil {
			errnums++
			err = fmt.Errorf("the new key %v already exists ", g.NewKeyName)
			continue
		}
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform rename, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
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
		{
			KeyName:      "new_key_name",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "new_key_name",
			DefaultNoUse: true,
			Description:  "修改后的字段名(new_key_name)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Rename) Stage() string {
	return transforms.StageAfterParser
}

func (g *Rename) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("rename", func() transforms.Transformer {
		return &Rename{}
	})
}
