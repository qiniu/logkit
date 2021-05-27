package mutate

import (
	"regexp"

	"github.com/pkg/errors"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Replacer{}
	_ transforms.Transformer      = &Replacer{}
	_ transforms.Initializer      = &Replacer{}
)

type Replacer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	OldKey    string `json:"old"` // 兼容老版本
	NewKey    string `json:"new"`
	Old       string `json:"old_string"`
	New       string `json:"new_string"`
	Regex     bool   `json:"regex"`
	stats     StatsInfo
	Regexp    *regexp.Regexp

	keys       []string
	numRoutine int
}

func (g *Replacer) Init() error {
	if g.Old == "" {
		g.Old = g.OldKey
	}
	if g.New == "" {
		g.New = g.NewKey
	}
	rgexpr := g.Old
	if !g.Regex {
		rgexpr = regexp.QuoteMeta(g.Old)
	}
	rgx, err := regexp.Compile(rgexpr)
	if err != nil {
		return err
	}
	g.Regexp = rgx
	g.keys = GetKeys(g.Key)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Replacer) Transform(datas []Data) ([]Data, error) {
	if len(g.keys) == 0 {
		g.Init()
	}
	var (
		err, fmtErr error
		errNum      int
	)
	for idx := range datas {
		val, getErr := GetMapValue(datas[idx], g.keys...)
		if getErr != nil {
			errNum++
			err = errors.New("transform key " + g.Key + " not exist in data")
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			errNum++
			err = errors.New("transform key " + g.Key + " data type is not string")
			continue
		}
		setErr := SetMapValue(datas[idx], g.Regexp.ReplaceAllString(strVal, g.New), false, g.keys...)
		if setErr != nil {
			errNum++
			err = errors.New("value of " + g.Key + " is not the type of map[string]interface{}")
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func (g *Replacer) RawTransform(datas []string) ([]string, error) {
	for i := range datas {
		datas[i] = g.Regexp.ReplaceAllString(datas[i], g.New)
	}

	g.stats, _ = transforms.SetStatsInfo(nil, g.stats, 0, int64(len(datas)), g.Type())
	return datas, nil
}

func (g *Replacer) Description() string {
	//return "replace old string to new"
	return "用新字符串替换旧字符串"
}

func (g *Replacer) Type() string {
	return "replace"
}

func (g *Replacer) SampleConfig() string {
	return `{
		"type":"replace",
		"stage":"before_parser",
		"key":"MyReplaceFieldKey",
		"old_string":"myOldString",
		"new_string":"myNewString",
        "regex":"false"
	}`
}

func (g *Replacer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyStage,
		transforms.KeyFieldName,
		{
			KeyName:      "old_string",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "myOldString",
			DefaultNoUse: true,
			Description:  "要替换的字符串内容(old_string)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "new_string",
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "myNewString",
			DefaultNoUse: true,
			Description:  "替换为的字符串内容(new_string)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "regex",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "是否启用正则匹配(regex)",
			Type:          transforms.TransformTypeBoolean,
		},
	}
}

func (g *Replacer) Stage() string {
	if g.StageTime == "" {
		return transforms.StageBeforeParser
	}
	return g.StageTime
}

func (g *Replacer) Stats() StatsInfo {
	return g.stats
}

func (g *Replacer) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("replace", func() transforms.Transformer {
		return &Replacer{}
	})
}
