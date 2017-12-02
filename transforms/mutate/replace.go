package mutate

import (
	"fmt"
	"regexp"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type Replacer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	Old       string `json:"old"`
	New       string `json:"new"`
	Regex     bool   `json:"regex"`
	stats     utils.StatsInfo
	Regexp    *regexp.Regexp
}

func (g *Replacer) Init() error {
	rgexpr := g.Old
	if !g.Regex {
		rgexpr = regexp.QuoteMeta(g.Old)
	}
	rgx, err := regexp.Compile(rgexpr)
	if err != nil {
		return err
	}
	g.Regexp = rgx
	return nil
}

func (g *Replacer) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	keys := utils.GetKeys(g.Key)
	for i := range datas {
		val, gerr := utils.GetMapValue(datas[i], keys...)
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
		utils.SetMapValue(datas[i], g.Regexp.ReplaceAllString(strval, g.New), false, keys...)
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform replace, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Replacer) RawTransform(datas []string) ([]string, error) {
	for i := range datas {
		datas[i] = g.Regexp.ReplaceAllString(datas[i], g.New)
	}
	g.stats.Success += int64(len(datas))
	return datas, nil
}

func (g *Replacer) Description() string {
	return "replace old string to new"
}

func (g *Replacer) Type() string {
	return "replace"
}

func (g *Replacer) SampleConfig() string {
	return `{
		"type":"replace",
		"stage":"before_parser",
		"key":"MyReplaceFieldKey",
		"old":"myOldString",
		"new":"myNewString"
	}`
}

func (g *Replacer) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyStage,
		transforms.KeyFieldName,
		{
			KeyName:      "old",
			ChooseOnly:   false,
			Default:      "myOldString",
			DefaultNoUse: true,
			Description:  "要替换的字符串内容(old)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "new",
			ChooseOnly:   false,
			Default:      "myNewString",
			DefaultNoUse: true,
			Description:  "替换为的字符串内容(new)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "regex",
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"false", "true"},
			Default:       "false",
			DefaultNoUse:  false,
			Description:   "是否启用正则匹配",
		},
	}
}

func (g *Replacer) Stage() string {
	if g.StageTime == "" {
		return transforms.StageBeforeParser
	}
	return g.StageTime
}

func (g *Replacer) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("replace", func() transforms.Transformer {
		return &Replacer{}
	})
}
