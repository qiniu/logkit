package mutate

import (
	"fmt"
	"regexp"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"

	"strings"
)

type Replacer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	Old       string `json:"old"`
	New       string `json:"new"`
	stats     utils.StatsInfo
	rgx		  *regexp.Regexp
}

func (g *Replacer) Init() error  {
	rgx, err := regexp.Compile(g.Old)
	if err != nil{
		return err
	}
	g.rgx = rgx
	return nil
}

func (g *Replacer) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	for i := range datas {
		//val, ok := datas[i][g.Key]
		separator := "."
		keys := strings.Split(g.Key, separator)
		val := utils.GetMapValue(datas[i], keys)
		if val == nil {
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
		//datas[i][g.Key] = strings.Replace(strval, g.Old, g.New, -1)
		//datas[i][g.Key] = g.rgx.ReplaceAllString(strval, g.New)
		utils.SetMapValue(datas[i], keys, g.rgx.ReplaceAllString(strval, g.New))
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
		//datas[i] = strings.Replace(datas[i], g.Old, g.New, -1)
		datas[i] = g.rgx.ReplaceAllString(datas[i], g.New)
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
