package mutate

import (
	"fmt"
	"regexp"

	"errors"
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
	Mode      string `json:"regex"`
	stats     utils.StatsInfo
	Regex     *regexp.Regexp
}

const (
	ModeString = "string"
	ModeRegex  = "regex"
)

func (g *Replacer) Init() error {
	if g.Mode == ModeString || g.Mode == "" {
		g.Mode = ModeString
	} else if g.Mode == ModeRegex {
		rgx, err := regexp.Compile(g.Old)
		if err != nil {
			return err
		}
		g.Regex = rgx
	} else {
		return errors.New("illegal mode")
	}
	return nil
}

func (g *Replacer) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	keys := utils.GetKeys(g.Key)
	switch g.Mode {
	case ModeRegex:
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
			utils.SetMapValue(datas[i], g.Regex.ReplaceAllString(strval, g.New), keys...)
		}
	case ModeString:
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
			utils.SetMapValue(datas[i], strings.Replace(strval, g.Old, g.New, -1), keys...)
		}
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
	switch g.Mode {
	case ModeRegex:
		for i := range datas {
			datas[i] = g.Regex.ReplaceAllString(datas[i], g.New)
		}
	case ModeString:
		for i := range datas {
			datas[i] = strings.Replace(datas[i], g.Old, g.New, -1)
		}
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
