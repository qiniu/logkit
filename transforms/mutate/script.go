package mutate

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"

	"github.com/robertkrimen/otto"
)

type Script struct {
	OldKey  string `json:"oldKey"` //格式为: key1:var1,key2:var2  其中key为待转换的数据中的字段名称,var为script中的变量名称. 可以写多对,用逗号分隔.如果key和var相同,var可省略
	NewKey  string `json:"newKey"` //格式为: key1:var1,key2:var2  其中key为要向数据中新设置的的字段名称,var为script中的变量名称.
	Script  string `json:"script"` //要执行的js脚本
	oldKeys [][]string
	oldVars []string
	newKeys [][]string
	newVars []string
	vm      *otto.Otto
	stats   utils.StatsInfo
}

func (g *Script) Init() (err error) {
	if g.OldKey != "" {
		g.oldKeys, g.oldVars, err = parseKey(g.OldKey)
	}
	if err != nil {
		return err
	}
	if g.NewKey != "" {
		g.newKeys, g.newVars, err = parseKey(g.NewKey)
	}
	if err != nil {
		return err
	}
	vm := otto.New()
	vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking
	g.vm = vm
	return nil
}

func parseKey(key string) ([][]string, []string, error) {
	sepRegexpItem := "\\s*,\\s*"
	sepRegexpP := "\\s*:\\s*"
	regexItem, error := regexp.Compile(sepRegexpItem)
	if error != nil {
		return nil, nil, error
	}
	regexP, error := regexp.Compile(sepRegexpP)
	if error != nil {
		return nil, nil, error
	}
	items := regexItem.Split(key, -1)
	var keyss [][]string
	var vars []string
	for _, item := range items {
		pair := regexP.Split(item, -1)
		keys := utils.GetKeys(pair[0])
		keyss = append(keyss, keys)
		if len(pair) > 1 {
			vars = append(vars, pair[1])
			continue
		}
		vars = append(vars, keys[len(keys)-1])
	}
	return keyss, vars, nil
}

func (g *Script) Transform(datas []sender.Data) (returnData []sender.Data, ferr error) {
	var err error
	errnums := 0
	vm := otto.New()
	returnData = utils.DeepCopy(datas).([]sender.Data)
	vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking
	halt := errors.New("script time out")
	defer func() {
		if caught := recover(); caught != nil {
			if caught == halt {
				return
			}
			panic(caught)
		}
	}()
	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		//执行超时,则认为全部执行失败,返回原数据
		time.Sleep(3 * time.Second) // Stop after two seconds
		g.stats.LastError = halt.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform script, last error info is %v", len(returnData), halt)
		g.stats.Errors += int64(len(datas))
		g.stats.Success += 0
		g.vm.Interrupt <- func() {
			panic(halt)
		}
	}(cancelCtx)

	for i := range datas {
		for j, keys := range g.oldKeys {
			val, gerr := utils.GetMapValue(datas[i], keys...)
			if gerr != nil {
				err = fmt.Errorf("transform key %v not exist in data", keys)
				break
			}
			g.vm.Set(g.oldVars[j], val)
		}
		if err != nil {
			errnums++
			continue
		}
		_, scriptErr := g.vm.Run(g.Script)
		if scriptErr != nil {
			err = fmt.Errorf("run script error: %v", scriptErr)
			errnums++
			continue
		}
		for j, keys := range g.newKeys {
			value, scriptErr := g.vm.Get(g.newVars[j])
			if scriptErr != nil {
				err = fmt.Errorf("can not get script value: %v, :%v", g.newVars[j], scriptErr)
				continue
			}
			var val interface{}
			if value.IsNumber() {
				val, _ = value.ToFloat()
			} else if value.IsString() {
				val, _ = value.ToString()
			} else if value.IsBoolean() {
				val, _ = value.ToBoolean()
			} else {
				err = fmt.Errorf("run script error: The value obtained only can be number,string or boolean ")
				break
			}
			sErr := utils.SetMapValue(datas[i], val, false, keys...)
			if sErr != nil {
				err = fmt.Errorf("run script error: %v", scriptErr)
				break
			}
		}
		if err != nil {
			errnums++
			continue
		}
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform script, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	cancel()
	returnData = datas
	return
}

func (g *Script) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("rename transformer not support rawTransform")
}

func (g *Script) Description() string {
	return "transform field by script"
}

func (g *Script) Type() string {
	return "script"
}

func (g *Script) SampleConfig() string {
	return `{
		"type":"script",
		"oldKey":"oldKey",
		"newKey":"newKey",
		"script":""
	}`
}

func (g *Script) ConfigOptions() []utils.Option {
	return []utils.Option{
		{
			KeyName:      "oldKey",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
			Description:  "旧的字段名",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "newKey",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
			Description:  "新的字段名",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "script",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
			Description:  "转换字段的脚本",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Script) Stage() string {
	return transforms.StageAfterParser
}

func (g *Script) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("script", func() transforms.Transformer {
		return &Script{}
	})
}
