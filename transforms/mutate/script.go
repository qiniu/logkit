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

	"github.com/qiniu/log"
	"github.com/robertkrimen/otto"
)

type Script struct {
	OldKey    string `json:"key"`       //格式为: key1:var1,key2:var2  其中key为待转换的数据中的字段名称,var为script中的变量名称. 可以写多对,用逗号分隔.如果key和var相同,var可省略
	NewKey    string `json:"newKey"`    //格式为: key1:var1,key2:var2  其中key为要向数据中新设置的的字段名称,var为script中的变量名称.
	Script    string `json:"script"`    //要执行的js脚本
	DeleteOld bool   `json:"deleteOld"` //是否删除旧字段
	oldKeys   [][]string
	oldVars   []string
	newKeys   [][]string
	newVars   []string
	vm        *otto.Otto
	stats     utils.StatsInfo
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
	//vm := otto.New()
	//vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking
	//g.vm = vm
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
	g.vm = otto.New()
	g.vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking
	returnData = utils.DeepCopy(datas).([]sender.Data)
	halt := errors.New("script transformer execution timeout")
	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		if caught := recover(); caught != nil {
			if caught == halt {
				return
			}
			//panic(caught)
			log.Errorf("err : %v", caught)
		}
	}()
	go func(ctx context.Context) {
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
		//执行超时,则认为全部执行失败,返回原数据
		g.stats.LastError = halt.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform script, last error info is %v,the batch data transform all faield", len(returnData), halt)
		log.Error(ferr)
		g.stats.Errors += int64(len(datas))
		g.stats.Success += 0
		g.vm.Interrupt <- func() {
			panic(halt)
		}
	}(cancelCtx)

	for i := range datas {
		//向js VM 中设置属性
		for j, keys := range g.oldKeys {
			val, gerr := utils.GetMapValue(datas[i], keys...)
			if gerr != nil {
				err = fmt.Errorf("transform key %v not exist in data", keys)
				break
			}
			g.vm.Set(g.oldVars[j], val)
			if g.DeleteOld {
				utils.DeleteMapValue(datas[i], keys...)
			}
		}
		if err != nil {
			errnums++
			ferr = err
			continue
		}
		//运行脚本
		_, scriptErr := g.vm.Run(g.Script)
		if scriptErr != nil {
			err = fmt.Errorf("run script error: %v", scriptErr)
			errnums++
			ferr = err
			continue
		}
		//获取VM环境中的属性值
		for j, keys := range g.newKeys {
			value, scriptErr := g.vm.Get(g.newVars[j])
			if scriptErr != nil {
				err = fmt.Errorf("can not get script value: %v, :%v", g.newVars[j], scriptErr)
				break
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
				err = fmt.Errorf("faild to set mapValue error: %v", sErr)
				break
			}
		}
		if err != nil {
			errnums++
			ferr = err
			err = nil
			continue
		}
	}
	if ferr != nil {
		g.stats.LastError = ferr.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform script, last error info is %v", errnums, ferr)
		log.Error(ferr)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
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
		"script":"",
		"deleteOld":false
	}`
}

func (g *Script) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyFieldName,
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
