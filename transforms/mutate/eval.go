package mutate

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/apaxa-go/eval"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	ErrInvalidExpr = func(expr string) error {
		return errors.New(fmt.Sprintf("%s is invalid", expr))
	}

	ErrInvalidPara = func(para string) error {
		return errors.New(fmt.Sprintf("%s is invalid", para))
	}
	ErrDataHasNoKey = func(data Data, key string) error {
		return errors.New(fmt.Sprintf("%v has not key %s", data, key))
	}
	ErrnNoSupport = errors.New("Only support int or string")
)

const TYPEEVAL = "eval"

type Eval struct {
	Key         string `json:"key"`
	hasParseKey bool
	exprs       map[string]*Expr
	StageTime   string `json:"stage"`
	stats       StatsInfo
}

type Expr struct {
	// ret1.ret2 = ${key1.key11} + ${key2.key22}
	expr       string              // ${key1.key11} + ${key2.key22}
	retKey     string              // ret1.ret2
	retKeyList []string            //[ret1, ret2]
	paras      map[string][]string //{${key1.key11} : [key1, key11], ${key2.key22} : [key2, key22]}
}

func (expr *Expr) initExpr(key string) error {
	strs := strings.Split(key, "=")

	if len(strs) != 2 {
		return ErrInvalidExpr(key)
	}
	expr.expr = strings.TrimSpace(strs[1])

	expr.retKey = strs[0]
	expr.retKeyList = strings.Split(expr.retKey, ".")

	if len(expr.retKeyList) < 0 {
		return ErrInvalidExpr(key)
	}

	reg, err := regexp.Compile(`\$\{(.*?)\}`)
	if err != nil {
		return err
	}
	paras := reg.FindAllString(strs[1], -1)
	expr.paras = make(map[string][]string)
	for _, para := range paras {
		expr.paras[para] = strings.Split(para[2:len(para)-1], ".")
		if len(expr.paras[para]) == 0 {
			return ErrInvalidPara(para)
		}
	}
	return nil
}

func (expr *Expr) calExpr(data map[string]interface{}) (Data, error) {
	exprStr := expr.expr
	var zero interface{}

	for keyStr, keys := range expr.paras {
		var val interface{} = data
		for _, key := range keys {
			if m, ok := val.(map[string]interface{}); ok {
				val, ok = m[key]
				if !ok {
					return data, ErrDataHasNoKey(data, keyStr)
				}
			} else {
				return data, ErrDataHasNoKey(data, keyStr)
			}
		}

		switch val.(type) {
		case int64, int32, int16, int8, int:
			exprStr = strings.Replace(exprStr, keyStr, fmt.Sprintf("%v", val), -1)
			zero = 0
		case string:
			exprStr = strings.Replace(exprStr, keyStr, fmt.Sprintf("`%v`", val), -1)
			zero = "``"
		default:
			return data, ErrnNoSupport
		}
	}

	// 加上0 值，避免expr 为赋值表达式
	exprStr = fmt.Sprintf("%s + %v", exprStr, zero)
	expression, err := eval.ParseString(exprStr, "")
	if err != nil {
		return data, err
	}
	v, err := expression.EvalToInterface(nil)
	if err != nil {
		return data, err
	}

	var val interface{} = data
	for index, key := range expr.retKeyList {
		if _, ok := val.(map[string]interface{}); !ok {
			return data, ErrDataHasNoKey(data, expr.retKey)
		}

		if _, ok := val.(map[string]interface{})[key]; !ok {
			if index == len(expr.retKeyList)-1 {
				val.(map[string]interface{})[key] = v
				return data, nil
			} else {
				val.(map[string]interface{})[key] = map[string]interface{}{}
			}

		}
		val = val.(map[string]interface{})[key]
	}
	return data, nil
}

func (e *Eval) getOrSetExpr(key string) (*Expr, error) {
	if expr, ok := e.exprs[key]; ok {
		return expr, nil
	}

	expr := Expr{}
	err := expr.initExpr(key)
	if err != nil {
		return nil, err
	}
	e.exprs[key] = &expr
	return &expr, nil
}

func (e *Eval) initKeyExpr() {
	if e.exprs != nil {
		return
	}
	e.exprs = make(map[string]*Expr)
	ks := strings.Split(e.Key, ",")
	for _, k := range ks {
		e.getOrSetExpr(k)
	}
}

func (e *Eval) RawTransform(datas []string) ([]string, error) {
	e.stats.Success += int64(len(datas))
	return datas, nil
}

func (e *Eval) Transform(datas []Data) ([]Data, error) {
	if !e.hasParseKey {
		e.initKeyExpr()
	}

	for _, data := range datas {
		var err error = nil
		for _, v := range e.exprs {
			data, err = v.calExpr(data)
			if err != nil {
				e.stats.LastError = err.Error()
			}
		}
		if err != nil {
			e.stats.Errors += 1
		} else {
			e.stats.Success += 1
		}
	}
	return datas, nil
}

func (e *Eval) Description() string {
	return `根据指定的公式计算一个新字段，如数据{"a":123, "b": 567}，指定公式 c=a+b，数据变为 {"a":123, "b": 321, "c": 444}`
}

func (e *Eval) Type() string {
	return TYPEEVAL
}

func (e *Eval) SampleConfig() string {
	return `{
		"type":"eval",
		"key":"name=${key1.key11}+${key2.key22}",
		"stage":"after_parser"
	}`
}

func (e *Eval) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
	}
}

func (e *Eval) Stage() string {
	if e.StageTime == "" {
		return transforms.StageAfterParser
	}
	return e.StageTime
}

func (e *Eval) Stats() StatsInfo {
	return e.stats
}

func init() {
	transforms.Add(TYPEEVAL, func() transforms.Transformer {
		return &Eval{}
	})
}
