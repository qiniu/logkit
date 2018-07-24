package mutate

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	urlParamPath = "url_param_path"
	urlParamHost = "url_param_host"
)

var (
	_ transforms.StatsTransformer = &UrlParam{}
	_ transforms.Transformer      = &UrlParam{}
	_ transforms.Initializer      = &UrlParam{}
)

type UrlParam struct {
	Key        string `json:"key"`
	SelectKeys string `json:"select_keys"`

	keys          []string
	selectKeyList []string // slice 形式存放收集的 key 名称
	stats         StatsInfo
}

func (p *UrlParam) Init() error {
	p.keys = GetKeys(p.Key)

	// 获取 keys 并剔除空值
	selectKeys := strings.Split(p.SelectKeys, ",")
	p.selectKeyList = make([]string, 0, len(selectKeys))
	for i := range selectKeys {
		if len(selectKeys[i]) > 0 {
			p.selectKeyList = append(p.selectKeyList, selectKeys[i])
		}
	}
	return nil
}

func (p *UrlParam) isSelectKey(key string) bool {
	if len(p.selectKeyList) == 0 {
		return true
	}
	for i := range p.selectKeyList {
		if key == p.selectKeyList[i] {
			return true
		}
	}
	return false
}

func (p *UrlParam) transformToMap(strVal string, key string) (map[string]interface{}, error) {
	resultMap := make(map[string]interface{})
	var urlPath string
	if idx := strings.Index(strVal, "?"); idx != -1 {
		if len(strVal[:idx]) != 0 {
			urlPath = strVal[:idx]
		}
		strVal = strVal[idx+1:]
	} else {
		urlPath = strVal
	}
	if len(urlPath) > 0 {
		uri, err := url.Parse(urlPath)
		if err != nil {
			return nil, err
		}
		if len(uri.Path) > 0 {
			//如果同时满足不包含前缀`/`，还包含`&`，说明是个param
			if strings.HasPrefix(uri.Path, "/") || !strings.Contains(uri.Path, "&") {
				resultMap[key+"_"+urlParamPath] = uri.Path
			}
		}
		if len(uri.Host) > 0 {
			resultMap[key+"_"+urlParamHost] = uri.Host
		}
	}
	if len(strVal) < 1 {
		return resultMap, nil
	}

	values, err := url.ParseQuery(strVal)
	if err != nil {
		return nil, err
	}
	for k, v := range values {
		if !p.isSelectKey(k) {
			continue
		}

		keyName := key + "_" + k
		if len(v) == 1 && v[0] != "" {
			resultMap[keyName] = v[0]
		} else if len(v) > 1 {
			resultMap[keyName] = strings.Join(v, "&")
		}
	}
	return resultMap, nil
}

func (p *UrlParam) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("param transformer not support rawTransform")
}

func (p *UrlParam) Transform(datas []Data) ([]Data, error) {
	if p.keys == nil {
		p.Init()
	}
	var err, fmtErr, toMapErr error
	errNum := 0
	newKeys := make([]string, len(p.keys))
	for i := range datas {
		copy(newKeys, p.keys)
		val, getErr := GetMapValue(datas[i], newKeys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, p.Key)
			continue
		}
		var res map[string]interface{}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", p.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}

		res, toMapErr = p.transformToMap(strVal, newKeys[len(newKeys)-1])
		if toMapErr != nil {
			errNum, err = transforms.SetError(errNum, toMapErr, transforms.General, "")
			continue
		}

		for key, mapVal := range res {
			suffix := 1
			keyName := key
			newKeys[len(newKeys)-1] = keyName
			_, getErr := GetMapValue(datas[i], newKeys...)
			for ; getErr == nil; suffix++ {
				if suffix > 5 {
					log.Warnf("keys %v -- %v already exist, the item %v will be ignored", key, keyName, key)
					break
				}
				keyName = key + strconv.Itoa(suffix)
				newKeys[len(newKeys)-1] = keyName
				_, getErr = GetMapValue(datas[i], newKeys...)
			}
			if suffix <= 5 {
				setErr := SetMapValue(datas[i], mapVal, false, newKeys...)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, strings.Join(newKeys, "."))
				}
			}
		}
	}

	p.stats, fmtErr = transforms.SetStatsInfo(err, p.stats, int64(errNum), int64(len(datas)), p.Type())
	return datas, fmtErr
}

func (p *UrlParam) Description() string {
	//return "parse url parameters like 'a=a&b=2&c=aa' into Data map {a:a,b:2,c:aa}"
	return "针对指定的字段做url param解析，例：'a=a&b=2&c=aa'解析为map{a:a,b:2,c:aa}"
}

func (p *UrlParam) Type() string {
	return "urlparam"
}

func (p *UrlParam) SampleConfig() string {
	return `{
		"type":"urlparam",
		"key":"ParamFieldKey"
	}`
}

func (p *UrlParam) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "select_keys",
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "key1,key2,key3",
			DefaultNoUse: true,
			Description:  "选中收集的参数名(select_keys)",
			ToolTip:      "多个参数名之间使用用逗号(,)连接，收集所有参数则留空",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (p *UrlParam) Stage() string {
	return transforms.StageAfterParser
}

func (p *UrlParam) Stats() StatsInfo {
	return p.stats
}

func (p *UrlParam) SetStats(err string) StatsInfo {
	p.stats.LastError = err
	return p.stats
}

func init() {
	transforms.Add("urlparam", func() transforms.Transformer {
		return &UrlParam{}
	})
}
