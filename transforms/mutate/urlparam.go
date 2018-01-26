package mutate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

type UrlParam struct {
	Key   string `json:"key"`
	stats utils.StatsInfo
}

func (p *UrlParam) transformToMap(strVal string, key string) (map[string]string, error) {
	resultMap := make(map[string]string)
	params := strings.Split(strVal, "&")
	for _, param := range params {
		keyVal := strings.Split(param, "=")
		if len(keyVal) != 2 {
			return nil, fmt.Errorf("the key value %v is not legal", strVal)
		}
		if keyVal[0] == "" {
			return nil, fmt.Errorf("the key value %v is not legal", strVal)
		}
		keyName := key + "_" + keyVal[0]
		resultMap[keyName] = keyVal[1]
	}
	return resultMap, nil
}

func (p *UrlParam) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("param transformer not support rawTransform")
}

func (p *UrlParam) Transform(datas []Data) ([]Data, error) {
	var err, pErr error
	errNums := 0
	keys := utils.GetKeys(p.Key)
	newkeys := make([]string, len(keys))
	for i := range datas {
		copy(newkeys, keys)
		val, gerr := utils.GetMapValue(datas[i], newkeys...)
		if gerr != nil {
			errNums++
			err = fmt.Errorf("transform key %v not exist in data", p.Key)
			continue
		}
		var res map[string]string
		if strVal, ok := val.(string); ok {
			res, err = p.transformToMap(strVal, newkeys[len(newkeys)-1])
		} else {
			err = fmt.Errorf("transform key %v data type is not string", p.Key)
		}
		if err == nil {
			for key, mapVal := range res {
				suffix := 1
				keyName := key
				newkeys[len(newkeys)-1] = keyName
				_, gerr := utils.GetMapValue(datas[i], newkeys...)
				for ; gerr == nil; suffix++ {
					if suffix > 5 {
						log.Warnf("keys %v -- %v already exist, the item %v will be ignored", key, keyName, key)
						break
					}
					keyName = key + strconv.Itoa(suffix)
					newkeys[len(newkeys)-1] = keyName
					_, gerr = utils.GetMapValue(datas[i], newkeys...)
				}
				if suffix <= 5 {
					utils.SetMapValue(datas[i], mapVal, false, newkeys...)
				}
			}
		} else {
			errNums++
		}
	}
	if err != nil {
		p.stats.LastError = err.Error()
		pErr = fmt.Errorf("find total %v erorrs in transform param, last error info is %v", errNums, err)
	}
	p.stats.Errors += int64(errNums)
	p.stats.Success += int64(len(datas) - errNums)
	return datas, pErr
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
		"key":"ParamFieldKey",
	}`
}

func (p *UrlParam) ConfigOptions() []Option {
	return []Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
	}
}

func (p *UrlParam) Stage() string {
	return transforms.StageAfterParser
}

func (p *UrlParam) Stats() utils.StatsInfo {
	return p.stats
}

func init() {
	transforms.Add("urlparam", func() transforms.Transformer {
		return &UrlParam{}
	})
}
