package mutate

import (
	"errors"

	"strings"

	"fmt"

	"strconv"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type UrlParam struct {
	Key   string `json:"key"`
	stats utils.StatsInfo
}

func (p *UrlParam) transformToMap(strVal string) (map[string]string, error) {
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
		keyName := p.Key + "_" + keyVal[0]
		resultMap[keyName] = keyVal[1]
	}
	return resultMap, nil
}

func (p *UrlParam) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("param transformer not support rawTransform")
}

func (p *UrlParam) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, pErr error
	errNums := 0
	for i := range datas {
		val, ok := datas[i][p.Key]
		if !ok {
			errNums++
			err = fmt.Errorf("transform key %v not exist in data", p.Key)
			continue
		}
		var res map[string]string
		if strVal, ok := val.(string); ok {
			res, err = p.transformToMap(strVal)
		} else {
			err = fmt.Errorf("transform key %v data type is not string", p.Key)
		}
		if err == nil {
			for key, mapVal := range res {
				suffix := 1
				keyName := key
				_, exist := datas[i][keyName]
				for ; exist; suffix++ {
					if suffix > 5 {
						log.Warnf("keys %v -- %v already exist, the item %v will be ignored", key, keyName, key)
						break
					}
					keyName = key + strconv.Itoa(suffix)
					_, exist = datas[i][keyName]
				}
				if suffix <= 5 {
					datas[i][keyName] = mapVal
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
	return "parse url parameters like 'a=a&b=2&c=aa' into sender.Data map {a:a,b:2,c:aa}"
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

func (p *UrlParam) ConfigOptions() []utils.Option {
	return []utils.Option{
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
