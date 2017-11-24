package mutate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

type ArrayExpand struct {
	Key   string `json:"key"`
	stats utils.StatsInfo
}

func (p *ArrayExpand) transformToMap(val interface{}) map[string]interface{} {
	resultMap := make(map[string]interface{})
	switch legalType := val.(type) {
	case []int:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int8:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int16:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int32:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int64:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint8:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint16:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint32:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint64:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []bool:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []string:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []float32:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []float64:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []complex64:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []complex128:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []interface{}:
		for index, arrVal := range legalType {
			key := p.Key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	default:
		return nil
	}
}

func (p *ArrayExpand) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("array expand transformer not support rawTransform")
}

func (p *ArrayExpand) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, pErr error
	errNums := 0
	separator := "."
	keys := strings.Split(p.Key, separator)
	newkeys := make([]string, len(keys))
	for i := range datas {
		copy(newkeys, keys)
		val, gerr := utils.GetMapValue(datas[i], keys...)
		if gerr != nil {
			errNums++
			err = fmt.Errorf("transform key %v not exist in data", p.Key)
			continue
		}
		if resultMap := p.transformToMap(val); resultMap != nil {
			for key, arrVal := range resultMap {
				suffix := 0
				keyName := key
				newkeys[len(newkeys) -1] = keyName
				_, gerr := utils.GetMapValue(datas[i], newkeys...)
				for ; gerr == nil; suffix++ {
					if suffix > 5 {
						log.Warnf("keys %v -- %v already exist, the key %v will be ignored", key, keyName, key)
						break
					}
					keyName = key + "_" + strconv.Itoa(suffix)
					newkeys[len(newkeys) -1] = keyName
					_, gerr = utils.GetMapValue(datas[i], newkeys...)
				}
				if suffix <= 5 {
					utils.SetMapValue(datas[i], arrVal, newkeys...)
				}
			}
		} else {
			errNums++
			err = fmt.Errorf("transform key %v data type is not array", p.Key)
		}
	}
	if err != nil {
		p.stats.LastError = err.Error()
		pErr = fmt.Errorf("find total %v erorrs in transform array expand, last error info is %v", errNums, err)
	}
	p.stats.Errors += int64(errNums)
	p.stats.Success += int64(len(datas) - errNums)
	return datas, pErr
}

func (p *ArrayExpand) Description() string {
	return "expand an array like arraykey:[a, b, c] into sender.Data map {arraykey0:a,arraykey1:b,arraykey2:c}"
}

func (p *ArrayExpand) Type() string {
	return "arrayexpand"
}

func (p *ArrayExpand) SampleConfig() string {
	return `{
		"type":"arrayexpand",
		"key":"ArrayFieldKey",
	}`
}

func (p *ArrayExpand) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
	}
}

func (p *ArrayExpand) Stage() string {
	return transforms.StageAfterParser
}

func (p *ArrayExpand) Stats() utils.StatsInfo {
	return p.stats
}

func init() {
	transforms.Add("arrayexpand", func() transforms.Transformer {
		return &ArrayExpand{}
	})
}
