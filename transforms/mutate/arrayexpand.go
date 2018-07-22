package mutate

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &ArrayExpand{}
	_ transforms.Transformer      = &ArrayExpand{}
	_ transforms.Initializer      = &ArrayExpand{}
)

type ArrayExpand struct {
	Key   string `json:"key"`
	stats StatsInfo

	keys []string
}

func (p *ArrayExpand) Init() error {
	p.keys = GetKeys(p.Key)
	return nil
}

func (p *ArrayExpand) transformToMap(val interface{}, key string) map[string]interface{} {
	resultMap := make(map[string]interface{})
	switch legalType := val.(type) {
	case []int:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int8:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int16:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int32:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []int64:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint8:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint16:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint32:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []uint64:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []bool:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []string:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []float32:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []float64:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []complex64:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []complex128:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
			resultMap[key] = arrVal
		}
		return resultMap
	case []interface{}:
		for index, arrVal := range legalType {
			key := key + strconv.Itoa(index)
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

func (p *ArrayExpand) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	if p.keys == nil {
		p.Init()
	}
	newKeys := make([]string, len(p.keys))
	for i := range datas {
		copy(newKeys, p.keys)
		val, getErr := GetMapValue(datas[i], p.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, p.Key)
			continue
		}
		if resultMap := p.transformToMap(val, newKeys[len(p.keys)-1]); resultMap != nil {
			for key, arrVal := range resultMap {
				suffix := 0
				keyName := key
				newKeys[len(newKeys)-1] = keyName
				_, getErr := GetMapValue(datas[i], newKeys...)
				for ; getErr == nil; suffix++ {
					if suffix > 5 {
						log.Warnf("keys %v -- %v already exist, the key %v will be ignored", key, keyName, key)
						break
					}
					keyName = key + "_" + strconv.Itoa(suffix)
					newKeys[len(newKeys)-1] = keyName
					_, getErr = GetMapValue(datas[i], newKeys...)
				}
				if suffix <= 5 {
					setErr := SetMapValue(datas[i], arrVal, false, newKeys...)
					if setErr != nil {
						errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, p.Key)
					}
				}
			}

		} else {
			typeErr := fmt.Errorf("transform key %v data type is not array", p.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
		}
	}
	p.stats, fmtErr = transforms.SetStatsInfo(err, p.stats, int64(errNum), int64(len(datas)), p.Type())
	return datas, fmtErr
}

func (p *ArrayExpand) Description() string {
	//return "expand an array like arraykey:[a, b, c] into Data map {arraykey0:a,arraykey1:b,arraykey2:c}"
	return "展开数组，例：arraykey:[a, b, c]展开为map{arraykey0:a,arraykey1:b,arraykey2:c}"
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

func (p *ArrayExpand) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
	}
}

func (p *ArrayExpand) Stage() string {
	return transforms.StageAfterParser
}

func (p *ArrayExpand) Stats() StatsInfo {
	return p.stats
}

func (p *ArrayExpand) SetStats(err string) StatsInfo {
	p.stats.LastError = err
	return p.stats
}

func init() {
	transforms.Add("arrayexpand", func() transforms.Transformer {
		return &ArrayExpand{}
	})
}
