package ua

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/ua-parser/uap-go/uaparser"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const Name = "UserAgent"

type UATransformer struct {
	Key              string `json:"key"`
	RegexYmlFilePath string `json:"regex_yml_path"`
	UA_Device        string `json:"device"`
	dev              bool
	UA_OS            string `json:"os"`
	os               bool
	UA_Agent         string `json:"agent"`
	agent            bool
	MemCache         string `json:"memory_cache"`
	memcache         bool
	stats            StatsInfo
	uap              *uaparser.Parser
	cache            map[string]*uaparser.Client
}

func (it *UATransformer) Init() (err error) {
	if it.RegexYmlFilePath != "" {
		it.uap, err = uaparser.New(it.RegexYmlFilePath)
		if err != nil {
			log.Errorf("load regex yml file error %v, use default one", err)
		}
	}
	if it.uap == nil {
		it.uap = uaparser.NewFromSaved()
	}
	it.cache = make(map[string]*uaparser.Client)
	it.memcache, _ = strconv.ParseBool(it.MemCache)
	it.agent, _ = strconv.ParseBool(it.UA_Agent)
	it.dev, _ = strconv.ParseBool(it.UA_Device)
	it.os, _ = strconv.ParseBool(it.UA_OS)
	return nil
}

func (it *UATransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("UserAgent transformer not support rawTransform")
}

func (it *UATransformer) getParsedData(line string) (UserAgent *uaparser.UserAgent, Os *uaparser.Os, Device *uaparser.Device) {
	if !it.dev && !it.os && !it.agent {
		return
	}

	if it.memcache {
		ag, ok := it.cache[line]
		if ok {
			return ag.UserAgent, ag.Os, ag.Device
		}
	}
	var wg sync.WaitGroup
	if it.agent {
		wg.Add(1)
		go func() {
			defer wg.Done()
			it.uap.RLock()
			UserAgent = it.uap.ParseUserAgent(line)
			it.uap.RUnlock()
		}()
	}
	if it.os {
		wg.Add(1)
		go func() {
			defer wg.Done()
			it.uap.RLock()
			Os = it.uap.ParseOs(line)
			it.uap.RUnlock()
		}()
	}
	if it.dev {
		wg.Add(1)
		go func() {
			defer wg.Done()
			it.uap.RLock()
			Device = it.uap.ParseDevice(line)
			it.uap.RUnlock()
		}()
	}
	wg.Wait()
	if it.memcache {
		it.cache[line] = &uaparser.Client{UserAgent, Os, Device}
	}
	return
}

func (it *UATransformer) Transform(datas []Data) ([]Data, error) {
	if it.uap == nil {
		it.uap = uaparser.NewFromSaved()
	}
	var err, ferr error
	errnums := 0
	keys := GetKeys(it.Key)
	newkeys := make([]string, len(keys))
	for i := range datas {
		copy(newkeys, keys)
		val, gerr := GetMapValue(datas[i], keys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", it.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", it.Key)
			continue
		}
		if strval == "" {
			errnums++
			err = fmt.Errorf("transform key %v is empty string", it.Key)
			continue
		}

		if it.agent {
			UserAgent := it.uap.ParseUserAgent(strval)
			if UserAgent.Family != "" {
				newkeys[len(newkeys)-1] = "UA_Family"
				SetMapValue(datas[i], UserAgent.Family, false, newkeys...)
			}
			if UserAgent.Major != "" {
				newkeys[len(newkeys)-1] = "UA_Major"
				SetMapValue(datas[i], UserAgent.Major, false, newkeys...)
			}
			if UserAgent.Minor != "" {
				newkeys[len(newkeys)-1] = "UA_Minor"
				SetMapValue(datas[i], UserAgent.Minor, false, newkeys...)
			}
			if UserAgent.Patch != "" {
				newkeys[len(newkeys)-1] = "UA_Patch"
				SetMapValue(datas[i], UserAgent.Patch, false, newkeys...)
			}
		}
		if it.agent {
			Device := it.uap.ParseDevice(strval)
			if Device.Family != "" {
				newkeys[len(newkeys)-1] = "UA_Device_Family"
				SetMapValue(datas[i], Device.Family, false, newkeys...)
			}
			if Device.Brand != "" {
				newkeys[len(newkeys)-1] = "UA_Device_Brand"
				SetMapValue(datas[i], Device.Brand, false, newkeys...)
			}
			if Device.Model != "" {
				newkeys[len(newkeys)-1] = "UA_Device_Model"
				SetMapValue(datas[i], Device.Model, false, newkeys...)
			}
		}

		if it.os {
			Os := it.uap.ParseOs(strval)
			if Os.Family != "" {
				newkeys[len(newkeys)-1] = "UA_OS_Family"
				SetMapValue(datas[i], Os.Family, false, newkeys...)
			}
			if Os.Patch != "" {
				newkeys[len(newkeys)-1] = "UA_OS_Patch"
				SetMapValue(datas[i], Os.Patch, false, newkeys...)
			}
			if Os.Minor != "" {
				newkeys[len(newkeys)-1] = "UA_OS_Minor"
				SetMapValue(datas[i], Os.Minor, false, newkeys...)
			}
			if Os.Major != "" {
				newkeys[len(newkeys)-1] = "UA_OS_Major"
				SetMapValue(datas[i], Os.Major, false, newkeys...)
			}
			if Os.PatchMinor != "" {
				newkeys[len(newkeys)-1] = "UA_OS_PatchMinor"
				SetMapValue(datas[i], Os.PatchMinor, false, newkeys...)
			}

		}

	}
	if err != nil {
		it.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform UserAgent, last error info is %v", errnums, err)
	}
	it.stats.Errors += int64(errnums)
	it.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (it *UATransformer) Description() string {
	//return "transform UserAgent will parse user_agent string to detail information"
	return "解析 User Agent 中的用户信息，包括浏览器型号、版本、系统信息、设备号等 "
}

func (it *UATransformer) Type() string {
	return "UserAgent"
}

func (it *UATransformer) SampleConfig() string {
	return `{
		"type":"UserAgent",
		"key":"MyUserAgentFieldKey",
		"regex_yml_path":"/your/path/to/regexes.yaml"
	}`
}

func (it *UATransformer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "regex_yml_path",
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "/your/path/to/regexes.yaml",
			DefaultNoUse: true,
			Description:  "UserAgent解析正则表达式文件路径(regex_yml_path)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "device",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "解析UserAgent中的设备信息(device)",
			Type:          transforms.TransformTypeString,
		},
		{
			KeyName:       "os",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "解析UserAgent中的操作系统信息(os)",
			Type:          transforms.TransformTypeString,
		},
		{
			KeyName:       "agent",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "解析UserAgent中的agent信息(agent)",
			Type:          transforms.TransformTypeString,
		},
		{
			KeyName:       "memory_cache",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "将解析结果缓存在内存中(memory_cache)",
			Type:          transforms.TransformTypeString,
			Advance:       true,
		},
	}
}

func (it *UATransformer) Stage() string {
	return transforms.StageAfterParser
}

func (it *UATransformer) Stats() StatsInfo {
	return it.stats
}

func init() {
	transforms.Add(Name, func() transforms.Transformer {
		return &UATransformer{}
	})
}
