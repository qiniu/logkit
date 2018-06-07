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

const Name = "user_agent"

type Transformer struct {
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

func (t *Transformer) Init() (err error) {
	if t.RegexYmlFilePath != "" {
		t.uap, err = uaparser.New(t.RegexYmlFilePath)
		if err != nil {
			log.Errorf("load regex yml file error %v, use default one", err)
		}
	}
	if t.uap == nil {
		t.uap = uaparser.NewFromSaved()
	}
	t.cache = make(map[string]*uaparser.Client)
	t.memcache, _ = strconv.ParseBool(t.MemCache)
	t.agent, _ = strconv.ParseBool(t.UA_Agent)
	t.dev, _ = strconv.ParseBool(t.UA_Device)
	t.os, _ = strconv.ParseBool(t.UA_OS)
	return nil
}

func (it *Transformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("user agent transformer not support rawTransform")
}

func (it *Transformer) getParsedData(line string) (UserAgent *uaparser.UserAgent, Os *uaparser.Os, Device *uaparser.Device) {
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

func (it *Transformer) Transform(datas []Data) ([]Data, error) {
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

func (it *Transformer) Description() string {
	//return "transform UserAgent will parse user_agent string to detail information"
	return "解析 User-Agent 中的用户信息，包括浏览器型号、版本、系统信息、设备号等 "
}

func (it *Transformer) Type() string {
	return Name
}

func (it *Transformer) SampleConfig() string {
	return `{
		"type":"` + Name + `",
		"key":"MyUserAgentFieldKey",
		"regex_yml_path":"/your/path/to/regexes.yaml"
	}`
}

func (it *Transformer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "regex_yml_path",
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "/your/path/to/regexes.yaml",
			DefaultNoUse: true,
			Description:  "User-Agent解析正则表达式文件路径(regex_yml_path)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "device",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{true, false},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "解析User-Agent中的设备信息(device)",
			Type:          transforms.TransformTypeBoolean,
		},
		{
			KeyName:       "os",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{true, false},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "解析User-Agent中的操作系统信息(os)",
			Type:          transforms.TransformTypeBoolean,
		},
		{
			KeyName:       "agent",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{true, false},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "解析User-Agent中的agent信息(agent)",
			Type:          transforms.TransformTypeBoolean,
		},
		{
			KeyName:       "memory_cache",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{true, false},
			Default:       "true",
			DefaultNoUse:  true,
			Description:   "将解析结果缓存在内存中(memory_cache)",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
	}
}

func (it *Transformer) Stage() string {
	return transforms.StageAfterParser
}

func (it *Transformer) Stats() StatsInfo {
	return it.stats
}

func init() {
	transforms.Add(Name, func() transforms.Transformer {
		return &Transformer{}
	})
}
