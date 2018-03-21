package ua

import (
	"errors"
	"fmt"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/ua-parser/uap-go/uaparser"
)

type UATransformer struct {
	Key              string `json:"key"`
	RegexYmlFilePath string `json:"regex_yml_path"`
	stats            StatsInfo
	uap              *uaparser.Parser
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
	return nil
}

func (it *UATransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("UserAgent transformer not support rawTransform")
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
		client := it.uap.Parse(strval)

		if client.UserAgent.Family != "" {
			newkeys[len(newkeys)-1] = "UA_Family"
			SetMapValue(datas[i], client.UserAgent.Family, false, newkeys...)
		}
		if client.UserAgent.Major != "" {
			newkeys[len(newkeys)-1] = "UA_Major"
			SetMapValue(datas[i], client.UserAgent.Major, false, newkeys...)
		}
		if client.UserAgent.Minor != "" {
			newkeys[len(newkeys)-1] = "UA_Minor"
			SetMapValue(datas[i], client.UserAgent.Minor, false, newkeys...)
		}
		if client.UserAgent.Patch != "" {
			newkeys[len(newkeys)-1] = "UA_Patch"
			SetMapValue(datas[i], client.UserAgent.Patch, false, newkeys...)
		}
		if client.Device.Family != "" {
			newkeys[len(newkeys)-1] = "UA_Device_Family"
			SetMapValue(datas[i], client.Device.Family, false, newkeys...)
		}
		if client.Device.Brand != "" {
			newkeys[len(newkeys)-1] = "UA_Device_Brand"
			SetMapValue(datas[i], client.Device.Brand, false, newkeys...)
		}
		if client.Device.Model != "" {
			newkeys[len(newkeys)-1] = "UA_Device_Model"
			SetMapValue(datas[i], client.Device.Model, false, newkeys...)
		}
		if client.Os.Family != "" {
			newkeys[len(newkeys)-1] = "UA_OS_Family"
			SetMapValue(datas[i], client.Os.Family, false, newkeys...)
		}
		if client.Os.Patch != "" {
			newkeys[len(newkeys)-1] = "UA_OS_Patch"
			SetMapValue(datas[i], client.Os.Patch, false, newkeys...)
		}
		if client.Os.Minor != "" {
			newkeys[len(newkeys)-1] = "UA_OS_Minor"
			SetMapValue(datas[i], client.Os.Minor, false, newkeys...)
		}
		if client.Os.Major != "" {
			newkeys[len(newkeys)-1] = "UA_OS_Major"
			SetMapValue(datas[i], client.Os.Major, false, newkeys...)
		}
		if client.Os.PatchMinor != "" {
			newkeys[len(newkeys)-1] = "UA_OS_PatchMinor"
			SetMapValue(datas[i], client.Os.PatchMinor, false, newkeys...)
		}

	}
	if err != nil {
		it.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform IP, last error info is %v", errnums, err)
	}
	it.stats.Errors += int64(errnums)
	it.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (it *UATransformer) Description() string {
	//return "transform UserAgent will parse user_agent string to detail information"
	return "解析 User Agent 中的用户信息"
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
			Default:      "/your/path/to/regexes.yaml",
			DefaultNoUse: true,
			Description:  "UserAgent解析正则表达式文件路径(regex_yml_path)",
			Type:         transforms.TransformTypeString,
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
	transforms.Add("UserAgent", func() transforms.Transformer {
		return &UATransformer{}
	})
}
