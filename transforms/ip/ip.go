package ip

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/wangtuanjie/ip17mon"
)

type IpTransformer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	DataPath  string `json:"data_path"`
	loc       *ip17mon.Locator
	stats     StatsInfo
}

func (it *IpTransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("IP transformer not support rawTransform")
}

func (it *IpTransformer) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	if it.loc == nil {
		it.loc, err = ip17mon.NewLocator(it.DataPath)
		if err != nil {
			return datas, err
		}
	}
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
		info, nerr := it.loc.Find(strval)
		if nerr != nil {
			err = nerr
			errnums++
			continue
		}
		newkeys[len(newkeys)-1] = "Region"
		SetMapValue(datas[i], info.Region, false, newkeys...)
		newkeys[len(newkeys)-1] = "City"
		SetMapValue(datas[i], info.City, false, newkeys...)
		newkeys[len(newkeys)-1] = "Country"
		SetMapValue(datas[i], info.Country, false, newkeys...)
		newkeys[len(newkeys)-1] = "Isp"
		SetMapValue(datas[i], info.Isp, false, newkeys...)
	}
	if err != nil {
		it.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform IP, last error info is %v", errnums, err)
	}
	it.stats.Errors += int64(errnums)
	it.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (it *IpTransformer) Description() string {
	//return "transform ip to country region and isp"
	return "获取IP的区域、国家、城市和运营商信息"
}

func (it *IpTransformer) Type() string {
	return "IP"
}

func (it *IpTransformer) SampleConfig() string {
	return `{
		"type":"IP",
		"stage":"after_parser",
		"key":"MyIpFieldKey",
		"data_path":"your/path/to/ip.dat"
	}`
}

func (it *IpTransformer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "data_path",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "your/path/to/ip.dat",
			DefaultNoUse: true,
			Description:  "IP数据库路径(data_path)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (it *IpTransformer) Stage() string {
	if it.StageTime == "" {
		return transforms.StageAfterParser
	}
	return it.StageTime
}

func (it *IpTransformer) Stats() StatsInfo {
	return it.stats
}

func init() {
	transforms.Add("IP", func() transforms.Transformer {
		return &IpTransformer{}
	})
}
