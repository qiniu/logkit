package ip

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
	"strings"
)

const Name = "IP"

const (
	Region       = "Region"
	City         = "City"
	Country      = "Country"
	Isp          = "Isp"
	CountryCode  = "CountryCode"
	Latitude     = "Latitude"
	Longitude    = "Longitude"
	DistrictCode = "DistrictCode"
)

type Transformer struct {
	StageTime   string `json:"stage"`
	Key         string `json:"key"`
	DataPath    string `json:"data_path"`
	KeyAsPrefix bool   `json:"key_as_prefix"`

	loc   Locator
	stats StatsInfo
}

func (_ *Transformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("IP transformer not support rawTransform")
}

func (t *Transformer) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	if t.loc == nil {
		loc, err := NewLocator(t.DataPath)
		if err != nil {
			return datas, err
		}
		t.loc = loc
	}
	errnums := 0
	keys := GetKeys(t.Key)
	newkeys := make([]string, len(keys))
	for i := range datas {
		copy(newkeys, keys)
		val, gerr := GetMapValue(datas[i], keys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", t.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", t.Key)
			continue
		}
		strval = strings.TrimSpace(strval)
		info, nerr := t.loc.Find(strval)
		if nerr != nil {
			err = nerr
			errnums++
			continue
		}
		newkeys[len(newkeys)-1] = Region
		SetMapValueWithPrefix(datas[i], info.Region, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		newkeys[len(newkeys)-1] = City
		SetMapValueWithPrefix(datas[i], info.City, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		newkeys[len(newkeys)-1] = Country
		SetMapValueWithPrefix(datas[i], info.Country, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		newkeys[len(newkeys)-1] = Isp
		SetMapValueWithPrefix(datas[i], info.Isp, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		if info.CountryCode != "" {
			newkeys[len(newkeys)-1] = CountryCode
			SetMapValueWithPrefix(datas[i], info.CountryCode, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		}
		if info.Latitude != "" {
			newkeys[len(newkeys)-1] = Latitude
			SetMapValueWithPrefix(datas[i], info.Latitude, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		}
		if info.Longitude != "" {
			newkeys[len(newkeys)-1] = Longitude
			SetMapValueWithPrefix(datas[i], info.Longitude, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		}
		if info.DistrictCode != "" {
			newkeys[len(newkeys)-1] = DistrictCode
			SetMapValueWithPrefix(datas[i], info.DistrictCode, keys[len(keys)-1], t.KeyAsPrefix, newkeys...)
		}
	}
	if err != nil {
		t.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform IP, last error info is %v", errnums, err)
	}
	t.stats.Errors += int64(errnums)
	t.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (_ *Transformer) Description() string {
	//return "transform ip to country region and isp"
	return "获取IP的区域、国家、城市和运营商信息"
}

func (_ *Transformer) Type() string {
	return "IP"
}

func (_ *Transformer) SampleConfig() string {
	return `{
		"type":"IP",
		"stage":"after_parser",
		"key":"MyIpFieldKey",
		"data_path":"your/path/to/ip.dat"
	}`
}

func (_ *Transformer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "data_path",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "your/path/to/ip.dat(x)",
			DefaultNoUse: true,
			Description:  "IP数据库路径(data_path)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "key_as_prefix",
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Required:      false,
			Default:       false,
			DefaultNoUse:  false,
			Description:   "字段名称作为前缀(key_as_prefix)",
			Type:          transforms.TransformTypeString,
		},
	}
}

func (t *Transformer) Stage() string {
	return transforms.StageAfterParser
}

func (t *Transformer) Stats() StatsInfo {
	return t.stats
}

func init() {
	transforms.Add(Name, func() transforms.Transformer {
		return &Transformer{}
	})
}
