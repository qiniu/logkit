package ip

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
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

var (
	_ transforms.StatsTransformer = &Transformer{}
	_ transforms.Transformer      = &Transformer{}
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
	var err, fmtErr error
	errNum := 0
	if t.loc == nil {
		loc, err := NewLocator(t.DataPath)
		if err != nil {
			t.stats, _ = transforms.SetStatsInfo(err, t.stats, int64(errNum), int64(len(datas)), t.Type())
			return datas, err
		}
		t.loc = loc
	}
	keys := GetKeys(t.Key)
	newKeys := make([]string, len(keys))
	for i := range datas {
		copy(newKeys, keys)
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, t.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			notStringErr := fmt.Errorf("transform key %v data type is not string", t.Key)
			errNum, err = transforms.SetError(errNum, notStringErr, transforms.General, "")
			continue
		}
		info, findErr := t.loc.Find(strVal)
		if findErr != nil {
			errNum, err = transforms.SetError(errNum, findErr, transforms.General, "")
			continue
		}
		newKeys[len(newKeys)-1] = Region
		SetMapValueWithPrefix(datas[i], info.Region, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		newKeys[len(newKeys)-1] = City
		SetMapValueWithPrefix(datas[i], info.City, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		newKeys[len(newKeys)-1] = Country
		SetMapValueWithPrefix(datas[i], info.Country, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		newKeys[len(newKeys)-1] = Isp
		SetMapValueWithPrefix(datas[i], info.Isp, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		if info.CountryCode != "" {
			newKeys[len(newKeys)-1] = CountryCode
			SetMapValueWithPrefix(datas[i], info.CountryCode, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		}
		if info.Latitude != "" {
			newKeys[len(newKeys)-1] = Latitude
			SetMapValueWithPrefix(datas[i], info.Latitude, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		}
		if info.Longitude != "" {
			newKeys[len(newKeys)-1] = Longitude
			SetMapValueWithPrefix(datas[i], info.Longitude, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		}
		if info.DistrictCode != "" {
			newKeys[len(newKeys)-1] = DistrictCode
			SetMapValueWithPrefix(datas[i], info.DistrictCode, keys[len(keys)-1], t.KeyAsPrefix, newKeys...)
		}
	}

	t.stats, fmtErr = transforms.SetStatsInfo(err, t.stats, int64(errNum), int64(len(datas)), t.Type())
	return datas, fmtErr
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

func (t *Transformer) SetStats(err string) StatsInfo {
	t.stats.LastError = err
	return t.stats
}

func init() {
	transforms.Add(Name, func() transforms.Transformer {
		return &Transformer{}
	})
}
