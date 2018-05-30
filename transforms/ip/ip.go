package ip

import (
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/wangtuanjie/ip17mon"
)

type IpTransformer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	DataPath  string `json:"data_path"`
	aloc      *AllLocator
	stats     StatsInfo
}

type AllLocator struct {
	v1      *ip17mon.Locator
	v2      *Locator
	version string
}

const (
	V1 = "v1"
	V2 = "v2"
)

func NewAllLocator(dataFile string) (*AllLocator, error) {
	aloc := &AllLocator{}
	if strings.HasSuffix(dataFile, ".datx") {
		loc, err := NewLocator(dataFile)
		if err != nil {
			return nil, err
		}
		aloc.version = V2
		aloc.v2 = loc
		return aloc, nil
	}
	aloc.version = V1
	loc, err := ip17mon.NewLocator(dataFile)
	if err != nil {
		return aloc, err
	}
	aloc.v1 = loc
	return aloc, nil
}

func (aloc *AllLocator) Find(str string) (info *LocationInfo, err error) {
	switch aloc.version {
	case V2:
		return aloc.v2.Find(str)
	case V1:
		newInfo, err := aloc.v1.Find(str)
		if err != nil {
			return nil, err
		}
		info = &LocationInfo{
			Country: newInfo.Country,
			Region:  newInfo.Region,
			City:    newInfo.City,
			Isp:     newInfo.Isp,
		}
		return info, nil
	default:
		err = fmt.Errorf("unkonw locator verison %v", aloc.version)
		return
	}
	return
}

func (it *IpTransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("IP transformer not support rawTransform")
}

func (it *IpTransformer) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	if it.aloc == nil {
		it.aloc, err = NewAllLocator(it.DataPath)
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
		info, nerr := it.aloc.Find(strval)
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
		if info.CountryCode != "" {
			newkeys[len(newkeys)-1] = "CountryCode"
			SetMapValue(datas[i], info.CountryCode, false, newkeys...)
		}
		if info.Latitude != "" {
			newkeys[len(newkeys)-1] = "Latitude"
			SetMapValue(datas[i], info.Latitude, false, newkeys...)
		}
		if info.Longitude != "" {
			newkeys[len(newkeys)-1] = "Longitude"
			SetMapValue(datas[i], info.Longitude, false, newkeys...)
		}
		if info.DistrictCode != "" {
			newkeys[len(newkeys)-1] = "DistrictCode"
			SetMapValue(datas[i], info.DistrictCode, false, newkeys...)
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
			Placeholder:  "your/path/to/ip.dat(x)",
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
