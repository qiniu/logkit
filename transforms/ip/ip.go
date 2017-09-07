package ip

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	"github.com/wangtuanjie/ip17mon"
)

//更全的免费数据可以在ipip.net下载
type IpTransformer struct {
	StageTime string `json:"stage"`
	Key       string `json:"key"`
	DataPath  string `json:"data_path"`
	loc       *ip17mon.Locator
	stats     utils.StatsInfo
}

func (it *IpTransformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("IP transformer not support rawTransform")
}

func (it *IpTransformer) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	if it.loc == nil {
		it.loc, err = ip17mon.NewLocator(it.DataPath)
		if err != nil {
			return datas, err
		}
	}
	errnums := 0
	for i := range datas {
		val, ok := datas[i][it.Key]
		if !ok {
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
		datas[i]["Region"] = info.Region
		datas[i]["City"] = info.City
		datas[i]["Country"] = info.Country
		datas[i]["Isp"] = info.Isp
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
	return "transform ip to country region and isp"
}

func (g *IpTransformer) Type() string {
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

func (it *IpTransformer) Stage() string {
	if it.StageTime == "" {
		return transforms.StageAfterParser
	}
	return it.StageTime
}

func (it *IpTransformer) Stats() utils.StatsInfo {
	return it.stats
}

func init() {
	transforms.Add("IP", func() transforms.Transformer {
		return &IpTransformer{}
	})
}
