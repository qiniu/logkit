package ip

import (
	"fmt"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/wangtuanjie/ip17mon"
)

//更全的免费数据可以在ipip.net下载
type IpTransformer struct {
	Key      string `json:"key"`
	DataPath string `json:"data_path"`
	loc      *ip17mon.Locator
}

func (it *IpTransformer) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err error
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
			err = fmt.Errorf("IP key %v not exist in data", it.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("IP key %v data type is not string", it.Key)
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
		err = fmt.Errorf("find total %v erorrs in transform IP, last error info is %v", errnums, err)
	}
	return datas, err
}

func (it *IpTransformer) Description() string {
	return "transform ip to country region and isp"
}

func (it *IpTransformer) SampleConfig() string {
	return `{
		"key":"MyIpFieldKey",
		"data_path":"your/path/to/ip.dat"
	}`
}

func init() {
	transforms.Add("IP", func() transforms.Transformer {
		return &IpTransformer{}
	})
}
