package aws

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const cloudTrailRecords = "Records"

var (
	_ transforms.StatsTransformer = &CloudTrail{}
	_ transforms.Transformer      = &CloudTrail{}
)

type CloudTrail struct {
	stats StatsInfo
}

func (g *CloudTrail) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("cloudtrail transformer not support rawTransform")
}

func (g *CloudTrail) Transform(datas []Data) (retdata []Data, fmtErr error) {
	var err, cloudTrailErr error
	errNum := 0
	var newData []Data
	for _, v := range datas {
		newData, cloudTrailErr = getCloudTrailData(v)
		if cloudTrailErr != nil {
			errNum, err = transforms.SetError(errNum, cloudTrailErr, transforms.General, "")
		}
		//不管有没有错误发生，都把返回的newdata加进来，让部分成功的情况也能解析出数据，同时也把原来的数据给进来
		retdata = append(retdata, newData...)
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return retdata, fmtErr
}

//如果有错误发生，要把原来的数据给回去
func getCloudTrailData(data Data) (retdata []Data, err error) {
	records, ok := data[cloudTrailRecords]
	if !ok {
		return []Data{data}, fmt.Errorf("data is not valid aws cloudtrail format as Records field is empty")
	}
	validr, ok := records.([]interface{})
	if !ok {
		return []Data{data}, fmt.Errorf("data is not valid aws cloudtrail format as Records field is not []interface{}")
	}
	var failcnt int
	var failreason string
	for _, v := range validr {
		mydata, ok := v.(map[string]interface{})
		if !ok {
			failcnt++
			failreason = "data in Records can not be assert to map[string]interface"
			continue
		}
		mydata = AddExtraField(data, mydata)
		retdata = append(retdata, mydata)
	}
	if failcnt > 0 {
		err = fmt.Errorf("we got total %v errors as %v", failcnt, failreason)
		retdata = append(retdata, data)
	}
	return
}

func AddExtraField(olddata, newData Data) Data {
	for k, v := range olddata {
		if k == cloudTrailRecords {
			continue
		}
		newData[k] = v
	}
	return newData
}

func (g *CloudTrail) Description() string {
	//return "cloudtrail can convert data to cloudtrail format data"
	return "cloudtrail transformer 将AWS CloudTrail 数据解析处理"
}

func (g *CloudTrail) Type() string {
	return "cloudtrail"
}

func (g *CloudTrail) SampleConfig() string {
	return `{
		"type":"cloudtrail"
	}`
}

func (g *CloudTrail) ConfigOptions() []Option {
	return []Option{}
}

func (g *CloudTrail) Stage() string {
	return transforms.StageAfterParser
}

func (g *CloudTrail) Stats() StatsInfo {
	return g.stats
}

func (g *CloudTrail) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("cloudtrail", func() transforms.Transformer {
		return &CloudTrail{}
	})
}
