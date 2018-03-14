package aws

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

type ClockTrail struct {
	stats utils.StatsInfo
}

func (g *ClockTrail) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("clocktrail transformer not support rawTransform")
}

func (g *ClockTrail) Transform(datas []Data) (retdata []Data, ferr error) {
	var err error
	errnums := 0
	var newdata []Data
	for _, v := range datas {
		newdata, err = getClockTrailData(v)
		if err != nil {
			errnums++
		}
		//不管有没有错误发生，都把返回的newdata加进来，让部分成功的情况也能解析出数据，同时也把原来的数据给进来
		retdata = append(retdata, newdata...)
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform clocktrail, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return
}

//如果有错误发生，要把原来的数据给回去
func getClockTrailData(data Data) (retdata []Data, err error) {
	records, ok := data["Records"]
	if !ok {
		return []Data{data}, fmt.Errorf("data is not valid aws clocktrail format as Records field is empty")
	}
	validr, ok := records.([]interface{})
	if !ok {
		return []Data{data}, fmt.Errorf("data is not valid aws clocktrail format as Records field is not []interface{}")
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
		retdata = append(retdata, mydata)
	}
	if failcnt > 0 {
		err = fmt.Errorf("we got total %v errors as %v", failcnt, failreason)
		retdata = append(retdata, data)
	}
	return
}

func (g *ClockTrail) Description() string {
	//return "clocktrail can convert data to clocktrail format data"
	return "clocktrail transformer 将aws clock trail数据解析处理"
}

func (g *ClockTrail) Type() string {
	return "clocktrail"
}

func (g *ClockTrail) SampleConfig() string {
	return `{
		"type":"clocktrail"
	}`
}

func (g *ClockTrail) ConfigOptions() []Option {
	return []Option{}
}

func (g *ClockTrail) Stage() string {
	return transforms.StageAfterParser
}

func (g *ClockTrail) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("clocktrail", func() transforms.Transformer {
		return &ClockTrail{}
	})
}
