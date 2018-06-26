package ip

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

type Number2Ip struct {
	Key   string `json:"key"`
	New   string `json:"new"`
	stats StatsInfo
}

func (g *Number2Ip) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errCount := 0
	keys := GetKeys(g.Key)
	news := GetKeys(g.New)

	for i := range datas {
		val, gerr := GetMapValue(datas[i], keys...)
		if gerr != nil {
			errCount++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		var number int64
		switch newv := val.(type) {
		case int64:
			number = newv
		case int:
			number = int64(newv)
		case int32:
			number = int64(newv)
		case int16:
			number = int64(newv)
		case uint64:
			number = int64(newv)
		case uint32:
			number = int64(newv)
		case json.Number:
			number, err = newv.Int64()
			if err != nil {
				errCount++
				err = fmt.Errorf("transform key %v data type is not int64", g.Key)
				continue
			}
		case string:
			number, err = strconv.ParseInt(newv, 10, 64)
			if err != nil {
				errCount++
				err = fmt.Errorf("transform key %v data type is not int64", g.Key)
				continue
			}
		default:
			errCount++
			err = fmt.Errorf("transform key %v data type is not correct", g.Key)
			continue
		}

		ipVal := convertIp(number)
		if len(news) == 0 {
			DeleteMapValue(datas[i], keys...)
			news = keys
		}
		serr := SetMapValue(datas[i], ipVal, false, news...)
		if serr != nil {
			errCount++
			err = fmt.Errorf("the new key %v already exists ", g.New)
		}

	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform json, last error info is %v", errCount, err)
	}
	g.stats.Errors += int64(errCount)
	g.stats.Success += int64(len(datas) - errCount)
	return datas, ferr
}

func (g *Number2Ip) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("json transformer not support rawTransform")
}

func convertIp(ipInt int64) (ip string) {
	// need to do two bit shifting and “0xff” masking
	b0 := strconv.FormatInt((ipInt>>24)&0xff, 10)
	b1 := strconv.FormatInt((ipInt>>16)&0xff, 10)
	b2 := strconv.FormatInt((ipInt>>8)&0xff, 10)
	b3 := strconv.FormatInt(ipInt&0xff, 10)
	return b0 + "." + b1 + "." + b2 + "." + b3
}

func (g *Number2Ip) Description() string {
	return `将整型数据/字符串转换为ip，如整型数据为为 223556667，转换后变为 13.83.52.59`
}

func (g *Number2Ip) Type() string {
	return "number2ip"
}

func (g *Number2Ip) SampleConfig() string {
	return `{
       "type":"number2ip",
       "key":"myParseKey",
       "new":"myNewKey"
    }`
}

func (g *Number2Ip) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
	}
}

func (g *Number2Ip) Stage() string {
	return transforms.StageAfterParser
}

func (g *Number2Ip) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("number2ip", func() transforms.Transformer {
		return &Number2Ip{}
	})
}
