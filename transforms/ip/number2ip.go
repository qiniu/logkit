package ip

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Number2Ip{}
	_ transforms.Transformer      = &Number2Ip{}
)

type Number2Ip struct {
	Key   string `json:"key"`
	New   string `json:"new"`
	stats StatsInfo
}

func (g *Number2Ip) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr, convertErr error
	errNum := 0
	keys := GetKeys(g.Key)
	news := GetKeys(g.New)

	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		var number int64
		switch newVal := val.(type) {
		case int64:
			number = newVal
		case int:
			number = int64(newVal)
		case int32:
			number = int64(newVal)
		case int16:
			number = int64(newVal)
		case uint64:
			number = int64(newVal)
		case uint32:
			number = int64(newVal)
		case json.Number:
			number, convertErr = newVal.Int64()
			if convertErr != nil {
				errNum++
				err = fmt.Errorf("transform key %v data type is not int64", g.Key)
				continue
			}
		case string:
			number, convertErr = strconv.ParseInt(newVal, 10, 64)
			if convertErr != nil {
				errNum, err = transforms.SetError(errNum, convertErr, transforms.General, "")
				continue
			}
		default:
			typeErr := fmt.Errorf("transform key %v data type is not correct", g.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}

		ipVal := convertIp(number)
		if len(news) == 0 {
			DeleteMapValue(datas[i], keys...)
			news = keys
		}
		setErr := SetMapValue(datas[i], ipVal, false, news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
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
	return `将整型数据/字符串转换为ip, 我们认为该数字为32位2进制数的十进制表示法，如整型数据为为 223556667，转换后变为 13.83.52.59`
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

func (g *Number2Ip) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("number2ip", func() transforms.Transformer {
		return &Number2Ip{}
	})
}
