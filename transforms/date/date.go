package date

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

type DateTrans struct {
	Key          string `json:"key"`
	Offset       int    `json:"offset"`
	LayoutBefore string `json:"time_layout_before"`
	LayoutAfter  string `json:"time_layout_after"`
	stats        StatsInfo
}

func (g *DateTrans) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("date transformer not support rawTransform")
}

func (g *DateTrans) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	keys := GetKeys(g.Key)
	for i := range datas {
		val, gerr := GetMapValue(datas[i], keys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		val, err = g.convertDate(val)
		if err != nil {
			errnums++
			continue
		}
		SetMapValue(datas[i], val, false, keys...)
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform date, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *DateTrans) convertDate(v interface{}) (interface{}, error) {
	var s int64
	switch newv := v.(type) {
	case int64:
		s = newv
	case int:
		s = int64(newv)
	case int32:
		s = int64(newv)
	case int16:
		s = int64(newv)
	case uint64:
		s = int64(newv)
	case uint32:
		s = int64(newv)
	case string:
		if g.LayoutBefore != "" {
			t, err := time.Parse(g.LayoutBefore, newv)
			if err != nil {
				return v, fmt.Errorf("can not parse %v with layout %v", newv, g.LayoutBefore)
			}
			return g.formatWithUserOption(t), nil
		}
		t, err := times.StrToTime(newv)
		if err != nil {
			return v, err
		}
		return g.formatWithUserOption(t), nil
	case json.Number:
		jsonNumber, err := newv.Int64()
		if err != nil {
			return v, err
		}
		s = jsonNumber
	default:
		return v, fmt.Errorf("can not parse %v type %v as date time", v, reflect.TypeOf(v))
	}
	news := s
	timestamp := strconv.FormatInt(news, 10)
	timeSecondPrecision := 16
	//补齐16位
	for i := len(timestamp); i < timeSecondPrecision; i++ {
		timestamp += "0"
	}
	// 取前16位，截取精度 微妙
	timestamp = timestamp[0:timeSecondPrecision]
	t, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return v, err
	}
	tm := time.Unix(0, t*int64(time.Microsecond))
	return g.formatWithUserOption(tm), nil
}

func (g *DateTrans) formatWithUserOption(t time.Time) interface{} {
	t = t.Add(time.Duration(g.Offset) * time.Hour)
	if g.LayoutAfter != "" {
		return t.Format(g.LayoutAfter)
	}
	return t.Format(time.RFC3339Nano)
}

func (g *DateTrans) Description() string {
	//return "transform string/long to specified date format"
	return "将string/long数据转换成指定的时间格式, 如 1523878855 变为 2018-04-16T19:40:55+08:00"
}

func (g *DateTrans) Type() string {
	return "date"
}

func (g *DateTrans) SampleConfig() string {
	return `{
		"type":"date",
		"key":"DateFieldKey",
		"offset":0,
		"time_layout_before":"",
		"time_layout_after":"2006-01-02T15:04:05Z07:00"
	}`
}

func (it *DateTrans) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyTimezoneoffset,
		{
			KeyName:      "time_layout_before",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "时间样式(不填自动解析)(time_layout_before)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "time_layout_after",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "解析后时间样式(不填默认rfc3339)(time_layout_after)",
			Type:         transforms.TransformTypeString,
			Advance:      true,
		},
	}
}

func (g *DateTrans) Stage() string {
	return transforms.StageAfterParser
}

func (g *DateTrans) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("date", func() transforms.Transformer {
		return &DateTrans{}
	})
}
