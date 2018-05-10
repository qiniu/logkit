package date

import (
	"errors"
	"fmt"

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
		val, err = ConvertDate(g.LayoutBefore, g.LayoutAfter, g.Offset, val)
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
			Description:  "当前时间样式(不填自动解析)(time_layout_before)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "time_layout_after",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "期望时间样式(不填默认rfc3339)(time_layout_after)",
			Type:         transforms.TransformTypeString,
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
