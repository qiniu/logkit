package date

import (
	"errors"
	"time"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Transformer{}
	_ transforms.Transformer      = &Transformer{}
)

type Transformer struct {
	Key          string `json:"key"`
	Offset       int    `json:"offset"`
	LayoutBefore string `json:"time_layout_before"`
	LayoutAfter  string `json:"time_layout_after"`
	stats        StatsInfo
}

func (t *Transformer) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("date transformer not support rawTransform")
}

func (t *Transformer) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	keys := GetKeys(t.Key)
	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, t.Key)
			continue
		}

		// 如果用户设置了 offset，则不默认使用本地时区
		loc := time.Local
		if t.Offset != 0 {
			loc = time.UTC
		}

		val, convertErr := ConvertDate(t.LayoutBefore, t.LayoutAfter, t.Offset, loc, val)
		if convertErr != nil {
			errNum, err = transforms.SetError(errNum, convertErr, transforms.General, "")
			continue
		}
		setErr := SetMapValue(datas[i], val, false, keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, t.Key)
		}
	}

	t.stats, fmtErr = transforms.SetStatsInfo(err, t.stats, int64(errNum), int64(len(datas)), t.Type())
	return datas, fmtErr
}

func (t *Transformer) Description() string {
	//return "transform string/long to specified date format"
	return "将string/long数据转换成指定的时间格式, 如 1523878855 变为 2018-04-16T19:40:55+08:00"
}

func (t *Transformer) Type() string {
	return "date"
}

func (t *Transformer) SampleConfig() string {
	return `{
		"type":"date",
		"key":"DateFieldKey",
		"offset":0,
		"time_layout_before":"",
		"time_layout_after":"2006-01-02T15:04:05Z07:00"
	}`
}

func (t *Transformer) ConfigOptions() []Option {
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
			Advance:      true,
			Description:  "期望时间样式(不填默认rfc3339)(time_layout_after)",
			Type:         transforms.TransformTypeString,
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
	transforms.Add("date", func() transforms.Transformer {
		return &Transformer{}
	})
}
