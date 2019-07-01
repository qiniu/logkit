package times

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var layouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"02/Jan/2006:15:04:05 -0700",
	"2006/01/02 15:04:05",
	`20060102T150405-07`,
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05 -0700",
	`2006-01-02T15:04:05-07:00`,
	"2006-01-02 15:04:05",
	"2006/01/02 15:04:05 -0700 MST",
	"2006/01/02 15:04:05 -0700",
	"2006-01-02 -0700 MST",
	"2006-01-02 -0700",
	"2006-01-02",
	"2006/01/02 -0700 MST",
	"2006/01/02 -0700",
	"2006/01/02",
	"02/01/2006--15:04:05",
	"02 Jan 06 15:04",
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
	time.Kitchen,
	time.Stamp,
	time.StampMilli,
	time.StampMicro,
	time.StampNano,
}

// AddLayout 可以增加用户自定义的时间类型
func AddLayout(udfLayouts []string) {
	layouts = append(udfLayouts, layouts...)
}

// Format 跟 PHP 中 date 类似的使用方式，如果 ts 没传递，则使用当前时间
func Format(format string, ts ...time.Time) string {
	patterns := []string{
		// 年
		"Y", "2006", // 4 位数字完整表示的年份
		"y", "06", // 2 位数字表示的年份

		// 月
		"m", "01", // 数字表示的月份，有前导零
		"n", "1", // 数字表示的月份，没有前导零
		"M", "Jan", // 三个字母缩写表示的月份
		"F", "January", // 月份，完整的文本格式，例如 January 或者 March

		// 日
		"d", "02", // 月份中的第几天，有前导零的 2 位数字
		"j", "2", // 月份中的第几天，没有前导零

		"D", "Mon", // 星期几，文本表示，3 个字母
		"l", "Monday", // 星期几，完整的文本格式;L的小写字母

		// 时间
		"g", "3", // 小时，12 小时格式，没有前导零
		"G", "15", // 小时，24 小时格式，没有前导零
		"h", "03", // 小时，12 小时格式，有前导零
		"H", "15", // 小时，24 小时格式，有前导零

		"a", "pm", // 小写的上午和下午值
		"A", "PM", // 小写的上午和下午值

		"i", "04", // 有前导零的分钟数
		"s", "05", // 秒数，有前导零
	}
	replacer := strings.NewReplacer(patterns...)
	format = replacer.Replace(format)

	t := time.Now()
	if len(ts) > 0 {
		t = ts[0]
	}
	return t.Format(format)
}

func GetTimeZone() (zoneName, zoneValue string) {
	zoneName, offset := time.Now().Zone()

	value := offset / 3600 * 100
	if value > 0 {
		return zoneName, fmt.Sprintf(" +%04d", value)
	}
	return zoneName, fmt.Sprintf(" -%04d", value)
}

func StrToTimeLocation(value string, loc *time.Location) (time.Time, error) {
	if value == "" {
		return time.Now(), errors.New("empty time string")
	}

	var t time.Time
	var err error
	for _, layout := range layouts {
		t, err = time.ParseInLocation(layout, value, loc)
		if err == nil {
			return t, nil
		}
	}
	return time.Now(), fmt.Errorf("can not find any layout to parse %v", value)
}

func StrToTime(value string) (time.Time, error) {
	return StrToTimeLocation(value, time.UTC)
}
