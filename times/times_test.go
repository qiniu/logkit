package times

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type test struct {
	time    time.Time
	format  string
	strTime string
}

var testCases = []test{
	{
		time.Date(2012, 11, 22, 21, 28, 10, 0, time.Local),
		"Y-m-d H:i:s",
		"2012-11-22 21:28:10",
	},
	{
		time.Date(2012, 11, 22, 0, 0, 0, 0, time.Local),
		"Y-m-d",
		"2012-11-22",
	},
	{
		time.Date(2012, 11, 22, 21, 28, 10, 0, time.Local),
		"Y-m-d H:i:s",
		"2012-11-22 21:28:10",
	},
}

func TestFormat(t *testing.T) {
	for _, testCase := range testCases {
		strTime := Format(testCase.format, testCase.time)
		if strTime != testCase.strTime {
			t.Errorf("(expected) %v != %v (actual)", testCase.time, strTime)
		}
	}
}

func TestStrToTime(t *testing.T) {

	zoneName, zoneValue := GetTimeZone()
	if len(zoneValue) > 0 {
		zoneValue += " " + zoneName
	}
	var testCases = []test{
		{
			time.Date(2012, 11, 22, 21, 28, 10, 0, time.Local),
			"",
			"2012-11-22 21:28:10" + zoneValue,
		},
		{
			time.Date(2012, 11, 22, 0, 0, 0, 0, time.Local),
			"",
			"2012/11/22" + zoneValue,
		},
		{
			time.Date(2012, 11, 22, 21, 28, 10, 0, time.Local),
			"",
			"2012-11-22 21:28:10" + zoneValue,
		},
		{
			time.Date(2016, 10, 20, 17, 20, 30, 600000000, time.Local),
			"",
			"2016/10/20 17:20:30.600000" + zoneValue,
		},
	}
	for _, testCase := range testCases {
		time, err := StrToTime(testCase.strTime)
		if err != nil {
			t.Error(err)
		}
		if !time.Equal(testCase.time) {
			t.Errorf("(expected) %v != %v (actual)", time, testCase.time)
		}
	}
}

func TestStrToTime2(t *testing.T) {
	testcase := "2016-10-20T21:44:14.989944Z"
	_, err := StrToTime(testcase)
	if err != nil {
		t.Error(err)
	}
	testcase = "2017/05/18 16:10:10.000000"
	tm, err := StrToTime(testcase)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(tm.String())
	nt := tm.Format(time.RFC3339)
	fmt.Println(nt)
	testcase = "2017-06-05T21:00:18+08:00"
	tm, err = StrToTime(testcase)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(tm.String())
	fmt.Println(time.Now().Format(time.RFC3339))
}

func TestAddLayout(t *testing.T) {
	tm, err := StrToTime("[02/Jan/2017:15:04:05 -0700]")
	if err == nil {
		t.Error(errors.New("should have error without layouts"))
	}
	AddLayout([]string{"[02/Jan/2006:15:04:05 -0700]"})
	tm, err = StrToTime("[02/Jan/2017:15:04:05 -0700]")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(tm.String())
	AddLayout([]string{"2006-1-02 15:04:05"})
	tm, err = StrToTime("2017-7-11 14:39:24")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(tm.String())

}

func a1() map[string]interface{} {
	x := map[string]interface{}{
		"a": 1,
		"b": "2",
		"c": 1,
		"d": "xx",
	}
	for k := range x {
		if k == "b" || k == "a" || k == "c" {
			delete(x, k)
		}
	}
	return x
}

func a2() map[string]interface{} {
	x := map[string]interface{}{
		"a": 1,
		"b": "2",
		"c": 1,
		"d": "xx",
	}
	x2 := make(map[string]interface{})
	for k, v := range x {
		if k != "b" && k != "a" && k != "c" {
			x2[k] = v
		}
	}
	return x2
}

func BenchmarkTest1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		a1()
	}
}

func BenchmarkTest2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		a2()
	}
}
