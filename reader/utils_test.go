package reader

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestFindFile(t *testing.T) {
	CreateFileForTest(1000)
	defer DestroyDir()

	fi, err := getLatestFile(Dir)
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != "f1" {
		t.Errorf("Latest file is f4, not %v", fi.Name())
	}

	fi, err = getOldestFile(Dir)
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != "f3" {
		t.Errorf("Oldest file is f1, not %v", fi.Name())
	}

}

func TestCondition(t *testing.T) {
	var fi os.FileInfo
	trueCondition := NoCondition
	falseCondition := NotCondition(trueCondition)

	assert.True(t, trueCondition(fi))
	assert.False(t, falseCondition(fi))
	assert.True(t, OrCondition(trueCondition, falseCondition)(fi))
	assert.False(t, AndCondition(trueCondition, falseCondition)(fi))
}

func TestHeadPatternMode(t *testing.T) {
	headreg, err := regexp.Compile("^xx$")
	if err != nil {
		t.Error(err)
	}
	tests := []struct {
		mode   string
		value  interface{}
		experr error
	}{
		{
			mode:   "hahah",
			experr: errors.New("unknown HeadPatternMode hahah"),
		},
		{
			mode:  ReadModeHeadPatternRegexp,
			value: headreg,
		},
		{
			mode:  ReadModeHeadPatternString,
			value: "^xx$",
		},
	}
	for _, ti := range tests {
		reg, err := HeadPatternMode(ti.mode, ti.value)
		if ti.experr != nil {
			assert.EqualError(t, err, ti.experr.Error())
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, headreg, reg)
	}
	headreg, err = regexp.Compile("^{\n$")
	assert.NoError(t, err)
	ret := headreg.Match([]byte(`{
`))
	assert.Equal(t, true, ret)
}

func TestParseDuration(t *testing.T) {
	dur, err := ParseLoopDuration("loop 1s")
	assert.NoError(t, err)
	assert.Equal(t, time.Second, dur)

	dur, err = ParseLoopDuration("loop 1-")
	assert.Error(t, err)
	assert.Equal(t, time.Duration(0), dur)
}

func TestModTimeLater(t *testing.T) {
	dir := "TestModTimeLater"
	err := os.Mkdir(dir, DefaultDirPerm)
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	for _, v := range []string{"f1", "f2", "f3"} {
		err := ioutil.WriteFile(filepath.Join(dir, v), []byte("abc"), 0644)
		assert.NoError(t, err)
	}
	cs, err := GetMaxFile(dir, func(info os.FileInfo) bool { return true }, ModTimeLater)
	assert.NoError(t, err)
	assert.Equal(t, "f3", cs.Name())
	cs, err = GetMinFile(dir, func(info os.FileInfo) bool { return true }, ModTimeLater)
	assert.NoError(t, err)
	assert.Equal(t, "f1", cs.Name())
}

func TestGetTags(t *testing.T) {
	tagFile := "./tagFile.json"
	err := ioutil.WriteFile(tagFile, []byte(`{  
	   	"Title":"tags",
	    "Author":["john","ada","alice"],
	    "IsTrue":true,
	    "Host":99
	  	}`), 0644)
	assert.NoError(t, err)
	defer os.Remove(tagFile)
	err = nil
	exp := map[string]interface{}{
		"Title":  "tags",
		"Author": []interface{}{"john", "ada", "alice"},
		"IsTrue": bool(true),
		"Host":   float64(99),
	}
	tags, err := getTags(tagFile)
	assert.NoError(t, err)
	assert.Equal(t, exp, tags)
}

func TestSetMapValueExistWithPrefix(t *testing.T) {
	data1 := map[string]interface{}{
		"a": "b",
	}
	err1 := SetMapValueExistWithPrefix(data1, "newVal", "prefix", "a")
	assert.NoError(t, err1)
	exp1 := map[string]interface{}{
		"a":        "b",
		"prefix_a": "newVal",
	}
	assert.Equal(t, exp1, data1)

	data2 := map[string]interface{}{
		"a": map[string]interface{}{
			"name": "qiniu",
			"age":  45,
		},
	}
	err2 := SetMapValueExistWithPrefix(data2, "newVal", "prefix", []string{"a", "name"}...)
	assert.NoError(t, err2)
	exp2 := map[string]interface{}{
		"a": map[string]interface{}{
			"name":        "qiniu",
			"age":         45,
			"prefix_name": "newVal",
		},
	}
	assert.Equal(t, exp2, data2)

	err3 := SetMapValueExistWithPrefix(data2, "newVal", "prefix", []string{"xy", "name"}...)
	assert.NoError(t, err3)

	err4 := SetMapValueExistWithPrefix(data2, "newVal", "prefix", []string{"a", "hello"}...)
	assert.NoError(t, err4)
	exp4 := map[string]interface{}{
		"a": map[string]interface{}{
			"name":        "qiniu",
			"age":         45,
			"prefix_name": "newVal",
			"hello":       "newVal",
		},
		"xy": map[string]interface{}{
			"name": "newVal",
		},
	}
	assert.Equal(t, exp4, data2)
}

func TestCompressFile(t *testing.T) {
	assert.Equal(t, true, CompressedFile("abc.gz"))
	assert.Equal(t, true, CompressedFile("xxx.tar"))
	assert.Equal(t, true, CompressedFile("as.zip"))
	assert.Equal(t, true, CompressedFile("123.tar.gz"))
	assert.Equal(t, false, CompressedFile("abc.taxs"))
}

func TestIgnoreFileSuffixes(t *testing.T) {
	suffixes := []string{".swap", ".gz", ".tar"}
	assert.Equal(t, false, IgnoreFileSuffixes("abc.log", suffixes))
	assert.Equal(t, true, IgnoreFileSuffixes("abc.tar", suffixes))
	assert.Equal(t, true, IgnoreFileSuffixes("abc.swap", suffixes))
}

func TestValidFile(t *testing.T) {
	assert.Equal(t, true, ValidFileRegex("abc.log", "*.log"))
	assert.Equal(t, true, ValidFileRegex("abc.1", "abc.[1-9]"))
	assert.Equal(t, false, ValidFileRegex("abc.1", "abc.[1-9]1"))
	assert.Equal(t, false, ValidFileRegex("abc.swap", "abc.s[*"))
	assert.Equal(t, true, ValidFileRegex("abc.swap", "abc.s*"))
	assert.Equal(t, true, ValidFileRegex("abc.log", ""))
}

func TestIgnoreHidden(t *testing.T) {
	assert.Equal(t, true, IgnoreHidden(".log", true))
	assert.Equal(t, false, IgnoreHidden(".1", false))
}

func TestParseNumber(t *testing.T) {
	tests := []struct {
		str    string
		expect int
	}{
		{
			str:    "01",
			expect: 1,
		},
		{
			str:    "001",
			expect: 01,
		},
		{
			str:    "10",
			expect: 10,
		},
		{
			str:    "10",
			expect: 10,
		},
	}

	for _, test := range tests {
		actual, err := ParseNumber(test.str)
		assert.Nil(t, err)
		assert.EqualValues(t, test.expect, actual)
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		timeStr      string
		expectHour   int
		expectMinute int
	}{
		{
			timeStr: "   ",
		},
		{
			timeStr:      " 12:21 ",
			expectHour:   12,
			expectMinute: 21,
		},
		{
			timeStr:      " 25:21 ",
			expectHour:   1,
			expectMinute: 21,
		},
		{
			timeStr:      " 24:60",
			expectHour:   0,
			expectMinute: 00,
		},
		{
			timeStr:      "01:02 ",
			expectHour:   1,
			expectMinute: 2,
		},
		{
			timeStr:      "01:30",
			expectHour:   1,
			expectMinute: 30,
		},
		{
			timeStr:      "01 : 30",
			expectHour:   1,
			expectMinute: 30,
		},
	}

	for _, test := range tests {
		actualHour, actualMinute, err := ParseTime(test.timeStr)
		assert.Nil(t, err)
		assert.EqualValues(t, test.expectHour, actualHour)
		assert.EqualValues(t, test.expectMinute, actualMinute)
	}
}

func TestParseRunTime(t *testing.T) {
	tests := []struct {
		runTimeStr string
		runTime    RunTime
		err        error
	}{
		{
			runTimeStr: "",
			err:        errors.New("empty string, must be 'hh:mm' (use 24 hour)"),
		},
		{
			runTimeStr: "-",
		},
		{
			runTimeStr: "12:00",
			runTime: RunTime{
				StartHour: 12,
			},
		},
		{
			runTimeStr: "12:00-",
			runTime: RunTime{
				StartHour: 12,
			},
		},
		{
			runTimeStr: "-12:00",
			runTime: RunTime{
				EndHour: 12,
			},
		},
		{
			runTimeStr: "12:00-12:1",
			runTime: RunTime{
				StartHour: 12,
				EndHour:   12,
				EndMin:    1,
			},
		},
		{
			runTimeStr: "02:30-24:02",
			runTime: RunTime{
				StartHour: 2,
				StartMin:  30,
				EndHour:   0,
				EndMin:    2,
			},
		},
		{
			runTimeStr: "12-12",
			runTime: RunTime{
				StartHour: 12,
				EndHour:   12,
			},
		},
		{
			runTimeStr: "12-24",
			runTime: RunTime{
				StartHour: 12,
				EndHour:   0,
			},
		},
		{
			runTimeStr: "12-11",
			runTime: RunTime{
				StartHour: 12,
				EndHour:   11,
			},
		},
		{
			runTimeStr: "12-02",
			runTime: RunTime{
				StartHour: 12,
				EndHour:   2,
			},
		},
	}

	for _, test := range tests {
		actualRunTime, actualErr := ParseRunTime(test.runTimeStr)
		assert.EqualValues(t, test.err, actualErr)
		assert.EqualValues(t, test.runTime, actualRunTime)
	}
}

func TestInRunTime(t *testing.T) {
	tests := []struct {
		hour, minute int
		runTime      RunTime
		expect       bool
	}{
		{
			expect: true,
		},
		{
			hour:    1,
			minute:  1,
			runTime: RunTime{},
			expect:  true,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				StartHour: 1,
				StartMin:  0,
			},
			expect: true,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				StartHour: 0,
				StartMin:  30,
			},
			expect: true,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				StartHour: 1,
				StartMin:  30,
			},
			expect: false,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				EndHour: 12,
				EndMin:  1,
			},
			expect: true,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				EndHour: 1,
			},
			expect: false,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				EndHour: 1,
				EndMin:  30,
			},
			expect: true,
		},
		{
			hour:   1,
			minute: 1,
			runTime: RunTime{
				StartHour: 1,
				StartMin:  0,
				EndHour:   1,
				EndMin:    30,
			},
			expect: true,
		},
	}

	for _, test := range tests {
		actual := InRunTime(test.hour, test.minute, test.runTime)
		assert.EqualValues(t, test.expect, actual)
	}
}

func TestParseRunTimeWithMode(t *testing.T) {
	tests := []struct {
		mode          string
		value         interface{}
		expectRunTime RunTime
	}{
		{
			mode:  ReadModeRunTimeString,
			value: "12-",
			expectRunTime: RunTime{
				StartHour: 12,
			},
		},
		{
			mode:  ReadModeRunTimeString,
			value: "12-24",
			expectRunTime: RunTime{
				StartHour: 12,
				EndHour:   0,
			},
		},
		{
			mode:  ReadModeRunTimeString,
			value: "12-15",
			expectRunTime: RunTime{
				StartHour: 12,
				EndHour:   15,
			},
		},
		{
			mode:          ReadModeRunTimeStruct,
			value:         RunTime{},
			expectRunTime: RunTime{},
		},
		{
			mode: ReadModeRunTimeStruct,
			value: RunTime{
				StartHour: 23,
				EndHour:   1,
			},
			expectRunTime: RunTime{
				StartHour: 23,
				EndHour:   1,
			},
		},
	}

	for _, test := range tests {
		actualRunTime, err := ParseRunTimeWithMode(test.mode, test.value)
		assert.Nil(t, err)
		assert.EqualValues(t, test.expectRunTime, actualRunTime)
	}
}
