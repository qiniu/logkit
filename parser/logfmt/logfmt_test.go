package logfmt

import (
	"fmt"
	"testing"

	"github.com/qiniu/logkit/parser"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	bench    []Data
	testData = utils.GetParseTestData("ts=2018-01-02T03:04:05.123Z lvl=info msg=\"http request\" method=PUT\nduration=1.23 log_id=123456abc", DefaultMaxBatchSize)
)

// now: 10	 150631356 ns/op	routine = 1  (2MB)
// now: 10	 139839802 ns/op	routine = 2  (2MB)
func Benchmark_ParseLine(b *testing.B) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	p, _ := NewParser(c)

	var m []Data
	for n := 0; n < b.N; n++ {
		m, _ = p.Parse(testData)
	}
	bench = m
}

func Test_parseLine(t *testing.T) {
	tests := []struct {
		line       string
		keepString bool
		expectData []Data
		existErr   bool
		splitter   string
	}{
		{
			expectData: []Data{},
			existErr:   true,
			splitter:   "=",
		},
		{
			line: "foo=\"bar\"",
			expectData: []Data{
				{"foo": "bar"},
			},
			splitter: "=",
		},
		{
			line: "foo=\"bar\"\n",
			expectData: []Data{
				{"foo": "bar"},
			},
			splitter: "=",
		},
		{
			line: "ts=2018-01-02T03:04:05.123Z lvl=info msg=\"http request\" method=PUT\nduration=1.23 log_id=123456abc",
			expectData: []Data{
				{
					"lvl":      "info",
					"msg":      "http request",
					"method":   "PUT",
					"ts":       "2018-01-02T03:04:05.123Z",
					"duration": 1.23,
					"log_id":   "123456abc",
				},
			},
			splitter: "=",
		},
		{
			line: `ts=2018-01-02T03:04:05.123Z lvl=info  method=PUT msg="http request" a=12345678901234567890123456789`,
			expectData: []Data{
				{
					"lvl":    "info",
					"msg":    "http request",
					"method": "PUT",
					"ts":     "2018-01-02T03:04:05.123Z",
					"a":      1.2345678901234568e+28,
				},
			},
			splitter: "=",
		},
		{
			line:       `ts=2018-01-02T03:04:05.123Z lvl=info  method=PUT msg="http request" a=12345678901234567890123456789`,
			keepString: true,
			expectData: []Data{
				{
					"lvl":    "info",
					"msg":    "http request",
					"method": "PUT",
					"ts":     "2018-01-02T03:04:05.123Z",
					"a":      "12345678901234567890123456789",
				},
			},
			splitter: "=",
		},
		{
			line:       `no data.`,
			expectData: []Data{},
			existErr:   true,
			splitter:   "=",
		},
		{
			line:       `foo="" bar=`,
			expectData: []Data{},
			existErr:   true,
			splitter:   "=",
		},
		{
			line: `abc=abc foo="def`,
			expectData: []Data{
				{
					"abc": "abc",
					"foo": "\"def",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line:       `"foo=" bar=abc`,
			expectData: []Data{},
			existErr:   true,
			splitter:   "=",
		},
	}
	l := Parser{
		name: TypeLogfmt,
	}
	for _, tt := range tests {
		l.keepString = tt.keepString
		l.splitter = tt.splitter
		got, err := l.parse(tt.line)
		fmt.Println("got: ", got, " err: ", err)
		assert.Equal(t, tt.existErr, err != nil)
		assert.Equal(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		s          []string
		expectData []Data
	}{
		{
			expectData: []Data{},
		},
		{
			s: []string{`ts=2018-01-02T03:04:05.123Z  CST   lvl=5 msg="error" isTest=true log_id=123456abc`},
			expectData: []Data{
				{
					"ts":     "2018-01-02T03:04:05.123Z  CST",
					"lvl":    float64(5),
					"msg":    "error",
					"isTest": true,
					"log_id": "123456abc",
				},
			},
		},
		{
			s: []string{"\nts=2018-01-02T03:04:05.123Z lvl=5 msg=\"error\" log_id=123456abc\nmethod=PUT duration=1.23 \n"},
			expectData: []Data{
				{
					"ts":       "2018-01-02T03:04:05.123Z",
					"lvl":      float64(5),
					"msg":      "error",
					"log_id":   "123456abc",
					"method":   "PUT",
					"duration": 1.23,
				},
			},
		},
	}
	l, err := NewParser(conf.MapConf{
		KeyParserName: TypeLogfmt,
	})
	assert.Nil(t, err)

	lType, ok := l.(parser.ParserType)
	assert.True(t, ok)
	assert.EqualValues(t, TypeKeyValue, lType.Type())

	assert.EqualValues(t, TypeLogfmt, l.Name())

	for _, tt := range tests {
		got, err := l.Parse(tt.s)
		if c, ok := err.(*StatsError); ok {
			assert.Equal(t, int64(0), c.Errors)
		}

		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}

	got, err := l.Parse([]string{"", "a"})
	assert.NotNil(t, err)
	assert.EqualValues(t, "success 0 errors 1 last error no splitter exist, will keep origin data in pandora_stash if disable_record_errdata field is false, send error detail <nil>", err.Error())
	assert.EqualValues(t, []Data{{"pandora_stash": "a"}}, got)

	l, err = NewParser(conf.MapConf{
		KeyParserName:  TypeLogfmt,
		KeyKeepRawData: "true",
	})
	assert.Nil(t, err)

	got, err = l.Parse([]string{"lvl= "})
	assert.NotNil(t, err)
	assert.EqualValues(t, []Data{{"pandora_stash": "lvl=", "raw_data": "lvl="}}, got)

	got, err = l.Parse([]string{" =50"})
	assert.NotNil(t, err)
	assert.EqualValues(t, []Data{{"pandora_stash": "=50", "raw_data": "=50"}}, got)

	got, err = l.Parse([]string{"lvl=50next=sam"})
	assert.Nil(t, err)
	assert.EqualValues(t, []Data{{"lvl": "50next=sam", "raw_data": "lvl=50next=sam"}}, got)

	got, err = l.Parse([]string{"lvl=50\n=sam"})
	assert.NotNil(t, err)
	assert.EqualValues(t, []Data{{"pandora_stash": "lvl=50\n=sam", "raw_data": "lvl=50\n=sam"}}, got)

	got, err = l.Parse([]string{"", "a"})
	assert.NotNil(t, err)
	assert.EqualValues(t, []Data{{"pandora_stash": "a", "raw_data": "a"}}, got)

	l, err = NewParser(conf.MapConf{
		KeyParserName:           TypeLogfmt,
		KeyDisableRecordErrData: "true",
	})
	assert.Nil(t, err)

	got, err = l.Parse([]string{"", "a"})
	assert.NotNil(t, err)
	assert.EqualValues(t, 0, len(got))
}

func TestParseWithKeepRawData(t *testing.T) {
	tests := []struct {
		s          []string
		expectData []Data
		splitter   string
	}{
		{
			expectData: []Data{},
			splitter:   "=",
		},
		{
			s: []string{`ts=2018-01-02T03:04:05.123Z lvl=5 msg="error" log_id=123456abc`},
			expectData: []Data{
				{
					"ts":       "2018-01-02T03:04:05.123Z",
					"lvl":      float64(5),
					"msg":      "error",
					"log_id":   "123456abc",
					"raw_data": `ts=2018-01-02T03:04:05.123Z lvl=5 msg="error" log_id=123456abc`,
				},
			},
			splitter: "=",
		},
		{
			s: []string{"ts=2018-01-02T03:04:05.123Z lvl=5 msg=\"error\" log_id=123456abc\nmethod=PUT duration=1.23 log_id=123456abc"},
			expectData: []Data{
				{
					"ts":       "2018-01-02T03:04:05.123Z",
					"lvl":      float64(5),
					"msg":      "error",
					"log_id":   "123456abc",
					"method":   "PUT",
					"duration": 1.23,
					"raw_data": "ts=2018-01-02T03:04:05.123Z lvl=5 msg=\"error\" log_id=123456abc\nmethod=PUT duration=1.23 log_id=123456abc",
				},
			},
			splitter: "=",
		},
		{
			s: []string{"ts:2018-01-02T03:04:05.123Z lvl:5 msg:\"error\" log_id:123456abc\nmethod:PUT duration:1.23 log_id:123456abc"},
			expectData: []Data{
				{
					"ts":       "2018-01-02T03:04:05.123Z",
					"lvl":      float64(5),
					"msg":      "error",
					"log_id":   "123456abc",
					"method":   "PUT",
					"duration": 1.23,
					"raw_data": "ts:2018-01-02T03:04:05.123Z lvl:5 msg:\"error\" log_id:123456abc\nmethod:PUT duration:1.23 log_id:123456abc",
				},
			},
			splitter: ":",
		},
	}
	l := Parser{
		name:        TypeLogfmt,
		keepRawData: true,
		numRoutine:  1,
	}
	for _, tt := range tests {
		l.splitter = tt.splitter
		got, err := l.Parse(tt.s)
		if c, ok := err.(*StatsError); ok {
			assert.Equal(t, int64(0), c.Errors)
		}

		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}
}

// 获取测试数据
func GetParseTestData(line string, size int) []string {
	testSlice := make([]string, 0)
	totalSize := 0
	for {
		if totalSize > size {
			return testSlice
		}
		testSlice = append(testSlice, line)
		totalSize += len(line)
	}
}

func Test_splitKV(t *testing.T) {

	tests := []struct {
		line       string
		expectData []string
		existErr   bool
		splitter   string
	}{
		{
			line: "foo=",
			expectData: []string{
				"foo",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "=def",
			expectData: []string{
				"",
				"def",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "foo=def abc = abc ",
			expectData: []string{
				"foo",
				"def",
				"abc",
				"abc",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "foo\n=def\nabc=a\nbc",
			expectData: []string{
				"abc",
				"abc",
				"foo",
				"def",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "foo=def \n abc =abc",
			expectData: []string{
				"abc",
				"abc",
				"foo",
				"def",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "foo=def\n abc=abc",
			expectData: []string{
				"abc",
				"abc",
				"foo",
				"def",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "foo=def\n test abc=abc",
			expectData: []string{
				"foo",
				"def test",
				"abc",
				"abc",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "time=2018-01-02T03:04:05.123Z \nCST abc=abc",
			expectData: []string{
				"time",
				"2018-01-02T03:04:05.123Z CST",
				"abc",
				"abc",
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: "foo:de:f ad abc:a:b:c",
			expectData: []string{
				"foo",
				"de:f ad",
				"abc",
				"a:b:c",
			},
			existErr: false,
			splitter: ":",
		},
		{
			line: "f:o:o::def",
			expectData: []string{
				"f:o:o",
				"def",
			},
			existErr: false,
			splitter: "::",
		},
		{
			line:       "f:o:o:def",
			expectData: []string{},
			existErr:   true,
			splitter:   "::",
		},
		{
			line: "f:o:o:\n:def",
			expectData: []string{
				"f:o:o",
				"def",
			},
			existErr: false,
			splitter: "::",
		},
		{
			line: "f:\no::a o:\n:def",
			expectData: []string{
				"f:o",
				"a",
				"o",
				"def",
			},
			existErr: false,
			splitter: "::",
		},
		{
			line:       "f:o:o: \n :def",
			expectData: []string{},
			existErr:   true,
			splitter:   "::",
		},
	}
	for _, tt := range tests {
		got, err := splitKV(tt.line, tt.splitter)
		assert.Equal(t, tt.existErr, err != nil)
		assert.Equal(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}

	}
}
