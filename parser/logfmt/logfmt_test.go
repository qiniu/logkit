package logfmt

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
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
	c[parser.KeyParserName] = "testparser"
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
		expectData []Data
		existErr   bool
	}{
		{
			expectData: []Data{},
		},
		{
			line: "foo=\"bar\"",
			expectData: []Data{
				{"foo": "bar"},
			},
		},
		{
			line: "foo=\"bar\"\n",
			expectData: []Data{
				{"foo": "bar"},
			},
		},
		{
			line: "ts=2018-01-02T03:04:05.123Z lvl=info msg=\"http request\" method=PUT\nduration=1.23 log_id=123456abc",
			expectData: []Data{
				{
					"lvl":    "info",
					"msg":    "http request",
					"method": "PUT",
					"ts":     "2018-01-02T03:04:05.123Z",
				},
				{
					"duration": 1.23,
					"log_id":   "123456abc",
				},
			},
		},
		{
			line: `ts=2018-01-02T03:04:05.123Z lvl=info  method=PUT msg="http request"`,
			expectData: []Data{
				{
					"lvl":    "info",
					"msg":    "http request",
					"method": "PUT",
					"ts":     "2018-01-02T03:04:05.123Z",
				},
			},
		},
		{
			line:       `no data.`,
			expectData: []Data{},
			existErr:   false,
		},
		{
			line:       `foo="" bar=`,
			expectData: []Data{},
			existErr:   false,
		},
		{
			line:       `abc=abc foo="def`,
			expectData: []Data{},
			existErr:   true,
		},
		{
			line:       `"foo=" bar=abc`,
			expectData: []Data{},
			existErr:   true,
		},
	}
	l := Parser{
		name: parser.TypeLogfmt,
	}
	for _, tt := range tests {

		got, err := l.parse(tt.line)
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
			s: []string{`ts=2018-01-02T03:04:05.123Z lvl=5 msg="error" log_id=123456abc`},
			expectData: []Data{
				{
					"ts":     "2018-01-02T03:04:05.123Z",
					"lvl":    int64(5),
					"msg":    "error",
					"log_id": "123456abc",
				},
			},
		},
		{
			s: []string{"ts=2018-01-02T03:04:05.123Z lvl=5 msg=\"error\" log_id=123456abc\nmethod=PUT duration=1.23 log_id=123456abc"},
			expectData: []Data{
				{
					"ts":     "2018-01-02T03:04:05.123Z",
					"lvl":    int64(5),
					"msg":    "error",
					"log_id": "123456abc",
				},
				{
					"method":   "PUT",
					"duration": 1.23,
					"log_id":   "123456abc",
				},
			},
		},
	}
	l := Parser{
		name: parser.TypeLogfmt,
	}
	for _, tt := range tests {
		got, err := l.Parse(tt.s)
		if c, ok := err.(*StatsError); ok {
			err = c.ErrorDetail
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
