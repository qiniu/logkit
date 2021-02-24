package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/utils/models"
)

// old: 500000	      2695 ns/op | 2550 ns/op | 2431 ns/op
// new: 500000	      2789 ns/op | 3330 ns/op | 3010 ns/op
func Benchmark_Parse(b *testing.B) {
	line := "ts=2018-01-02T03:04:05.123Z lvl=info msg=\"http request\" method=PUT\nduration=1.23 log_id=123456abc"
	sep := "="
	l := Parser{
		KeepString: false,
		Splitter:   sep,
	}
	for n := 0; n < b.N; n++ {
		l.Parse(line)
	}
}

func Test_parseLine(t *testing.T) {
	tests := []struct {
		line       string
		keepString bool
		expectData []models.Data
		existErr   bool
		splitter   string
	}{
		{
			expectData: []models.Data{},
			existErr:   true,
			splitter:   "=",
		},
		{
			line: "foo=\"bar\"",
			expectData: []models.Data{
				{"foo": "bar"},
			},
			splitter: "=",
		},
		{
			line: "foo=\"\"",
			expectData: []models.Data{
				{"foo": ""},
			},
			splitter: "=",
		},
		{
			line: "foo=\"bar\"\n",
			expectData: []models.Data{
				{"foo": "bar"},
			},
			splitter: "=",
		},
		{
			line: `ts=2018-01-02T03:04:05.123Z lvl=info  method=PUT msg="http request" a=12345678901234567890123456789`,
			expectData: []models.Data{
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
			line: `ts=2018-01-02T03:04:05.123Z lvl=  method="" msg="http request" a=12345678901234567890123456789`,
			expectData: []models.Data{
				{
					"lvl":    "",
					"msg":    "http request",
					"method": "",
					"ts":     "2018-01-02T03:04:05.123Z",
					"a":      1.2345678901234568e+28,
				},
			},
			splitter: "=",
		},
		{
			line:       `ts=2018-01-02T03:04:05.123Z lvl=info  method=PUT msg="http request" a=12345678901234567890123456789`,
			keepString: true,
			expectData: []models.Data{
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
			expectData: []models.Data{},
			existErr:   true,
			splitter:   "=",
		},
		{
			line: `foo="" bar=`,
			expectData: []models.Data{
				{
					"foo": "",
					"bar": "",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo= = = =`,
			expectData: []models.Data{
				{
					"foo": "= = =",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo= = = = a = b`,
			expectData: []models.Data{
				{
					"foo": "= = =",
					"a":   "b",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo==`,
			expectData: []models.Data{
				{
					"foo": "=",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo== =a`,
			expectData: []models.Data{
				{
					"foo": "= =a",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo== = b =`,
			expectData: []models.Data{
				{
					"foo": "= =",
					"b":   "",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `abc=abc foo="def`,
			expectData: []models.Data{
				{
					"abc": "abc",
					"foo": "\"def",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `"foo=" bar=abc`,
			expectData: []models.Data{
				{
					"\"foo=\" bar": "abc",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo=`,
			expectData: []models.Data{
				{
					"foo": "",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `"= `,
			expectData: []models.Data{
				{
					"\"": "",
				},
			},
			existErr: false,
			splitter: "=",
		},
	}
	l := Parser{
		KeepString: false,
		Splitter:   "",
	}
	for _, tt := range tests {
		l.KeepString = tt.keepString
		l.Splitter = tt.splitter
		got, err := l.Parse(tt.line)
		assert.Equal(t, tt.existErr, err != nil)
		assert.Equal(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}
}

func Test_getSepPos(t *testing.T) {
	tests := []struct {
		line     string
		pos      int
		splitter string
	}{
		{
			line:     "foo=",
			pos:      3,
			splitter: "=",
		},
		{
			line:     "=def",
			pos:      0,
			splitter: "=",
		},
		{
			line:     "foo=def abc = abc ",
			pos:      3,
			splitter: "=",
		},
		{
			line:     "foo\n=def\nabc=a\nbc",
			pos:      4,
			splitter: "=",
		},
		{
			line:     " foo =def \n abc =abc",
			pos:      5,
			splitter: "=",
		},
		{
			line:     "\"foo\" =def\n abc=abc",
			pos:      6,
			splitter: "=",
		},
		{
			line:     "\"a=b\"a\"foo=def",
			pos:      10,
			splitter: "=",
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.pos, getSepPos(tt.line, tt.splitter))
	}
}

func Test_getSpacePos(t *testing.T) {
	tests := []struct {
		line      string
		pos       int
		direction int
	}{
		{
			line:      "foo=",
			pos:       -1,
			direction: 1,
		},
		{
			line:      "=def",
			pos:       -1,
			direction: 1,
		},
		{
			line:      "foo=def abc = abc ",
			pos:       7,
			direction: 1,
		},
		{
			line:      "foo\n=def\nabc=a\nbc",
			pos:       3,
			direction: 1,
		},
		{
			line:      " foo =def \n abc =abc",
			pos:       0,
			direction: 1,
		},
		{
			line:      "\"name = a b\" foo=def",
			pos:       12,
			direction: 1,
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.pos, getSpacePos(tt.line, tt.direction))
	}
}
