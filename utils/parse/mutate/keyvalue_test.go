package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/utils/models"
)

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
					"foo": "",
					"bar": "abc",
				},
			},
			existErr: false,
			splitter: "=",
		},
		{
			line: `foo= `,
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

func Test_splitKV(t *testing.T) {

	tests := []struct {
		line       string
		expectData []string
		splitter   string
	}{
		{
			line: "foo=",
			expectData: []string{
				"foo",
				"",
			},
			splitter: "=",
		},
		{
			line: "=def",
			expectData: []string{
				"",
				"def",
			},
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
			splitter: "=",
		},
		{
			line: "foo\n=def\nabc=a\nbc",
			expectData: []string{
				"foo",
				"def",
				"abc",
				"a\nbc",
			},
			splitter: "=",
		},
		{
			line: "foo=def \n abc =abc",
			expectData: []string{
				"foo",
				"def",
				"abc",
				"abc",
			},
			splitter: "=",
		},
		{
			line: "foo=def\n abc=abc",
			expectData: []string{
				"foo",
				"def",
				"abc",
				"abc",
			},
			splitter: "=",
		},
		{
			line: "foo=def\n test abc=abc",
			expectData: []string{
				"foo",
				"def\n test",
				"abc",
				"abc",
			},
			splitter: "=",
		},
		{
			line: "time=2018-01-02T03:04:05.123Z \nCST abc=abc",
			expectData: []string{
				"time",
				"2018-01-02T03:04:05.123Z \nCST",
				"abc",
				"abc",
			},
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
			splitter: ":",
		},
		{
			line: "f:o:o::def",
			expectData: []string{
				"f:o:o",
				"def",
			},
			splitter: "::",
		},
		{
			line:       "f:o:o:def",
			expectData: []string{},
			splitter:   "::",
		},
		{
			line:       "f:o:o:\n:def",
			expectData: nil,
			splitter:   "::",
		},
		{
			line: "f:\no::a o:\n:def",
			expectData: []string{
				"f:\no",
				"a o:\n:def",
			},
			splitter: "::",
		},
		{
			line:       "f:o:o: \n :def",
			expectData: []string{},
			splitter:   "::",
		},
	}
	for _, tt := range tests {
		got := splitKV(tt.line, tt.splitter)
		assert.Equal(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}

	}
}
