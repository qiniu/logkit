package mutate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestKV_Transform(t *testing.T) {
	tests := []struct {
		line       []Data
		expectData []Data
		expectErr  error
		splitter   string
		keepString bool
		new        string
	}{
		{
			expectData: []Data(nil),
			splitter:   "=",
			new:        "raw",
		},
		{
			line: []Data{{"raw": "foo=\"bar\""}},
			expectData: []Data{
				{"raw": Data{"foo": "bar"}},
			},
			splitter: "=",
			new:      "raw",
		},
		{
			line:       []Data{{"raw": "foo=\"bar\"\n"}},
			expectData: []Data{{"raw": Data{"foo": "bar"}}},
			splitter:   "=",
			new:        "raw",
		},
		{
			line: []Data{{"raw": "ts=2018-01-02T03:04:05.123Z  lvl=info msg=\"http request\" 	method=PUT\nduration=1.23 log_id=123456abc"}},
			expectData: []Data{
				{
					"raw": Data{
						"lvl":      "info",
						"msg":      "http request",
						"method":   "PUT",
						"ts":       "2018-01-02T03:04:05.123Z",
						"duration": 1.23,
						"log_id":   "123456abc",
					},
				},
			},
			splitter: "=",
			new:      "raw",
		},
		{
			line: []Data{{"raw": `ts=2018-01-02T03:04:05.123Z lvl=info  method=PUT msg="http request"`}},
			expectData: []Data{
				{
					"raw": Data{
						"lvl":    "info",
						"msg":    "http request",
						"method": "PUT",
						"ts":     "2018-01-02T03:04:05.123Z",
					},
				},
			},
			splitter: "=",
			new:      "raw",
		},
		{
			line:       []Data{{"raw": `123456789012345`}},
			expectData: []Data{{"raw": `123456789012345`}},
			expectErr:  errors.New("find total 1 erorrs in transform keyvalue, last error info is parse transform key value failed, error msg: no splitter exist, will keep origin data in pandora_stash if disable_record_errdata field is false"),
			splitter:   "=",
			keepString: true,
			new:        "raw",
		},
		{
			line:       []Data{{"raw": `a:12345678901234567890123456789`}},
			expectData: []Data{{"raw": Data{"a": 1.2345678901234568e+28}}},
			splitter:   ":",
			new:        "raw",
		},
		{
			line:       []Data{{"raw": `a:12345678901234567890123456789`}},
			expectData: []Data{{"raw": Data{"a": "12345678901234567890123456789"}}},
			splitter:   ":",
			keepString: true,
			new:        "raw",
		},
		{
			line:       []Data{{"raw": `foo="" bar=`}},
			expectData: []Data{{"raw": `foo="" bar=`}},
			expectErr:  errors.New("find total 1 erorrs in transform keyvalue, last error info is parse transform key value failed, error msg: key value not match, will keep origin data in pandora_stash if disable_record_errdata field is false"),
			splitter:   "=",
			new:        "raw",
		},
		{
			line:       []Data{{"raw": `abc=abc foo="def`}},
			expectData: []Data{{"raw": Data{"abc": "abc", "foo": "\"def"}}},
			expectErr:  nil,
			splitter:   "=",
			new:        "raw",
		},
		{
			line:       []Data{{"raw": `"foo=" bar=abc`}},
			expectData: []Data{{"raw": `"foo=" bar=abc`}},
			expectErr:  errors.New(`find total 1 erorrs in transform keyvalue, last error info is parse transform key value failed, error msg: no value or key was parsed after logfmt, will keep origin data in pandora_stash if disable_record_errdata field is false`),
			splitter:   "=",
			new:        "raw",
		},
		{
			line: []Data{{"raw": "ts:2018-01-02T03:04:05.123Z lvl:info msg:\"http request\" method:PUT\nduration:1.23 log_id:123456abc"}},
			expectData: []Data{
				{
					"raw":      "ts:2018-01-02T03:04:05.123Z lvl:info msg:\"http request\" method:PUT\nduration:1.23 log_id:123456abc",
					"lvl":      "info",
					"msg":      "http request",
					"method":   "PUT",
					"ts":       "2018-01-02T03:04:05.123Z",
					"duration": 1.23,
					"log_id":   "123456abc",
				},
			},
			splitter: ":",
		},
	}

	k := &KV{
		Key: "raw",
	}

	for _, test := range tests {
		k.New = test.new
		k.KeepString = test.keepString
		k.Init()
		k.Splitter = test.splitter
		actual, err := k.Transform(test.line)
		assert.EqualValues(t, test.expectErr, err)
		assert.EqualValues(t, test.expectData, actual)
	}

	k = &KV{
		Key:        "raw",
		DiscardKey: true,
		Splitter:   "=",
	}
	assert.Nil(t, k.Init())
	line := []Data{{"raw": "foo=\"bar\"\n"}}
	res, err := k.Transform(line)
	assert.Nil(t, err)
	expect := []Data{{"foo": "bar"}}
	assert.EqualValues(t, expect, res)

	k = &KV{
		Key:        "test.Old",
		New:        "test.New",
		DiscardKey: true,
		Splitter:   "=",
	}
	assert.Nil(t, k.Init())
	line = []Data{{"test": Data{"Old": "foo=\"bar\"\n"}}}
	res, err = k.Transform(line)
	assert.Nil(t, err)
	expect = []Data{{"test": Data{"New": Data{"foo": "bar"}}}}
	assert.EqualValues(t, expect, res)
}

func Test_kvTransform(t *testing.T) {
	type args struct {
		strVal     string
		splitter   string
		keepString bool
	}
	tests := []struct {
		name    string
		args    args
		want    Data
		wantErr bool
	}{
		{
			name: "empty_test",
			args: args{
				strVal:     "",
				splitter:   "=",
				keepString: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "normal_test",
			args: args{
				strVal:     "foo=\"bar\"",
				splitter:   "=",
				keepString: true,
			},
			want:    Data{"foo": "bar"},
			wantErr: false,
		},
		{
			name: "normal_test_2",
			args: args{
				strVal: "ts=2018-01-02T03:04:05.123Z  lvl=info msg=\"http request\" 	method=PUT\nduration=1.23 log_id=123456abc",
				splitter:   "=",
				keepString: false,
			},
			want: Data{
				"ts":       "2018-01-02T03:04:05.123Z",
				"lvl":      "info",
				"msg":      "http request",
				"method":   "PUT",
				"duration": 1.23,
				"log_id":   "123456abc",
			},
			wantErr: false,
		},
		{
			name: "empty_test",
			args: args{
				strVal:     "123456789012345",
				splitter:   "=",
				keepString: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "err_test_1",
			args: args{
				strVal:     "a:12345678901234567890123456789",
				splitter:   "=",
				keepString: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "err_test_2",
			args: args{
				strVal:     `foo="" bar="`,
				splitter:   "=",
				keepString: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "err_test_3",
			args: args{
				strVal:     `abc=abc foo="def`,
				splitter:   "=",
				keepString: true,
			},
			want: Data{
				"abc": "abc",
				"foo": "\"def",
			},
			wantErr: false,
		},
		{
			name: "err_test_4",
			args: args{
				strVal:     `"foo=" bar=abc`,
				splitter:   "=",
				keepString: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "err_test_5",
			args: args{
				strVal:     "ts:2018-01-02T03:04:05.123Z lvl:info msg:\"http request\" method:PUT\nduration:1.23 log_id:123456abc",
				splitter:   "=",
				keepString: true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "err_test_6",
			args: args{
				strVal:     `foo="bar"`,
				splitter:   "=",
				keepString: true,
			},
			want: Data{
				"foo": "bar",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := kvTransform(tt.args.strVal, tt.args.splitter, tt.args.keepString)
			if (err != nil) != tt.wantErr {
				t.Errorf("kvTransform() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("kvTransform() got = %v, want %v", got, tt.want)
			}
		})
	}
}
