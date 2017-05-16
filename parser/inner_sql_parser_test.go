package parser

import (
	"database/sql"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

func TestInnerSQLParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "_sql"
	c[KeyInnerMysqlSchema] = "a long, b string, c float, d date"
	c[KeyLabels] = "mm abc"
	timeStr := time.Now().Format(time.RFC3339)
	timeNow, _ := time.Parse(time.RFC3339Nano, timeStr)
	expTime := timeNow.Format(time.RFC3339Nano)
	p, _ := NewInternalSQLParser(c)
	tests := []struct {
		in  []string
		exp []sender.Data
	}{
		{
			in: []string{"1", "a", "1.1", timeStr},
			exp: []sender.Data{sender.Data{
				"a":  int64(1),
				"b":  "a",
				"c":  1.1,
				"d":  expTime,
				"mm": "abc",
			}},
		},
		{
			in: []string{"121311121311111", "abcccccc", "0.1", timeStr},
			exp: []sender.Data{sender.Data{
				"a":  int64(121311121311111),
				"b":  "abcccccc",
				"c":  0.1,
				"d":  expTime,
				"mm": "abc",
			}},
		},
	}
	for _, ti := range tests {
		var inputs []sql.RawBytes
		for _, sc := range ti.in {
			inputs = append(inputs, sql.RawBytes(sc))
		}
		tmp := utils.TuoEncode(inputs)
		m, err := p.Parse([]string{string(tmp)})
		if err != nil {
			errx, _ := err.(*utils.StatsError)
			if errx.ErrorDetail != nil {
				t.Error(errx.ErrorDetail)
			}
		}
		assert.EqualValues(t, ti.exp, m)
	}
	assert.EqualValues(t, "testparser", p.Name())
}

func TestParseInnerMysqlSchema(t *testing.T) {
	tests := []struct {
		in  string
		exp []InnerMysqlSchema
	}{
		{
			in: "a long,b float,c string,d date",
			exp: []InnerMysqlSchema{
				{Name: "a", Type: "long"},
				{Name: "b", Type: "float"},
				{Name: "c", Type: "string"},
				{Name: "d", Type: "date"},
			},
		},
		{
			in: "a long,b float,c ,d date",
			exp: []InnerMysqlSchema{
				{Name: "a", Type: "long"},
				{Name: "b", Type: "float"},
				{Name: "c", Type: "string"},
				{Name: "d", Type: "date"},
			},
		},
		{
			in: ",a ,b ,c ,d,",
			exp: []InnerMysqlSchema{
				{Name: "a", Type: "string"},
				{Name: "b", Type: "string"},
				{Name: "c", Type: "string"},
				{Name: "d", Type: "string"},
			},
		},
	}
	for _, ti := range tests {
		got := parseInnerSQLSchema(ti.in)
		assert.EqualValues(t, ti.exp, got)
	}
}

func TestConvertDataType(t *testing.T) {
	tests := []struct {
		data []byte
		Type string
		exp  interface{}
	}{
		{
			data: []byte("abc"),
			Type: "string",
			exp:  "abc",
		},
		{
			data: []byte("123"),
			Type: "long",
			exp:  int64(123),
		},
		{
			data: []byte("123.3"),
			Type: "float",
			exp:  123.3,
		},
		{
			data: []byte("2017-04-11T16:06:29+08:00"),
			Type: "date",
			exp:  "2017-04-11T16:06:29+08:00",
		},
	}
	for _, ti := range tests {
		got, err := convertDataType(ti.data, ti.Type)
		assert.NoError(t, err)
		assert.EqualValues(t, ti.exp, got)
	}
}
