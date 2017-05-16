package parser

import (
	"encoding/json"
	"testing"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

func TestJsonParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testjsonparser"
	c[KeyParserType] = "json"
	c[KeyLabels] = "mm abc"
	p, _ := NewJsonParser(c)
	tests := []struct {
		in  []string
		exp []sender.Data
	}{
		{
			in: []string{`{"a":1,"b":[1.0,2.0,3.0],"c":{"d":"123","g":1.2},"e":"x","f":1.23}`},
			exp: []sender.Data{sender.Data{
				"a": json.Number("1"),
				"b": []interface{}{json.Number("1.0"), json.Number("2.0"), json.Number("3.0")},
				"c": map[string]interface{}{
					"d": "123",
					"g": json.Number("1.2"),
				},
				"e":  "x",
				"f":  json.Number("1.23"),
				"mm": "abc",
			}},
		},
		{
			in: []string{`{"a":1,"b":[1.0,2.0,3.0],"c":{"d":"123","g":1.2},"e":"x","mm":1.23,"jjj":1493797500346428926}`},
			exp: []sender.Data{sender.Data{
				"a": json.Number("1"),
				"b": []interface{}{json.Number("1.0"), json.Number("2.0"), json.Number("3.0")},
				"c": map[string]interface{}{
					"d": "123",
					"g": json.Number("1.2"),
				},
				"e":   "x",
				"mm":  json.Number("1.23"),
				"jjj": json.Number("1493797500346428926"),
			}},
		},
	}
	for _, ti := range tests {
		m, err := p.Parse(ti.in)
		if err != nil {
			errx, _ := err.(*utils.StatsError)
			if errx.ErrorDetail != nil {
				t.Error(errx.ErrorDetail)
			}
		}
		assert.EqualValues(t, ti.exp, m)
	}
	assert.EqualValues(t, "testjsonparser", p.Name())
}
