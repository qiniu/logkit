package mutate

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestConvertTransformer(t *testing.T) {
	gsub := &Converter{
		DSL: "myword array(long)",
	}
	data, err := gsub.Transform([]Data{{"myword": []string{"123", "456"}, "abc": "x1 y2"}, {"myword": []string{"321", "654"}, "abc": "x1"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": []interface{}{int64(123), int64(456)}, "abc": "x1 y2"},
		{"myword": []interface{}{int64(321), int64(654)}, "abc": "x1"}}
	assert.Equal(t, exp, data)
	gsub2 := &Converter{
		DSL: "myword",
	}
	newd, err := gsub2.Transform([]Data{{"myword": 123, "abc": "x1 y2"}, {"myword": 654, "abc": "x1"}})
	assert.NoError(t, err)
	exp = []Data{
		{"myword": "123", "abc": "x1 y2"},
		{"myword": "654", "abc": "x1"}}
	assert.Equal(t, exp, newd)
	assert.Equal(t, gsub.Stage(), transforms.StageAfterParser)

	gsub3 := &Converter{
		DSL: "multi.myword array(long)",
	}
	data3, err3 := gsub3.Transform([]Data{{"multi": map[string]interface{}{"myword": []string{"123", "456"}, "abc": "x1 y2"}}, {"multi": map[string]interface{}{"myword": []string{"321", "654"}, "abc": "x1"}}})
	assert.NoError(t, err3)
	exp3 := []Data{
		{"multi": map[string]interface{}{"myword": []interface{}{int64(123), int64(456)}, "abc": "x1 y2"}},
		{"multi": map[string]interface{}{"myword": []interface{}{int64(321), int64(654)}, "abc": "x1"}},
	}
	assert.Equal(t, exp3, data3)
}

func TestConvert(t *testing.T) {
	cases := []struct {
		v   Data
		DSL string
		exp Data
	}{
		{
			v:   Data{"abc": ""},
			DSL: "abc long",
			exp: Data{},
		},
		{
			v:   Data{"abc": "123"},
			DSL: "abc long",
			exp: Data{"abc": int64(123)},
		},
		{
			v:   Data{"abc": "123", "nihao": "true"},
			DSL: "abc long,nihao bool",
			exp: Data{"abc": int64(123), "nihao": true},
		},
		{
			v:   Data{"abc": "123", "nihao": "true", "haha": map[string]interface{}{"abc": 123}},
			DSL: "abc long,nihao bool,haha{abc string}",
			exp: Data{"abc": int64(123), "nihao": true, "haha": map[string]interface{}{"abc": "123"}},
		},
		{
			v:   Data{"abc": "2018-06-15 15:15:21"},
			DSL: "abc date",
			exp: Data{"abc": "2018-06-15T15:15:21Z"},
		},
	}
	for _, ca := range cases {
		gsub := &Converter{
			DSL: ca.DSL,
		}
		gots, err := gsub.Transform([]Data{ca.v})
		assert.NoError(t, err)
		assert.Equal(t, ca.exp, gots[0])
	}
}

func TestConvertType(t *testing.T) {
	gsub := &Converter{
		DSL: "myword string",
	}
	data, err := gsub.Transform([]Data{{"myword": json.Number("123")}, {"myword": 456}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "123"},
		{"myword": "456"}}
	assert.Equal(t, exp, data)
	dt := data[0]["myword"]
	assert.Equal(t, "string", reflect.TypeOf(dt).Name())
	sdt, ok := dt.(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "123", sdt)

}

func TestConvertData(t *testing.T) {
	type helloint int
	tests := []struct {
		v      interface{}
		schema DslSchemaEntry
		exp    interface{}
	}{
		{
			v: helloint(1),
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeLong,
			},
			exp: helloint(1),
		},
		{
			v: helloint(1),
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeString,
			},
			exp: "1",
		},
		{
			v: json.Number("1"),
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeLong,
			},
			exp: int64(1),
		},
		{
			v: "1",
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeLong,
			},
			exp: int64(1),
		},
		{
			v: []int{1, 2, 3},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeArray,
				ElemType:  pipeline.PandoraTypeLong,
			},
			exp: []interface{}{1, 2, 3},
		},
		{
			v: []int{1, 2, 3},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeArray,
				ElemType:  pipeline.PandoraTypeString,
			},
			exp: []interface{}{"1", "2", "3"},
		},
		{
			v: []interface{}{1, 2, 3},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeArray,
				ElemType:  pipeline.PandoraTypeString,
			},
			exp: []interface{}{"1", "2", "3"},
		},
		{
			v: `[1, 2, 3]`,
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeArray,
				ElemType:  pipeline.PandoraTypeString,
			},
			exp: []interface{}{"1", "2", "3"},
		},
		{
			v: `["1", "2", "3"]`,
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeArray,
				ElemType:  pipeline.PandoraTypeFloat,
			},
			exp: []interface{}{float64(1), float64(2), float64(3)},
		},
		{
			v: "1.1",
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeFloat,
			},
			exp: float64(1.1),
		},
		{
			v: map[string]interface{}{
				"a": 123,
			},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeMap,
				Schema: []DslSchemaEntry{
					{ValueType: pipeline.PandoraTypeString, Key: "a"},
				},
			},
			exp: map[string]interface{}{
				"a": "123",
			},
		},
		{
			v: map[string]interface{}{
				"a": 123,
			},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeMap,
				Schema: []DslSchemaEntry{
					{ValueType: pipeline.PandoraTypeFloat, Key: "a"},
				},
			},
			exp: map[string]interface{}{
				"a": 123,
			},
		},
		{
			v: map[string]interface{}{
				"a": "123",
				"b": "hello",
			},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeMap,
				Schema: []DslSchemaEntry{
					{ValueType: pipeline.PandoraTypeFloat, Key: "a"},
					{ValueType: pipeline.PandoraTypeString, Key: "b"},
				},
			},
			exp: map[string]interface{}{
				"a": float64(123),
				"b": "hello",
			},
		},
		{
			v: `{
				"a": "123",
				"b": "hello"
			}`,
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeMap,
				Schema: []DslSchemaEntry{
					{ValueType: pipeline.PandoraTypeFloat, Key: "a"},
					{ValueType: pipeline.PandoraTypeString, Key: "b"},
				},
			},
			exp: map[string]interface{}{
				"a": float64(123),
				"b": "hello",
			},
		},
		{
			v: `{
				"a": "123.23",
				"b": "hello"
			}`,
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeMap,
				Schema: []DslSchemaEntry{
					{ValueType: pipeline.PandoraTypeLong, Key: "a"},
					{ValueType: pipeline.PandoraTypeString, Key: "b"},
				},
			},
			exp: map[string]interface{}{
				"a": int64(123),
				"b": "hello",
			},
		},
		{
			v: `{
				"a": "123.23",
				"b": {
					"c":123
				}
			}`,
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeMap,
				Schema: []DslSchemaEntry{
					{ValueType: pipeline.PandoraTypeLong, Key: "a"},
					{ValueType: pipeline.PandoraTypeMap, Key: "b", Schema: []DslSchemaEntry{
						{ValueType: pipeline.PandoraTypeLong, Key: "c"}},
					},
				},
			},
			exp: map[string]interface{}{
				"a": int64(123),
				"b": map[string]interface{}{
					"c": int64(123),
				},
			},
		},
		{
			v: []interface{}{1, "2", 3.2, true, "false"},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeString,
			},
			exp: `[1,"2",3.2,true,"false"]`,
		},
		{
			v: [5]interface{}{1, "2", 3.2, true, "false"},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeString,
			},
			exp: `[1,"2",3.2,true,"false"]`,
		},
		{
			v: []int{1, 2, 3, 4, 5},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeString,
			},
			exp: `[1,2,3,4,5]`,
		},
		{
			v: [6]float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6},
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeString,
			},
			exp: `[1.1,2.2,3.3,4.4,5.5,6.6]`,
		},
		{
			v: nil,
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeString,
				Default:   "",
			},
			exp: "",
		},
		{
			v: "NIHAO",
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeFloat,
				Default:   12.1,
			},
			exp: 12.1,
		},
		{
			v: "",
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeFloat,
			},
			exp: nil,
		},
		{
			v: "",
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeFloat,
				Default:   float64(1.0),
			},
			exp: float64(1.0),
		},
		{
			v: "",
			schema: DslSchemaEntry{
				ValueType: pipeline.PandoraTypeLong,
				Default:   int64(2),
			},
			exp: int64(2),
		},
	}
	for _, ti := range tests {
		got, err := dataConvert(ti.v, ti.schema)
		assert.NoError(t, err)
		assert.Equal(t, ti.exp, got)
	}
}

func TestDefaultConvert(t *testing.T) {
	tests := []struct {
		str string
		tp  string
		exp interface{}
	}{
		{
			str: "hello",
			tp:  "string",
			exp: "hello",
		},
		{
			str: "123",
			tp:  "long",
			exp: int64(123),
		},
		{
			str: "1.1",
			tp:  "long",
			exp: int64(1),
		},
		{
			str: "1",
			tp:  "float",
			exp: float64(1.0),
		},
		{
			str: "false",
			tp:  "boolean",
			exp: false,
		},
	}
	for _, ti := range tests {
		got, err := defaultConvert(ti.str, ti.tp)
		assert.NoError(t, err)
		assert.Equal(t, ti.exp, got)
	}
}

func TestParseDSL(t *testing.T) {
	tests := []struct {
		dsl string
		exp []DslSchemaEntry
	}{
		{
			dsl: "hello l",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "long"}},
		},
		{
			dsl: "hello f",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "float"}},
		},
		{
			dsl: "hello {nihao f}",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "map", Schema: []DslSchemaEntry{{Key: "nihao", ValueType: "float"}}}},
		},
		{
			dsl: "hello {nihao f 0.1}",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "map", Schema: []DslSchemaEntry{{Key: "nihao", ValueType: "float", Default: float64(0.1)}}}},
		},
		{
			dsl: "hello map{nihao string abc}",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "map", Schema: []DslSchemaEntry{{Key: "nihao", ValueType: "string", Default: "abc"}}}},
		},
		{
			dsl: "hello { nihao f}, nihao ( float)",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "map", Schema: []DslSchemaEntry{{Key: "nihao", ValueType: "float"}}}, {Key: "nihao", ValueType: "array", ElemType: "float"}},
		},
		{
			dsl: "hello f 0.1",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "float", Default: float64(0.1)}},
		},
		{
			dsl: "hello long 1",
			exp: []DslSchemaEntry{{Key: "hello", ValueType: "long", Default: int64(1)}},
		},
	}
	for _, v := range tests {
		gots, err := ParseDsl(v.dsl, 0)
		assert.NoError(t, err)
		assert.Equal(t, v.exp, gots)
	}
}

func TestGetField(t *testing.T) {
	tests := []struct {
		str    string
		expkey string
		expVtp string
		etp    string
		dft    interface{}
	}{
		{
			str:    "hello f 0.1",
			expkey: "hello",
			expVtp: "float",
			etp:    "",
			dft:    float64(0.1),
		},
		{
			str:    "hello long 0",
			expkey: "hello",
			expVtp: "long",
			etp:    "",
			dft:    int64(0),
		},
		{
			str:    "hello string 0",
			expkey: "hello",
			expVtp: "string",
			etp:    "",
			dft:    "0",
		},
		{
			str:    "hello long",
			expkey: "hello",
			expVtp: "long",
			etp:    "",
		},
		{
			str:    "hello",
			expkey: "hello",
			expVtp: "",
			etp:    "",
		},
	}
	for _, v := range tests {
		key, vtp, etp, dft, err := getField(v.str)
		assert.NoError(t, err)
		assert.Equal(t, v.expkey, key)
		assert.Equal(t, v.expVtp, vtp)
		assert.Equal(t, v.etp, etp)
		assert.Equal(t, v.dft, dft)
	}
}
