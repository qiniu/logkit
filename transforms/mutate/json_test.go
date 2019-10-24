package mutate

import (
	"encoding/json"
	"testing"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestJsonTransformer(t *testing.T) {
	jsonConf := &Json{
		Key: "json",
		New: "json",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res, err := jsonConf.Transform(data)
	assert.NoError(t, err)
	exp := []Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp, res)

	jsonConf2 := &Json{
		Key: "json",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data2 := []Data{{"key1": "value1", "json": `{"json":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res2, err2 := jsonConf2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []Data{{"key1": "value1", "json": map[string]interface{}{"json": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp2, res2)

	jsonConf3 := &Json{
		Key: "json",
		New: "newKey",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data3 := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res3, err3 := jsonConf3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`, "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`, "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp3, res3)

	jsonConf4 := &Json{
		Key: "json....",
		New: "newKey....",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data4 := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res4, err4 := jsonConf4.Transform(data4)
	assert.NoError(t, err4)
	exp4 := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`, "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`, "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp4, res4)

	jsonConf5 := &Json{
		Key:     "json",
		New:     "newKey",
		Extract: true,
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data5 := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res5, err5 := jsonConf5.Transform(data5)
	assert.NoError(t, err5)
	exp5 := []Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`, "name": "小明", "sex": "男"}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`, "name": "小红", "sex": "女"}}
	assert.Equal(t, exp5, res5)

	jsonConf6 := &Json{
		Key:     "json.a",
		New:     "newKey.b",
		Extract: true,
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data6 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}}}
	res6, err6 := jsonConf6.Transform(data6)
	assert.NoError(t, err6)
	exp6 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}, "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}}
	assert.Equal(t, exp6, res6)

	jsonConf71 := &Json{
		Key: "json.a",
		New: "newKey",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data71 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}}}
	res71, err71 := jsonConf71.Transform(data71)
	assert.NoError(t, err71)
	exp71 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}, "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}}
	assert.Equal(t, exp71, res71)

	jsonConf7 := &Json{
		Key:     "json.a",
		New:     "newKey",
		Extract: true,
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data7 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}}}
	res7, err7 := jsonConf7.Transform(data7)
	assert.NoError(t, err7)
	exp7 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}, "name": "小明", "sex": "男"}}
	assert.Equal(t, exp7, res7)

	jsonConf8 := &Json{
		Key:     "json.a",
		Extract: true,
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	data8 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "b": "c"}}}
	res8, err8 := jsonConf8.Transform(data8)
	assert.NoError(t, err8)
	exp8 := []Data{{"key1": "value1", "json": map[string]interface{}{"a": `{"name":"小明", "sex":"男"}`, "name": "小明", "sex": "男", "b": "c"}}}
	assert.Equal(t, exp8, res8)
}

func TestParseJson(t *testing.T) {
	jsonTool := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()

	data := `{"name":"ethancai", "fansCount": 9223372036854775807}`
	res, err := parseJson(jsonTool, data)
	assert.NoError(t, err)
	exp := map[string]interface{}{"name": "ethancai", "fansCount": json.Number("9223372036854775807")}
	assert.Equal(t, exp, res)

	data = `["a","b"]`
	res, err = parseJson(jsonTool, data)
	assert.NoError(t, err)
	exp2 := []interface{}{"a", "b"}
	assert.Equal(t, exp2, res)

	data = `[{"name":"ethancai", "fansCount": 9223372036854775807}]`
	res, err = parseJson(jsonTool, data)
	assert.NoError(t, err)
	exp3 := []interface{}{
		map[string]interface{}{"name": "ethancai", "fansCount": json.Number("9223372036854775807")},
	}
	assert.Equal(t, exp3, res)
}
