package mutate

import (
	"encoding/json"
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
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
