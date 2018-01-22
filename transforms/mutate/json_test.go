package mutate

import (
	"github.com/qiniu/logkit/sender"
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/json-iterator/go"
	"encoding/json"
)

/*func TestJsonTransformer(t *testing.T) {
	gusb := &Json{
		OldKey: "json",
		NewKey: "json",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	gusb.Init()
	data := []sender.Data{{"key1": "value1", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data, err := gusb.Transform(data)
	assert.NoError(t, err)
	exp := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp, data)

	gusb2 := &Json{
		OldKey: "json",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	gusb2.Init()
	data2 := []sender.Data{{"key1": "value1", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data2, err2 := gusb2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []sender.Data{{"key1": "value1", "name": "小明", "sex": "男", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "name": "小红", "sex": "女", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	assert.Equal(t, exp2, data2)

	gusb3 := &Json{
		OldKey: "json",
		NewKey: "newKey",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	gusb3.Init()
	data3 := []sender.Data{{"key1": "value1", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data3, err3 := gusb3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []sender.Data{{"key1": "value1", "json": "{\"name\":\"小明\", \"sex\":\"男\"}", "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}", "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp3, data3)

}*/

func TestJsonTransformer(t *testing.T) {
	jsonConf := &Json{
		OldKey: "json",
		NewKey: "json",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	jsonConf.Init()
	data := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res, err := jsonConf.Transform(data)
	assert.NoError(t, err)
	exp := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp, res)

	jsonConf2 := &Json{
		OldKey: "json",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	jsonConf2.Init()
	data2 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res2, err2 := jsonConf2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []sender.Data{{"key1": "value1", "name": "小明", "sex": "男", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "name": "小红", "sex": "女", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	assert.Equal(t, exp2, res2)

	jsonConf3 := &Json{
		OldKey: "json",
		NewKey: "newKey",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	jsonConf3.Init()
	data3 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res3, err3 := jsonConf3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`, "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`, "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp3, res3)

	jsonConf4 := &Json{
		OldKey: "json....",
		NewKey: "newKey",
		jsonTool: jsoniter.Config{
			EscapeHTML: true,
			UseNumber:  true,
		}.Froze(),
	}
	jsonConf4.Init()
	data4 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	res4, err4 := jsonConf4.Transform(data4)
	assert.NoError(t, err4)
	exp4 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`, "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`, "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
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
