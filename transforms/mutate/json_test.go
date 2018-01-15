package mutate

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/stretchr/testify/assert"
)

func TestJsonTransformer(t *testing.T) {
	gusb := &Json{
		Key: "json",
		New: "json",
	}
	data := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	data, err := gusb.Transform(data)
	assert.NoError(t, err)
	exp := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp, data)

	gusb2 := &Json{
		Key: "json",
	}
	data2 := []sender.Data{{"key1": "value1", "json": `{"json":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	data2, err2 := gusb2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"json": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp2, data2)

	gusb3 := &Json{
		Key: "json",
		New: "newKey",
	}
	data3 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	data3, err3 := gusb3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []sender.Data{{"key1": "value1", "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp3, data3)

	gusb4 := &Json{
		Key: "json",
		New: "newKey",
		ReserveTag: true,
	}
	data4 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	data4, err4 := gusb4.Transform(data4)
	assert.NoError(t, err4)
	exp4 := []sender.Data{{"key1": "value1", "newKey": map[string]interface{}{"name": "小明", "sex": "男"}, "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "newKey": map[string]interface{}{"name": "小红", "sex": "女"}, "json": `{"name":"小红", "sex":"女"}`}}
	assert.Equal(t, exp4, data4)

	gusb5 := &Json{
		Key: "json",
		ReserveTag: true,
	}
	data5 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	data5, err5 := gusb5.Transform(data5)
	assert.NoError(t, err5)
	exp5 := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp5, data5)

	gusb6 := &Json{
		Key: "json",
		New: "json",
		ReserveTag: true,
	}
	data6 := []sender.Data{{"key1": "value1", "json": `{"name":"小明", "sex":"男"}`}, {"key2": "value2", "json": `{"name":"小红", "sex":"女"}`}}
	data6, err6 := gusb6.Transform(data6)
	assert.NoError(t, err6)
	exp6 := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp6, data6)
}
