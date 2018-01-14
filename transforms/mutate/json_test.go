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
	data := []sender.Data{{"key1": "value1", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data, err := gusb.Transform(data)
	assert.NoError(t, err)
	exp := []sender.Data{{"key1": "value1", "json": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "json": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp, data)

	gusb2 := &Json{
		Key: "json",
	}
	data2 := []sender.Data{{"key1": "value1", "json": "{\"json\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data2, err2 := gusb2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []sender.Data{{"key1": "value1", "json": "小明", "sex": "男"}, {"key2": "value2", "name": "小红", "sex": "女"}}
	assert.Equal(t, exp2, data2)

	gusb3 := &Json{
		Key: "json",
		New: "newKey",
	}
	data3 := []sender.Data{{"key1": "value1", "json": "{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2": "value2", "json": "{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data3, err3 := gusb3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []sender.Data{{"key1": "value1", "newKey": map[string]interface{}{"name": "小明", "sex": "男"}}, {"key2": "value2", "newKey": map[string]interface{}{"name": "小红", "sex": "女"}}}
	assert.Equal(t, exp3, data3)

}
