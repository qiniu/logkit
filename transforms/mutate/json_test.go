package mutate

import (
	"testing"
	"github.com/qiniu/logkit/sender"
	"github.com/stretchr/testify/assert"
)

func TestJsonTransformer(t *testing.T) {
	gusb := &Json{
		Key: "json",
	}
	data := []sender.Data{{"key1":"value1", "json":"{\"name\":\"小明\", \"sex\":\"男\"}"}, {"key2":"value2", "json":"{\"name\":\"小红\", \"sex\":\"女\"}"}}
	data, err := gusb.Transform(data)
	assert.NoError(t, err)
	exp := []sender.Data{{"key1":"value1", "json":sender.Data{"name":"小明", "sex":"男"}},{"key2":"value2", "json":sender.Data{"name":"小红", "sex":"女"}}}
	assert.Equal(t, exp, data)
}