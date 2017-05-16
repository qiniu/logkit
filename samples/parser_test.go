package samples

import (
	"testing"

	"github.com/qiniu/logkit/conf"

	"github.com/stretchr/testify/assert"
)

func TestMyParser(t *testing.T) {
	c := conf.MapConf{}
	c["name"] = "my_parser_test"
	c["type"] = "myparser"
	c["max_len"] = "10"
	parser, err := NewMyParser(c)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, parser.Name(), "my_parser_test")

	lines := []string{}
	lines = append(lines, "1234567890123456789\n")
	lines = append(lines, "12345\n")
	datas, err := parser.Parse(lines)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, len(datas), 2)
	d0 := datas[0]
	l, exist := d0["log"]
	assert.True(t, exist)
	assert.Equal(t, l, "1234567890")
	d1 := datas[1]
	l, exist = d1["log"]
	assert.True(t, exist)
	assert.Equal(t, l, "12345")
}
