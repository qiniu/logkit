package samples

import (
	"testing"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestMysender(t *testing.T) {
	c := conf.MapConf{
		"name":   "ohmysender",
		"prefix": "test",
	}
	s, err := NewMySender(c)
	datas := []Data{
		{
			"abc": 1,
			"cde": "testmessage",
		},
		{
			"abc": 2,
			"cde": "testmessage2",
		},
	}
	assert.Nil(t, err)
	assert.Equal(t, s.Name(), "ohmysender")
	assert.Nil(t, s.Send(datas))
	assert.Nil(t, s.Close())
}
