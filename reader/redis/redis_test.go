package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

func TestNewRedisReader(t *testing.T) {
	myconf := conf.MapConf{
		reader.KeyRedisDataType: "0",
		reader.KeyRedisKey:      "mykey",
	}

	rrr, err := NewReader(nil, myconf)
	rr := rrr.(*Reader)
	assert.NoError(t, err)
	assert.Equal(t, StatsInfo{}, rr.Status())
}
