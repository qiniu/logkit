package reader

import (
	"testing"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestNewRedisReader(t *testing.T) {
	myconf := conf.MapConf{
		KeyRedisDataType: "0",
		KeyRedisKey:      "mykey",
	}

	rrr, err := NewRedisReader(nil, myconf)
	rr := rrr.(*RedisReader)
	assert.NoError(t, err)
	assert.Equal(t, StatsInfo{}, rr.Status())
}
