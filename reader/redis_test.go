package reader

import (
	"testing"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisReader(t *testing.T) {
	myconf := conf.MapConf{
		KeyRedisDataType: "0",
		KeyRedisKey:      "mykey",
	}

	rr, err := NewRedisReader(nil, myconf)
	assert.NoError(t, err)
	assert.Equal(t, utils.StatsInfo{}, rr.Status())
}
