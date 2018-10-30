package sender

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/utils/models"
)

func Test_HandleStat(t *testing.T) {
	fs := FtSender{
		statsMutex: new(sync.RWMutex),
	}
	err := fs.handleStat(nil, true, 30)
	assert.Nil(t, err)
	assert.Equal(t, "", fs.stats.LastError)
	assert.Equal(t, int64(-30), fs.stats.Errors)
	assert.Equal(t, int64(30), fs.stats.Success)

	err = fs.handleStat(nil, false, 30)
	assert.Nil(t, err)
	assert.Equal(t, "", fs.stats.LastError)
	assert.Equal(t, int64(-30), fs.stats.Errors)
	assert.Equal(t, int64(60), fs.stats.Success)

	err = fs.handleStat(errors.New("i am test"), false, 30)
	assert.Equal(t, "i am test", err.Error())
	assert.Equal(t, "i am test", fs.stats.LastError)
	assert.Equal(t, int64(0), fs.stats.Errors)

	err = fs.handleStat(errors.New("i am test"), false, 30)
	assert.Equal(t, "i am test", err.Error())
	assert.Equal(t, "i am test", fs.stats.LastError)
	assert.Equal(t, int64(30), fs.stats.Errors)

	se := &models.StatsError{
		StatsInfo: models.StatsInfo{
			Success:   100,
			Errors:    20,
			LastError: "i am detail",
		},
	}

	fs.stats.Errors = 0
	fs.stats.Success = 0
	err = fs.handleStat(se, false, 130)
	assert.Equal(t, "success 100 errors 20 last error i am detail, send error detail <nil>", err.Error())
	assert.Equal(t, "i am detail", fs.stats.LastError)
	assert.Equal(t, int64(0), fs.stats.Success)
	assert.Equal(t, int64(130), fs.stats.Errors)

	se = &models.StatsError{
		SendError: reqerr.NewSendError("senderror", nil, "no"),
		StatsInfo: models.StatsInfo{
			Success: 100,
			Errors:  20,
		},
	}

	fs.stats.Errors = 0
	fs.stats.Success = 0
	err = fs.handleStat(se, false, 30)
	assert.Equal(t, "success 100 errors 20 last error , send error detail SendError: senderror, failDatas size : 0", err.Error())
	assert.Equal(t, "SendError: senderror, failDatas size : 0", fs.stats.LastError)
	assert.Equal(t, int64(20), fs.stats.Errors)
}

func Test_isErrorEmpty(t *testing.T) {
	tests := []struct {
		err error
		res bool
	}{
		{
			err: nil,
			res: true,
		},
		{
			err: errors.New("test error"),
			res: false,
		},
		{
			err: &models.StatsError{
				StatsInfo: models.StatsInfo{
					Success: 10,
					Errors:  0,
				},
			},
			res: true,
		},
		{
			err: &models.StatsError{
				StatsInfo: models.StatsInfo{
					Success:   10,
					Errors:    1,
					LastError: "last error is test error",
				},
			},
			res: false,
		},
		{
			err: &models.StatsError{
				StatsInfo: models.StatsInfo{
					Success: 10,
					Errors:  1,
				},
				SendError: reqerr.NewSendError(
					"bulk failed with last error: error test",
					[]map[string]interface{}{
						{"a": "b"},
					},
					reqerr.TypeDefault,
				),
			},
			res: false,
		},
		{
			err: &models.StatsError{
				StatsInfo: models.StatsInfo{
					Success:   10,
					Errors:    1,
					LastError: "last error is error test",
				},
				SendError: reqerr.NewSendError(
					"bulk failed with last error: error test",
					[]map[string]interface{}{
						{"c": "d"},
					},
					reqerr.TypeDefault,
				),
			},
			res: false,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.res, isErrorEmpty(test.err))
	}
}
