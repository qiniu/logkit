package common

import (
	"errors"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

// NotAsyncSender return when sender is not async
var ErrNotAsyncSender = errors.New("This Sender does not support for Async Push")

// Sender send data to pandora, prometheus such different destinations
type Sender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
}

type StatsSender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
	Stats() StatsInfo
	// 恢复 sender 停止之前的状态
	Restore(*StatsInfo)
}

type TokenRefreshable interface {
	TokenRefresh(conf.MapConf) error
}

// Ft sender默认同步一次meta信息的数据次数
const DefaultFtSyncEvery = 10
