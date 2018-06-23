package queue

import (
	. "github.com/qiniu/logkit/utils/models"
)

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Name() string
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
	SyncMeta()
}

// DataQueue 代表了无需编解码可直接放取 Data 的队列
type DataQueue interface {
	// PutDatas 用于存放一组数据
	PutDatas([]Data) error
	// ReadDatasChan 用于获取直接读取 Data 的管道
	ReadDatasChan() <-chan []Data
}

const (
	FROM_NONE = iota
	FROM_DISK
	FROM_MEMORY
)
