package queue

import (
	"errors"
	"sync"

	. "github.com/qiniu/logkit/utils/models"
)

const (
	StatusInit int32 = iota
	StatusClosed
)

var _ DataQueue = &directQueue{}

type directQueue struct {
	name    string
	channel chan []Data
	mux     sync.Mutex
	status  int32
	quit    chan bool
}

func NewDirectQueue(name string) BackendQueue {
	return &directQueue{
		name:    name,
		channel: make(chan []Data),
		mux:     sync.Mutex{},
		status:  StatusInit,
		quit:    make(chan bool),
	}
}

func (dq *directQueue) Name() string {
	return dq.name
}

func (dq *directQueue) Put(msg []byte) error {
	return errors.New("method Put is not supported, please use PutData")
}

func (dq *directQueue) ReadChan() <-chan []byte {
	return make(chan []byte) // Blocks forever because no inputs
}

func (dq *directQueue) PutDatas(datas []Data) error {
	dq.mux.Lock()
	defer dq.mux.Unlock()
	if dq.status == StatusClosed {
		return ErrQueueClosed
	}

	select {
	case dq.channel <- datas:
		return nil
	case <-dq.quit:
		return ErrQueueClosed
	}
}

func (dq *directQueue) ReadDatasChan() <-chan []Data {
	return dq.channel
}

func (dq *directQueue) Close() error {
	close(dq.quit)

	dq.mux.Lock()
	defer dq.mux.Unlock()
	dq.status = StatusClosed
	close(dq.channel)
	return nil
}

func (dq *directQueue) Delete() error {
	return dq.Close()
}

func (dq *directQueue) Depth() int64 {
	return 0
}

func (dq *directQueue) Empty() error {
	return nil
}
