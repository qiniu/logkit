package queue

import (
	"errors"
	"fmt"
	"sync"
)

const (
	StatusInit int32 = iota
	StatusClosed
)

type memoryQueue struct {
	name           string
	maxQueueLength int
	channel        chan []byte
	mux            sync.Mutex
	status         int32
}

func NewMemoryQueue(name string, maxQueueLength int) BackendQueue {
	return &memoryQueue{
		name:           name,
		mux:            sync.Mutex{},
		channel:        make(chan []byte, maxQueueLength),
		maxQueueLength: maxQueueLength,
		status:         StatusInit,
	}
}

func (mq *memoryQueue) Name() string {
	return mq.name
}

func (mq *memoryQueue) Put(msg []byte) error {
	if len(mq.channel) >= mq.maxQueueLength {
		fmt.Println("channel size", len(mq.channel))
		return errors.New("memory queue is full")
	}
	if mq.status == StatusClosed {
		return errors.New("memory queue is closed")
	}
	mq.mux.Lock()
	defer mq.mux.Unlock()
	if len(mq.channel) >= mq.maxQueueLength {
		fmt.Println("channel size", len(mq.channel))
		return errors.New("memory queue is full")
	}
	if mq.status == StatusClosed {
		return errors.New("memory queue is closed")
	}
	mq.channel <- msg
	return nil
}

func (mq *memoryQueue) ReadChan() <-chan []byte {
	return mq.channel
}

func (mq *memoryQueue) Close() error {
	mq.mux.Lock()
	defer mq.mux.Unlock()
	mq.status = StatusClosed
	close(mq.channel)
	return nil
}

func (mq *memoryQueue) Delete() error {
	return mq.Close()
}

func (mq *memoryQueue) Depth() int64 {
	mq.mux.Lock()
	defer mq.mux.Unlock()
	return int64(len(mq.channel))
}

func (mq *memoryQueue) Empty() error {
	mq.mux.Lock()
	defer mq.mux.Unlock()
	length := len(mq.channel)
	for i := 0; i < length; i++ {
		<-mq.channel
	}
	return nil
}
