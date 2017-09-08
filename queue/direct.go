package queue

import (
	"errors"
	"sync"
)

const (
	StatusInit int32 = iota
	StatusClosed
)

type directQueue struct {
	name    string
	channel chan []byte
	mux     sync.Mutex
	status  int32
	quit    chan bool
}

func NewDirectQueue(name string) BackendQueue {
	return &directQueue{
		name:    name,
		channel: make(chan []byte),
		mux:     sync.Mutex{},
		status:  StatusInit,
		quit:    make(chan bool),
	}
}

func (dq *directQueue) Name() string {
	return dq.name
}

func (dq *directQueue) Put(msg []byte) error {
	dq.mux.Lock()
	defer dq.mux.Unlock()
	if dq.status == StatusClosed {
		return errors.New("direct queue is closed")
	}
	select {
	case dq.channel <- msg:
		return nil
	case <-dq.quit:
		return errors.New("direct queue is closed")
	}
}

func (dq *directQueue) ReadChan() <-chan []byte {
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
