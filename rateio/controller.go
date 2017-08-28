// rate limit and traffic control
package rateio

import (
	"io"
	"sync"
	"time"
)

var Window = 50 * time.Millisecond

// Controller can limit multiple io.Reader(or io.Writer) within specific rate.
type Controller struct {
	capacity      int
	threshold     int
	cond          *sync.Cond
	done          chan struct{}
	ratePerSecond int
	closeOnce     *sync.Once
}

func NewController(ratePerSecond int) *Controller {
	capacity := ratePerSecond * int(Window) / int(time.Second)
	if capacity < 64 {
		capacity = 64
	}
	self := &Controller{
		ratePerSecond: ratePerSecond,
		threshold:     capacity,
		capacity:      capacity,
		cond:          sync.NewCond(new(sync.Mutex)),
		done:          make(chan struct{}, 1),
		closeOnce:     &sync.Once{},
	}
	go self.run(capacity)
	return self
}

func (self *Controller) assign(size int) int {
	self.cond.L.Lock()
	for self.capacity == 0 {
		self.cond.Wait()
	}
	if size > self.capacity {
		size = self.capacity
	}
	self.capacity -= size
	self.cond.L.Unlock()
	return size
}

func (self *Controller) fill(size int) {
	if size <= 0 {
		return
	}

	self.cond.L.Lock()
	self.capacity += size
	if self.capacity > self.threshold {
		self.capacity = self.threshold
	}
	self.cond.L.Unlock()
	self.cond.Broadcast()
}

func (self *Controller) run(capacity int) {
	t := time.NewTicker(Window)
	for {
		select {
		case <-t.C:
			self.cond.L.Lock()
			self.capacity = capacity
			self.cond.L.Unlock()
			self.cond.Broadcast()
		case <-self.done:
			t.Stop()
			// 认为Close一定发生在所有Read之后
			return
		}
	}
}

func (self *Controller) Close() error {
	self.closeOnce.Do(func() {
		self.done <- struct{}{}
	})
	return nil
}

func (self *Controller) Reader(underlying io.Reader) io.Reader {
	return &rateReader{
		underlying: underlying,
		c:          self,
	}
}

func (self *Controller) Writer(underlying io.Writer) io.Writer {
	return &rateWriter{
		underlying: underlying,
		c:          self,
	}
}

func (self *Controller) GetRateLimit() int {
	return self.ratePerSecond
}
