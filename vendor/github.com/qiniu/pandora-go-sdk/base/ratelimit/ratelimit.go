//ratelimit and traffic control
package ratelimit

import (
	"sync"
	"time"
)

var Window = 50 * time.Millisecond

// Limiter can limit multiple io.Reader(or io.Writer) within specific rate.
type Limiter struct {
	capacity      int64
	threshold     int64
	cond          *sync.Cond
	done          chan struct{}
	ratePerSecond int64
	closeOnce     *sync.Once
}

// NewLimiter 限速的最小粒度是20
func NewLimiter(ratePerSecond int64) *Limiter {
	quantity := (ratePerSecond*int64(Window) + int64(time.Second)) / int64(time.Second)
	self := &Limiter{
		ratePerSecond: ratePerSecond,
		threshold:     ratePerSecond,
		capacity:      ratePerSecond,
		cond:          sync.NewCond(new(sync.Mutex)),
		done:          make(chan struct{}, 1),
		closeOnce:     &sync.Once{},
	}
	go self.run(quantity)
	return self
}

func (self *Limiter) Assign(size int64) int64 {
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

func (self *Limiter) Fill(size int64) {
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

func (self *Limiter) run(quantity int64) {
	t := time.NewTicker(Window)
	for {
		select {
		case <-t.C:
			self.cond.L.Lock()
			self.capacity += quantity
			if self.capacity >= self.threshold {
				self.capacity = self.threshold
			}
			self.cond.L.Unlock()
			self.cond.Broadcast()
		case <-self.done:
			t.Stop()
			// 认为Close一定发生在所有Read之后
			return
		}
	}
}

func (self *Limiter) Close() error {
	self.closeOnce.Do(func() {
		self.done <- struct{}{}
	})
	return nil
}

func (self *Limiter) GetRateLimit() int64 {
	return self.ratePerSecond
}
