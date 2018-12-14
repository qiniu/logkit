package ratelimit

import (
	"sync/atomic"
	"time"
)

type Limiter interface {
	Limit(uint64) bool
}

type noopLimiter struct{}

func (r *noopLimiter) Limit(uint64) bool {
	return true
}

type countLimiter struct {
	limit  uint64
	count  uint64
	lockCh chan struct{}
}

func NewLimiter(limit int64) Limiter {
	if limit <= 0 {
		return &noopLimiter{}
	}
	r := &countLimiter{
		limit:  uint64(limit),
		count:  0,
		lockCh: make(chan struct{}),
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if r.loadCount() > r.limit {
				select {
				case <-r.lockCh:
				default:
					r.resetCount()
				}
			}
			if r.loadCount() > 0 {
				r.resetCount()
			}
		}
	}()
	return r
}

func (r *countLimiter) Limit(delta uint64) bool {
	r.addCount(delta)
	if r.loadCount() > r.limit {
		r.lockCh <- struct{}{}
	}
	return true
}

func (r *countLimiter) addCount(delta uint64) {
	atomic.AddUint64(&r.count, delta)
}

func (r *countLimiter) loadCount() uint64 {
	return atomic.LoadUint64(&r.count)
}

func (r *countLimiter) resetCount() {
	atomic.StoreUint64(&r.count, 0)
}
