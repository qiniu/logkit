package ratelimit

import (
	"testing"
	"time"
)

func TestLimiter(t *testing.T) {
	limiter := NewLimiter(-1)
	for i := 0; i < 100; i++ {
		limiter.Limit(1000)
	}

	start := time.Now()
	limiter = NewLimiter(10000)
	for i := 0; i < 30; i++ {
		limiter.Limit(1001)
	}
	end := time.Now()
	duration := end.Sub(start)
	if duration < 3*time.Second {
		t.Errorf("duration less than %d seconds", (30*1001)/10000)
	}
}
