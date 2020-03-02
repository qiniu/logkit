package utils

import (
	"math"
	"time"
)

type Backoff struct {
	factor    float64
	threshold float64
	attempt   float64
	min, max  time.Duration
}

func NewBackoff(factor, threshold float64, min, max time.Duration) *Backoff {
	return &Backoff{
		factor:    factor,
		threshold: threshold,
		min:       min,
		max:       max,
	}
}

func (b *Backoff) Duration() time.Duration {
	d := b.ForAttempt(b.attempt)
	b.attempt++
	return d
}

func (b *Backoff) ForAttempt(attempt float64) time.Duration {
	min := b.min
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	max := b.max
	if max <= 0 {
		max = 10 * time.Second
	}
	if min >= max {
		return max
	}

	factor := b.factor
	if factor <= 0 {
		factor = 2
	}
	threshold := b.threshold
	if threshold <= 0 {
		threshold = 2
	}
	expf := attempt - threshold
	if expf < 0 {
		expf = 0
	}
	minf := float64(min)
	durf := minf * math.Pow(factor, expf)
	if durf > float64(max) {
		return max
	}
	return time.Duration(durf)
}

func (b *Backoff) Reset() {
	b.attempt = 0
}

func (b *Backoff) Attempt() float64 {
	return b.attempt
}
