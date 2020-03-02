package utils

import (
	"reflect"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	b := NewBackoff(2, 4, 1*time.Second, 32*time.Second)

	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 2*time.Second)
	equals(t, b.Duration(), 4*time.Second)
	equals(t, b.Duration(), 8*time.Second)
	equals(t, b.Duration(), 16*time.Second)
	equals(t, b.Duration(), 32*time.Second)
	equals(t, b.Duration(), 32*time.Second)
	equals(t, b.Duration(), 32*time.Second)

	b.Reset()

	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 1*time.Second)
	equals(t, b.Duration(), 2*time.Second)
	equals(t, b.Duration(), 4*time.Second)
	equals(t, b.Duration(), 8*time.Second)
	equals(t, b.Duration(), 16*time.Second)
	equals(t, b.Duration(), 32*time.Second)
	equals(t, b.Duration(), 32*time.Second)
	equals(t, b.Duration(), 32*time.Second)

	b.Reset()

	equals(t, b.Duration(), 1*time.Second)
	for i := 0; i < 2000; i++ {
		b.Duration()
	}
	equals(t, b.Duration(), 32*time.Second)
	equals(t, b.Duration(), 32*time.Second)
	equals(t, b.Duration(), 32*time.Second)
}

func equals(t *testing.T, v1, v2 interface{}) {
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("got %v, want %v", v1, v2)
	}
}
