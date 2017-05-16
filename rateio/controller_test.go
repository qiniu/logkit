package rateio

import (
	"bytes"
	"crypto/rand"
	"io"
	"log"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	size int    = 4 * 1024 * 1024
	b    []byte = make([]byte, size)
)

func init() {
	runtime.GOMAXPROCS(4)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rand.Read(b)
}

func TestControllerReader(t *testing.T) {
	{
		size := size
		c := NewController(2 * 1024 * 1024)
		defer c.Close()
		assert.Equal(t, 2*1024*1024, c.GetRateLimit())
		r1 := c.Reader(bytes.NewReader(b))
		r2 := c.Reader(bytes.NewReader(b))

		b1 := make([]byte, size)
		b2 := make([]byte, size)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			n, err := io.ReadFull(r1, b1)
			assert.NoError(t, err)
			assert.Equal(t, size, n)
			wg.Done()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			n, err := io.ReadFull(r2, b2)
			assert.NoError(t, err)
			assert.Equal(t, size, n)
			wg.Done()
		}()

		now := time.Now()
		wg.Wait()
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.Equal(t, b, b1)
		assert.Equal(t, b, b2)
	}
}

func TestControllerWriter(t *testing.T) {
	{
		size := size
		c := NewController(2 * 1024 * 1024)
		defer c.Close()

		b1 := new(bytes.Buffer)
		b2 := new(bytes.Buffer)
		w1 := c.Writer(b1)
		w2 := c.Writer(b2)

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			n, err := io.Copy(w1, bytes.NewReader(b))
			assert.NoError(t, err)
			assert.Equal(t, size, int(n))
			wg.Done()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			n, err := io.Copy(w2, bytes.NewReader(b))
			assert.NoError(t, err)
			assert.Equal(t, size, int(n))
			wg.Done()
		}()

		now := time.Now()
		wg.Wait()
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.Equal(t, b, b1.Bytes())
		assert.Equal(t, b, b2.Bytes())
	}
}
