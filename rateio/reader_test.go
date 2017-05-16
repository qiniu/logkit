package rateio

import (
	"bytes"
	"io"
	"log"
	"math"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateReader(t *testing.T) {
	{
		size := size

		rc := NewRateReader(bytes.NewReader(b), 1*1024*1024)
		defer rc.Close()

		b2 := make([]byte, size)
		now := time.Now()
		n, err := io.ReadFull(rc, b2)
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b, b2)
	}
	{
		size := size

		rc := NewRateReader(bytes.NewReader(b), 1*1024*1024)
		defer rc.Close()

		b2 := new(bytes.Buffer)
		now := time.Now()
		n, err := io.Copy(b2, rc)
		elapsed := time.Since(now).Seconds()
		log.Println(elapsed)
		assert.True(t, math.Abs(4-elapsed) < 0.5)
		assert.NoError(t, err)
		assert.Equal(t, size, int(n))
		assert.Equal(t, b, b2.Bytes())
	}
	{
		size := size

		rc := NewRateReader(iotest.TimeoutReader(bytes.NewReader(b)), 1*1024*1024)
		defer rc.Close()

		b2 := make([]byte, size)
		n, err := io.ReadFull(rc, b2)
		assert.Equal(t, iotest.ErrTimeout, err)
		assert.Equal(t, 1*1024*1024*int(Window)/int(time.Second), n)
		assert.Equal(t, b[:n], b2[:n])
	}
	{
		size := 10
		rc := NewRateReader(iotest.OneByteReader(bytes.NewReader(b)), 1*1024*1024)
		defer rc.Close()

		b2 := make([]byte, size)
		n, err := io.ReadFull(rc, b2)
		assert.NoError(t, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b[:n], b2[:n])
	}
	{
		size := 10
		rc := NewRateReader(iotest.OneByteReader(bytes.NewReader(b[:size])), 4)
		defer rc.Close()

		b2 := make([]byte, size+1)
		n, err := io.ReadFull(rc, b2)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
		assert.Equal(t, size, n)
		assert.Equal(t, b[:n], b2[:n])
	}
}
