package file

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type writeCloser struct {
	*bytes.Buffer
}

func (wc *writeCloser) Close() error {
	wc.Buffer.WriteString("Closed")
	return nil
}

func Test_writer(t *testing.T) {
	buf := bytes.NewBuffer([]byte(""))
	w := &writer{
		wc: &writeCloser{
			Buffer: buf,
		},
	}

	w.SetBusy()
	assert.True(t, w.IsBusy())
	w.SetIdle()
	assert.False(t, w.IsBusy())

	// 并发测试
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			w.SetBusy()
			wg.Done()
		}()
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			assert.True(t, w.IsBusy())
			w.SetIdle()
			wg.Done()
		}()
	}
	wg.Wait()
	assert.False(t, w.IsBusy())

	n, err := w.Write([]byte("something"))
	assert.NoError(t, err)
	assert.Equal(t, 9, n)

	buf.Reset()
	assert.NoError(t, w.Close())
	assert.Equal(t, "Closed", buf.String())
}
