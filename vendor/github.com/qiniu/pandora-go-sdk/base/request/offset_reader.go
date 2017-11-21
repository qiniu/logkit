package request

import (
	"io"
	"sync"
)

type offsetReader struct {
	buf    io.ReadSeeker
	lock   sync.RWMutex
	closed bool
}

func newOffsetReader(buf io.ReadSeeker, offset int64) *offsetReader {
	reader := &offsetReader{}
	buf.Seek(offset, 0)

	reader.buf = buf
	return reader
}

func (r *offsetReader) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.closed = true
	return nil
}

func (r *offsetReader) Read(p []byte) (int, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.closed {
		return 0, io.EOF
	}

	return r.buf.Read(p)
}

func (r *offsetReader) CloseAndCopy(offset int64) *offsetReader {
	r.Close()
	return newOffsetReader(r.buf, offset)
}
