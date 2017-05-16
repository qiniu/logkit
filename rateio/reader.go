package rateio

import "io"

type rateReader struct {
	underlying io.Reader
	c          *Controller
}

func (self *rateReader) Read(p []byte) (n int, err error) {

	size := len(p)
	size = self.c.assign(size)

	n, err = self.underlying.Read(p[:size])
	self.c.fill(size - n)
	return
}

func NewRateReader(r io.Reader, ratePerSecond int) io.ReadCloser {
	c := NewController(ratePerSecond)
	r = c.Reader(r)
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: r,
		Closer: c,
	}
}
