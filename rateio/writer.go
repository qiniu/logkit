package rateio

import "io"

type rateWriter struct {
	underlying io.Writer
	c          *Controller
}

func (self *rateWriter) Write(p []byte) (written int, err error) {
write:
	size := len(p)
	size = self.c.assign(size)

	n, err := self.underlying.Write(p[:size])
	self.c.fill(size - n)
	written += n
	if err != nil {
		return
	}
	if size != len(p) {
		p = p[size:]
		goto write
	}
	return
}

func NewRateWriter(w io.Writer, ratePerSecond int) io.WriteCloser {
	c := NewController(ratePerSecond)
	w = c.Writer(w)
	return struct {
		io.Writer
		io.Closer
	}{
		Writer: w,
		Closer: c,
	}
}
