// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bufio implements buffered I/O.  It wraps an FileReader or io.Writer
// object, creating another object (Reader or Writer) that also implements
// the interface but provides buffering and some help for textual I/O.
package reader

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/qiniu/log"

	"github.com/axgle/mahonia"
)

const (
	defaultBufSize = 4096
)

var (
	ErrInvalidUnreadByte = errors.New("bufio: invalid use of UnreadByte")
	ErrInvalidUnreadRune = errors.New("bufio: invalid use of UnreadRune")
	ErrBufferFull        = errors.New("bufio: buffer full")
	ErrNegativeCount     = errors.New("bufio: negative count")
)

// buffered input.

// BufReader implements buffering for an FileReader object.
type BufReader struct {
	buf          []byte
	rd           FileReader // reader provided by the client
	r, w         int        // buf read and write positions
	err          error
	lastByte     int
	lastRuneSize int

	decoder mahonia.Decoder

	meta *Meta // 存放offset的元信息
}

const minReadBufferSize = 16
const maxConsecutiveEmptyReads = 100

// NewReaderSize returns a new Reader whose buffer has at least the specified
// size. If the argument FileReader is already a Reader with large enough
// size, it returns the underlying Reader.
func NewReaderSize(rd FileReader, meta *Meta, size int) (*BufReader, error) {
	// Is it already a Reader?
	if size < minReadBufferSize {
		size = minReadBufferSize
	}

	bufFromFile := true

	readPos, writePos, lastSize, err := meta.ReadBufMeta()
	if err != nil {
		if os.IsNotExist(err) {
			log.Warnf("%s cannot find out buf meta file, start from zero", rd.Name())
			bufFromFile = false
		} else {
			log.Errorf("%s cannot read buf meta info %v", rd.Name(), err)
			return nil, err
		}
	}
	if size < lastSize {
		size = lastSize
	}
	r := new(BufReader)
	r.reset(make([]byte, size), rd)
	r.meta = meta
	if r.meta.GetEncodingWay() != "" {
		r.decoder = mahonia.NewDecoder(r.meta.GetEncodingWay())
	}
	if meta.IsExist() && meta.IsValid() {
		r.r = readPos
		r.w = writePos

		if bufFromFile {
			_, err = meta.ReadBuf(r.buf)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

func (b *BufReader) reset(buf []byte, r FileReader) {
	*b = BufReader{
		buf:          buf,
		rd:           r,
		lastByte:     -1,
		lastRuneSize: -1,
	}
}

var errNegativeRead = errors.New("bufio: reader returned negative count from Read")

// fill reads a new chunk into the buffer.
func (b *BufReader) fill() {
	// Slide existing data to beginning.
	if b.r > 0 {
		copy(b.buf, b.buf[b.r:b.w])
		b.w -= b.r
		b.r = 0
	}

	if b.w >= len(b.buf) {
		panic("bufio: tried to fill full buffer")
	}

	// Read new data: try a limited number of times.
	for i := maxConsecutiveEmptyReads; i > 0; i-- {
		n, err := b.rd.Read(b.buf[b.w:])
		if n < 0 {
			panic(errNegativeRead)
		}
		b.w += n
		if err != nil {
			b.err = err
			return
		}
		if n > 0 {
			return
		}
	}
	b.err = io.ErrNoProgress
}

func (b *BufReader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

// buffered returns the number of bytes that can be read from the current buffer.
func (b *BufReader) buffered() int { return b.w - b.r }

// readSlice reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If readSlice encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// readSlice fails with error ErrBufferFull if the buffer fills without a delim.
// Because the data returned from readSlice will be overwritten
// by the next I/O operation, most clients should use
// readBytes or ReadString instead.
// readSlice returns err != nil if and only if line does not end in delim.
func (b *BufReader) readSlice(delim byte) (line []byte, err error) {
	for {

		// Search buffer.
		if i := bytes.IndexByte(b.buf[b.r:b.w], delim); i >= 0 {
			line = b.buf[b.r : b.r+i+1]
			b.r += i + 1
			break
		}
		// Pending error?
		if b.err != nil {
			line = b.buf[b.r:b.w]
			b.r = b.w
			err = b.readErr()
			break
		}

		// Buffer full?
		if b.buffered() >= len(b.buf) {
			b.r = b.w
			line = b.buf
			err = ErrBufferFull
			break
		}

		b.fill() // buffer is not full
	}
	// Handle last byte, if any.
	if i := len(line) - 1; i >= 0 {
		b.lastByte = int(line[i])
		b.lastRuneSize = -1
	}

	return
}

// readBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// If readBytes encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// readBytes returns err != nil if and only if the returned data does not end in
// delim.
// For simple uses, a Scanner may be more convenient.
func (b *BufReader) readBytes(delim byte) ([]byte, error) {
	// Use readSlice to look for array,
	// accumulating full buffers.
	var frag []byte
	var full [][]byte
	var err error
	for {
		var e error
		frag, e = b.readSlice(delim)
		if e == nil { // got final fragment
			break
		}

		if e != ErrBufferFull { // unexpected error
			err = e
			break
		}

		// Make a copy of the buffer.
		buf := make([]byte, len(frag))
		copy(buf, frag)
		full = append(full, buf)
	}
	// Allocate new buffer to hold the full pieces and the fragment.
	n := 0
	for i := range full {
		n += len(full[i])
	}
	n += len(frag)

	// Copy full pieces and fragment in.
	buf := make([]byte, n)
	n = 0
	for i := range full {
		n += copy(buf[n:], full[i])
	}
	copy(buf[n:], frag)
	return buf, err
}

// ReadString reads until the first occurrence of delim in the input,
// returning a string containing the data up to and including the delimiter.
// If ReadString encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadString returns err != nil if and only if the returned data does not end in
// delim.
// For simple uses, a Scanner may be more convenient.
func (b *BufReader) ReadString(delim byte) (ret string, err error) {
	bytes, err := b.readBytes(delim)
	ret = string(bytes)
	//默认都是utf-8
	if b.meta.GetEncodingWay() != "" && b.meta.GetEncodingWay() != "utf-8" {
		ret = b.decoder.ConvertString(ret)
	}
	return
}

//ReadLine returns a string line as a normal Reader
func (b *BufReader) ReadLine() (string, error) {
	return b.ReadString('\n')
}

var errNegativeWrite = errors.New("bufio: writer returned negative count from Write")

// writeBuf writes the Reader's buffer to the writer.
func (b *BufReader) writeBuf(w io.Writer) (int64, error) {
	n, err := w.Write(b.buf[b.r:b.w])
	if n < 0 {
		panic(errNegativeWrite)
	}
	b.r += n
	return int64(n), err
}

func (b *BufReader) Name() string {
	return b.rd.Name()
}

func (b *BufReader) Source() string {
	return b.rd.Source()
}

func (b *BufReader) Close() error {
	return b.rd.Close()
}

func (b *BufReader) SyncMeta() {
	err := b.meta.WriteBuf(b.buf, b.r, b.w, len(b.buf))
	if err != nil {
		log.Errorf("%s cannot write buf, err :%v", b.Name(), err)
		return
	}
	err = b.rd.SyncMeta()
	if err != nil {
		log.Errorf("%s cannot write reader %v's meta info, err %v", b.Name(), b.rd.Name(), err)
		return
	}
}
