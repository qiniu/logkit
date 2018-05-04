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
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"unsafe"

	"time"

	"github.com/axgle/mahonia"
	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	defaultBufSize           = 4096
	MaxHeadPatternBufferSize = 20 * 1024 * 1024
)

var (
	ErrInvalidUnreadByte = errors.New("bufio: invalid use of UnreadByte")
	ErrInvalidUnreadRune = errors.New("bufio: invalid use of UnreadRune")
	ErrBufferFull        = errors.New("bufio: buffer full")
	ErrNegativeCount     = errors.New("bufio: negative count")
)

// buffered input.

type LastSync struct {
	cache string
	buf   string
	r, w  int
}

// BufReader implements buffering for an FileReader object.
type BufReader struct {
	stopped       int32
	buf           []byte
	mutiLineCache []string
	rd            FileReader // reader provided by the client
	r, w          int        // buf read and write positions
	err           error
	lastByte      int
	lastRuneSize  int
	lastSync      LastSync

	mux     sync.Mutex
	decoder mahonia.Decoder

	meta            *Meta // 存放offset的元信息
	multiLineRegexp *regexp.Regexp

	stats     StatsInfo
	statsLock sync.RWMutex

	lastErrShowTime time.Time
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
	if meta == nil {
		return nil, errors.New("meta is nil pointer error")
	}
	readPos, writePos, lastSize, err := meta.ReadBufMeta()
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] %s cannot find out buf meta file, start from zero", meta.RunnerName, rd.Name())
			bufFromFile = false
		} else {
			log.Warnf("Runner[%v] %s cannot read buf meta info %v", meta.RunnerName, rd.Name(), err)
			return nil, err
		}
	} else {
		log.Debugf("Runner[%v] %v restore meta success %v %v %v", meta.RunnerName, meta.LogPath(), readPos, writePos, lastSize)
	}
	if size < lastSize {
		size = lastSize
	}
	linesbytes, err := meta.ReadCacheLine()
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] ReadCacheLine from file error %v", meta.RunnerName, err)
		} else {
			log.Warnf("Runner[%v] ReadCacheLine from file error %v", meta.RunnerName, err)
		}
		err = nil
		linesbytes = []byte("")
	} else {
		log.Debugf("Runner[%v] %v restore line cache success: [%v]", meta.RunnerName, meta.LogPath(), string(linesbytes))
	}

	r := new(BufReader)
	r.stopped = 0
	r.reset(make([]byte, size), rd)

	r.meta = meta
	if r.meta.GetEncodingWay() != "" {
		r.decoder = mahonia.NewDecoder(r.meta.GetEncodingWay())
		if r.decoder == nil {
			log.Warnf("Encoding Way [%v] is not supported, will read as utf-8", r.meta.GetEncodingWay())
		}
	}
	if meta.IsExist() && meta.IsValid() {
		r.r = readPos
		r.w = writePos

		if bufFromFile {
			_, err = meta.ReadBuf(r.buf)
			if err != nil {
				return nil, err
			} else {
				log.Debugf("Runner[%v] %v restore buf success [%v]", meta.RunnerName, meta.LogPath(), string(r.buf))
			}
		}
	}
	if len(linesbytes) > 0 {
		r.mutiLineCache = append(r.mutiLineCache, string(linesbytes))
	}
	return r, nil
}

func (b *BufReader) SetMode(mode string, v interface{}) (err error) {
	b.multiLineRegexp, err = HeadPatternMode(mode, v)
	if err != nil {
		err = fmt.Errorf("%v set mode error %v ", b.Name(), err)
		return
	}
	return
}

func (b *BufReader) reset(buf []byte, r FileReader) {
	*b = BufReader{
		buf:           buf,
		rd:            r,
		lastByte:      -1,
		lastRuneSize:  -1,
		mutiLineCache: make([]string, 0, 16),
		lastSync:      LastSync{},
		mux:           sync.Mutex{},
		statsLock:     sync.RWMutex{},
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
		panic(fmt.Sprintf("Runner[%v] bufio: tried to fill full buffer", b.meta.RunnerName))
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
	b.mux.Lock()
	defer b.mux.Unlock()
	for {
		if atomic.LoadInt32(&b.stopped) > 0 {
			log.Warn("BufReader was stopped while reading...")
			return
		}
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
	ret = *(*string)(unsafe.Pointer(&bytes))
	//默认都是utf-8
	if b.meta.GetEncodingWay() != "" && b.meta.GetEncodingWay() != "utf-8" && b.decoder != nil {
		ret = b.decoder.ConvertString(ret)
	}
	return
}

//ReadPattern读取日志直到匹配行首模式串
func (b *BufReader) ReadPattern() (string, error) {
	var maxTimes int = 0
	for {
		line, err := b.ReadString('\n')
		//读取到line的情况
		if len(line) > 0 {
			if len(b.mutiLineCache) <= 0 {
				b.mutiLineCache = []string{line}
				continue
			}
			//匹配行首，成功则返回之前的cache，否则加入到cache，返回空串
			if b.multiLineRegexp.Match([]byte(line)) {
				tmp := line
				line = string(b.formMutiLine())
				b.mutiLineCache = make([]string, 0, 16)
				b.mutiLineCache = append(b.mutiLineCache, tmp)
				return line, err
			}
			b.mutiLineCache = append(b.mutiLineCache, line)
			maxTimes = 0
		} else { //读取不到日志
			if err != nil {
				line = string(b.formMutiLine())
				b.mutiLineCache = make([]string, 0, 16)
				return line, err
			}
			maxTimes++
			//对于又没有错误，也读取不到日志的情况，最多允许10次重试
			if maxTimes > 10 {
				log.Debugf("Runner[%v] %v read empty line 10 times return empty", b.meta.RunnerName, b.Name())
				return "", nil
			}
		}
		//对于读取到了Cache的情况，继续循环，直到超过最大限制
		if b.calcMutiLineCache() > MaxHeadPatternBufferSize {
			line = string(b.formMutiLine())
			b.mutiLineCache = make([]string, 0, 16)
			return line, err
		}
	}
}

func (b *BufReader) formMutiLine() []byte {
	if len(b.mutiLineCache) <= 0 {
		return make([]byte, 0)
	}
	n := 0
	for i := 0; i < len(b.mutiLineCache); i++ {
		n += len(b.mutiLineCache[i])
	}

	xb := make([]byte, n)
	bp := copy(xb, b.mutiLineCache[0])
	for _, s := range b.mutiLineCache[1:] {
		bp += copy(xb[bp:], s)
	}
	return xb
}

func (b *BufReader) calcMutiLineCache() (ret int) {
	for _, v := range b.mutiLineCache {
		ret += len(v)
	}
	return
}

//ReadLine returns a string line as a normal Reader
func (b *BufReader) ReadLine() (ret string, err error) {
	if b.multiLineRegexp == nil {
		ret, err = b.ReadString('\n')
		if os.IsNotExist(err) {
			if b.lastErrShowTime.Add(5 * time.Second).Before(time.Now()) {
				log.Errorf("%v ReadLine err %v", b.meta.RunnerName, err)
				b.lastErrShowTime = time.Now()
			}
		}
	} else {
		ret, err = b.ReadPattern()
	}
	if skp, ok := b.rd.(LineSkipper); ok {
		if skp.IsNewOpen() {
			log.Infof("%v Skip line %v as first line skipper was configured %v", b.meta.RunnerName, ret)
			ret = ""
			skp.SetSkipped()
		}
	}
	if err != nil && err != io.EOF {
		b.setStatsError(err.Error())
	}
	return
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
	atomic.StoreInt32(&b.stopped, 1)
	return b.rd.Close()
}

func (b *BufReader) Status() StatsInfo {
	b.statsLock.RLock()
	defer b.statsLock.RUnlock()
	return b.stats
}

func (b *BufReader) setStatsError(err string) {
	b.statsLock.Lock()
	defer b.statsLock.Unlock()
	b.stats.Errors++
	b.stats.LastError = err
}

func (b *BufReader) Lag() (rl *LagInfo, err error) {
	lr, ok := b.rd.(LagReader)
	if ok {
		return lr.Lag()
	}
	err = fmt.Errorf("internal reader haven't support lag info yet")
	return
}

func (b *BufReader) SyncMeta() {
	b.mux.Lock()
	defer b.mux.Unlock()
	linecache := string(b.formMutiLine())
	//把linecache也缓存
	if b.lastSync.cache != linecache || b.lastSync.buf != string(b.buf) || b.r != b.lastSync.r || b.w != b.lastSync.w {
		log.Debugf("Runner[%v] %v sync meta started, linecache [%v] buf [%v] （%v %v）", b.meta.RunnerName, b.Name(), linecache, string(b.buf), b.r, b.w)
		err := b.meta.WriteBuf(b.buf, b.r, b.w, len(b.buf))
		if err != nil {
			log.Errorf("Runner[%v] %s cannot write buf, err :%v", b.meta.RunnerName, b.Name(), err)
			return
		}
		err = b.meta.WriteCacheLine(linecache)
		if err != nil {
			log.Errorf("Runner[%v] %s cannot write linecache, err :%v", b.meta.RunnerName, b.Name(), err)
			return
		}
		b.lastSync.cache = linecache
		b.lastSync.buf = string(b.buf)
		b.lastSync.r = b.r
		b.lastSync.w = b.w
		log.Debugf("Runner[%v] %v sync meta succeed, linecache [%v] buf [%v] （%v %v）", b.meta.RunnerName, b.Name(), linecache, string(b.buf), b.r, b.w)
	} else {
		log.Debugf("Runner[%v] %v meta data was just syncd, cache %v, buf %v, r,w =(%v,%v), ignore this sync...", b.meta.RunnerName, b.Name(), linecache, string(b.buf), b.r, b.w)
	}
	err := b.rd.SyncMeta()
	if err != nil {
		log.Errorf("Runner[%v] %s cannot write reader %v's meta info, err %v", b.meta.RunnerName, b.Name(), b.rd.Name(), err)
		return
	}
}
