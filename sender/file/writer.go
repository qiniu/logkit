package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/utils"
)

const defaultFileWriterPoolSize = 10

type writer struct {
	inUseStatus int32 // Note: 原子操作
	lastWrote   int32
	wc          io.WriteCloser
}

// IsBusy 当任务数为 0 时返回 false 表示可以被安全回收，否则返回 true
func (w *writer) IsBusy() bool {
	return atomic.LoadInt32(&w.inUseStatus) > 0
}

// SetBusy 将 writer 的正在处理任务数添加 1，使对象表示正忙，防止因 lastWrote 过期被意外回收
func (w *writer) SetBusy() {
	atomic.AddInt32(&w.inUseStatus, 1)
}

// SetIdle 将 writer 的正在处理任务数减去 1
func (w *writer) SetIdle() {
	atomic.AddInt32(&w.inUseStatus, -1)
}

func (w *writer) Write(b []byte) (int, error) {
	atomic.StoreInt32(&w.lastWrote, int32(time.Now().Unix()))
	return w.wc.Write(b)
}

func (w *writer) Close() error {
	return w.wc.Close()
}

// writerStore 维护了指定大小的文件句柄池，根据需要自动关闭和开启句柄。
// 因为打开新句柄时可能无法立刻关闭最不活跃的句柄，因此对句柄池大小的设置为最终一致而不是实时一致
type writerStore struct {
	size    int
	lock    sync.RWMutex
	writers map[string]*writer
}

func newWriterStore(size int) *writerStore {
	if size <= 0 {
		size = defaultFileWriterPoolSize
	}
	return &writerStore{
		size:    size,
		writers: make(map[string]*writer, size),
	}
}

// Get 根据文件名称返回对应的 writer，如果不存在则返回 nil，调用者在用完对象后必须调用 SetIdle 便于回收
func (s *writerStore) Get(filename string) *writer {
	s.lock.RLock()
	defer s.lock.RUnlock()

	w := s.writers[filename]
	if w != nil {
		w.SetBusy()
	}
	return w
}

// NewWriter 会根据文件名新建一个文件句柄，如果打开的句柄数已经达到限制，则会关闭最不活跃的句柄
func (s *writerStore) NewWriter(filename string) (w *writer, _ error) {
	w = s.Get(filename)
	if w != nil {
		return w, nil
	}

	dir := filepath.Dir(filename)
	if !utils.IsExist(dir) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("create parent directory: %v", err)
		}
	}
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %v", err)
	}
	w = &writer{
		wc: f,
	}
	w.SetBusy()

	s.lock.Lock()
	defer s.lock.Unlock()

	s.writers[filename] = w

	// 关闭并删除 (total - size) 个最不活跃的句柄，因为可能同时创建多个句柄而当时无法确认谁是最不活跃的
	for i := 1; i <= len(s.writers)-s.size; i++ {
		var expiredFilename string
		lastWrite := int32(time.Now().Unix())
		for name, w := range s.writers {
			currentLastWrite := atomic.LoadInt32(&w.lastWrote)
			if currentLastWrite == 0 || w.IsBusy() {
				continue // 暂时忽略刚创建和正忙的句柄
			}

			if currentLastWrite < lastWrite {
				expiredFilename = name
				lastWrite = currentLastWrite
			}
		}

		if len(expiredFilename) > 0 {
			if err := s.writers[expiredFilename].Close(); err != nil {
				log.Errorf("Failed to close expired file writer %q: %v", expiredFilename, err)
			}
			delete(s.writers, expiredFilename)
		}
	}
	return w, nil
}

// Write 向指定文件写入数据，调用者不需要关心文件是否存在，函数自身会协调打开的文件句柄
func (s *writerStore) Write(filename string, b []byte) (int, error) {
	w, err := s.NewWriter(filename)
	if err != nil {
		return 0, fmt.Errorf("new writer: %v", err)
	}
	defer w.SetIdle()
	return w.Write(b)
}

func (s *writerStore) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var lastErr error
	for name, w := range s.writers {
		lastErr = w.Close()
		delete(s.writers, name)
	}
	return lastErr
}
