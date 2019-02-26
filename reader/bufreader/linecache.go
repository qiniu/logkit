package bufreader

import (
	"sync"
)

type LineCache struct {
	lines []string
	lock  *sync.RWMutex
}

func NewLineCache() *LineCache {
	return &LineCache{
		lines: make([]string, 0, 16),
		lock:  new(sync.RWMutex),
	}
}

func (l *LineCache) Size() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.lines)
}

func (l *LineCache) Set(r []string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.lines = r
}

func (l *LineCache) Append(r string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.lines = append(l.lines, r)
}

func (l *LineCache) TotalLen() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	var ret int
	for _, v := range l.lines {
		ret += len(v)
	}
	return ret
}

func (l *LineCache) Combine() []byte {
	if l.Size() <= 0 {
		return make([]byte, 0)
	}
	n := l.TotalLen()
	xb := make([]byte, n)
	l.lock.RLock()
	defer l.lock.RUnlock()
	bp := copy(xb, l.lines[0])
	for _, s := range l.lines[1:] {
		bp += copy(xb[bp:], s)
	}
	return xb
}
