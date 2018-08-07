package ip

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const Null = "N/A"

var ErrInvalidIP = errors.New("invalid IP format")

type ErrInvalidFile struct {
	Format string
	Reason string
}

func (e ErrInvalidFile) Error() string {
	return fmt.Sprintf("invalid file format: %s - %s", e.Format, e.Reason)
}

// LocatorStore 按照文件路径保存了对应的 Locator，避免重复实例化浪费内存
type LocatorStore struct {
	lock      sync.RWMutex
	locators  map[string]Locator
	refCounts map[string]int
}

// Get 返回对应路径的 Locator 并将其引用计数加 1，如果 Locator 不存在则返回 nil
func (s *LocatorStore) Get(fpath string) Locator {
	s.lock.RLock()
	defer s.lock.RUnlock()
	old, ok := s.refCounts[fpath]
	if !ok {
		return nil
	}
	s.refCounts[fpath] = old + 1
	return s.locators[fpath]
}

// Set 将 Locator 按照指定路径保存到 Store 中
func (s *LocatorStore) Set(fpath string, loc Locator) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.locators[fpath] = loc
	s.refCounts[fpath] = 1
}

// Remove 将指定路径的 Locator 引用计数减 1 并在为零时从 Store 中移除
func (s *LocatorStore) Remove(fpath string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	old, _ := s.refCounts[fpath]
	if old <= 1 {
		if _, ok := s.locators[fpath]; !ok {
			return
		}
		delete(s.locators, fpath)
		delete(s.refCounts, fpath)
		return
	}
	s.refCounts[fpath] = old - 1
}

var locatorStore = &LocatorStore{
	locators:  make(map[string]Locator, 3), // 一般情况下最多就 dat、 datx 和 mmdb 相关的 Locator
	refCounts: make(map[string]int, 3),
}

// Locator represents an IP information loc.
type Locator interface {
	Find(string) (*LocationInfo, error)
	Close() error
}

// NewLocator returns a new IP locator based on extension of given data file.
func NewLocator(dataFile, language string) (Locator, error) {
	loc := locatorStore.Get(dataFile)
	if loc != nil {
		return loc, nil
	}

	var err error
	switch {
	case strings.HasSuffix(dataFile, ".dat"):
		loc, err = newDatLocator(dataFile)
	case strings.HasSuffix(dataFile, ".datx"):
		loc, err = newDatxLocator(dataFile)
	case strings.HasSuffix(dataFile, ".mmdb"):
		loc, err = newMmdbLocator(dataFile, language)
	default:
		return nil, errors.New("unrecognized data file format")
	}
	if err != nil {
		return nil, err
	}

	locatorStore.Set(dataFile, loc)
	return loc, nil
}
