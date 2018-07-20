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
	lock     sync.RWMutex
	locators map[string]Locator
}

// Get 返回对应路径的 Locator，如果 Locator 不存在则返回 nil
func (s *LocatorStore) Get(fpath string) Locator {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.locators[fpath]
}

// Set 将 Locator 按照指定路径保存到 Store 中
func (s *LocatorStore) Set(fpath string, loc Locator) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.locators[fpath] = loc
}

var locatorStore = &LocatorStore{
	locators: make(map[string]Locator, 2), // 一般情况下最多就 dat 和 datx 相关的 Locator
}

// Locator represents an IP information loc.
type Locator interface {
	Find(string) (*LocationInfo, error)
}

// NewLocator returns a new IP locator based on extension of given data file.
func NewLocator(dataFile string) (Locator, error) {
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
	default:
		return nil, errors.New("unrecognized data file format")
	}
	if err != nil {
		return nil, err
	}

	locatorStore.Set(dataFile, loc)
	return loc, nil
}
