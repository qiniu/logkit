package reader

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"syscall"
	"time"
)

var ErrStopped = errors.New("runner stopped")
var ErrNoFileChosen = errors.New("no files found")
var ErrMetaFileRead = errors.New("cannot read meta file")
var ErrFileNotRegular = errors.New("file is not regular")
var ErrFileNotDir = errors.New("file is not directory")

var WaitNoSuchFile = time.Second

// getInode 获得文件inode
func getInode(f os.FileInfo) uint64 {
	s := f.Sys()
	if s == nil {
		return 0
	}
	switch s := s.(type) {
	case *syscall.Stat_t:
		return s.Ino
	default:
		return 0
	}
}

// getLatestFile 获得当前文件夹下最新的文件
func getLatestFile(logdir string) (os.FileInfo, error) {
	return getMaxFile(logdir, noCondition, modTimeLater)
}

// getOldestFile 获得当前文件夹下最旧的文件
func getOldestFile(logdir string) (os.FileInfo, error) {
	return getMinFile(logdir, noCondition, modTimeLater)
}

// getMaxFile 在指定的限制条件condition下，根据比较函数gte 选择最大的os.FileInfo
// condition 文件必须满足的条件
// gte f1 >= f2 则返回true
func getMaxFile(logdir string, condition func(os.FileInfo) bool, gte func(f1, f2 os.FileInfo) bool) (chosen os.FileInfo, err error) {
	files, err := ioutil.ReadDir(logdir)
	if err != nil {
		return nil, err
	}
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		if condition == nil || !condition(fi) {
			continue
		}
		if chosen == nil || gte(fi, chosen) {
			chosen = fi
		}
	}
	if chosen == nil {
		return nil, os.ErrNotExist
	}
	return
}

// getMinFile 于getMaxFile 相反，返回最小的文件
func getMinFile(logdir string, condition func(os.FileInfo) bool, gte func(f1, f2 os.FileInfo) bool) (os.FileInfo, error) {
	return getMaxFile(logdir, condition, func(f1, f2 os.FileInfo) bool {
		return !gte(f1, f2)
	})
}

// noCondition 无限制条件
func noCondition(f os.FileInfo) bool {
	return true
}

func andCondition(f1, f2 func(os.FileInfo) bool) func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {
		return f1(fi) && f2(fi)
	}
}

func orCondition(f1, f2 func(os.FileInfo) bool) func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {
		return f1(fi) || f2(fi)
	}
}

func notCondition(f1 func(os.FileInfo) bool) func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {
		return !f1(fi)
	}
}

// modTimeLater 按最后修改时间进行比较
func modTimeLater(f1, f2 os.FileInfo) bool {
	return f1.ModTime().Unix() >= f2.ModTime().Unix()
}

func HeadPatternMode(mode string, v interface{}) (reg *regexp.Regexp, err error) {
	switch mode {
	case ReadModeHeadPatternString:
		pattern, ok := v.(string)
		if !ok {
			err = fmt.Errorf(" %v is not pattern string", v)
			return
		}
		reg, err = regexp.Compile(pattern)
		if err != nil {
			err = fmt.Errorf("pattern %v compile error %v ", v, err)
			return
		}
		return
	case ReadModeHeadPatternRegexp:
		reg1, ok := v.(*regexp.Regexp)
		if !ok {
			err = fmt.Errorf(" %v is not *regexp.Regexp type value", v)
		}
		reg = reg1
		return
	default:
		err = fmt.Errorf("unknown HeadPatternMode %v", mode)
		return
	}
}
