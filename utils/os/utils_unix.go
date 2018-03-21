// +build darwin linux

package os

import (
	"fmt"
	"os"
	"syscall"
)

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

func GetIdentifyIDByPath(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		err = fmt.Errorf("os.Stat(%v) error %v", path, err)
		return 0, err
	}
	inode := getInode(fi)
	return inode, nil
}

func GetIdentifyIDByFile(f *os.File) (uint64, error) {
	finfo, err := f.Stat()
	if err != nil {
		return 0, err
	}
	inode := getInode(finfo)
	return inode, nil
}
