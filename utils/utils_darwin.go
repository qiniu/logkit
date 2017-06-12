// +build darwin

package utils

import (
	"os"
	"syscall"
)

// GetInode 获得文件inode
func GetInode(f os.FileInfo) uint64 {
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
