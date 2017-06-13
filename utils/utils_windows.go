// +build windows
package utils

import (
	"os"

	"github.com/qiniu/log"
)

// GetInode 获得文件inode
func GetInode(f os.FileInfo) uint64 {
	log.Errorf("!!Error: get inode is not supported in windows, %v", f.Name())
	return 0
}
