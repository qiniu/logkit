// +build linux freebsd openbsd netbsd darwin solaris illumos dragonfly

package os

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
	"bytes"
	"runtime"

	"github.com/qiniu/log"
)

func GetOSInfo() *OSInfo {
	out := _getInfo()
	tryTime := 0
	for strings.Index(out, "broken pipe") != -1 {
		if tryTime > 3 {
			break
		}

		time.Sleep(500 * time.Millisecond)
		out = _getInfo()
		tryTime++
	}
	osStr := strings.Replace(out, "\n", "", -1)
	osStr = strings.Replace(osStr, "\r\n", "", -1)
	osInfo := strings.Split(osStr, " ")
	for i := len(osInfo); i < 3; i++ {
		osInfo = append(osInfo, "unknown")
	}
	gio := &OSInfo{Kernel: osInfo[0], Core: osInfo[1], Platform: runtime.GOARCH, OS: osInfo[0]}
	gio.Hostname, _ = os.Hostname()
	return gio
}

func _getInfo() string {
	cmd := exec.Command("uname", "-srm")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Error("getInfo:", err)
	}
	return out.String()
}

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
