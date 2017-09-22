// +build windows
package utils

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	"github.com/qiniu/log"
)

func GetIdentifyIDByPath(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return GetIdentifyIDByFile(f)
}

func GetIdentifyIDByFile(f *os.File) (uint64, error) {
	var d syscall.ByHandleFileInformation

	if err := syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &d); err != nil {
		err = fmt.Errorf(" syscall.GetFileInformationByHandle error %v", err)
		return 0, err
	}
	inode := uint64(d.FileIndexHigh)
	inode <<= 32
	inode += uint64(d.FileIndexLow)
	return inode, nil
}

func GetOSInfo() *OSInfo {
	cmd := exec.Command("cmd", "ver")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Error(err)
		return &OSInfo{Kernel: "windows", Core: "unknown", Platform: runtime.GOARCH, OS: "windows"}
	}
	osStr := strings.Replace(out.String(), "\n", "", -1)
	osStr = strings.Replace(osStr, "\r\n", "", -1)
	tmp1 := strings.Index(osStr, "[Version")
	tmp2 := strings.Index(osStr, "]")
	var ver string
	if tmp1 == -1 || tmp2 == -1 || tmp2 <= tmp1 || tmp2 > len(osStr) {
		ver = "unknown"
	} else {
		ver = osStr[tmp1+9 : tmp2]
	}
	gio := &OSInfo{Kernel: "windows", Core: ver, Platform: runtime.GOARCH, OS: "windows"}
	gio.Hostname, _ = os.Hostname()
	return gio
}
