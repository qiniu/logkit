// +build windows

package os

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
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
	// default osInfo
	osInfo := &OSInfo{Kernel: "windows", Core: "unknown", Platform: runtime.GOARCH, OS: "windows"}
	osInfo.Hostname, _ = os.Hostname()
	// call cmd "ver"
	cmd := exec.Command("cmd", "ver")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Error(err)
		return osInfo
	}
	osStr := strings.Replace(out.String(), "\n", "", -1)
	osStr = strings.Replace(osStr, "\r\n", "", -1)
	verReg := regexp.MustCompile(`([1-9]\d*\.)(\d*\.){1,2}\d*`) // e.g. 6.3.9600
	osInfo.Core = verReg.FindString(osStr)
	if len(osInfo.Core) == 0 {
		osInfo.Core = "unknown"
	}
	return osInfo
}
