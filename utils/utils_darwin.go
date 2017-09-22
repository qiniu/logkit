package utils

import (
	"bytes"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/qiniu/log"
)

func GetOSInfo() *OSInfo {
	out := _getInfo()
	tryTime := 0
	for strings.Index(out, "broken pipe") != -1 {
		if tryTime > 3 {
			break
		}
		out = _getInfo()
		time.Sleep(500 * time.Millisecond)
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
