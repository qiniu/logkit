package os

import (
	"errors"
	"fmt"
	"net"

	. "github.com/qiniu/logkit/utils/models"
)

type OSInfo struct {
	Kernel   string
	Core     string
	Platform string
	OS       string
	Hostname string
}

func (oi *OSInfo) String() string {
	return fmt.Sprintf("%s; %s; %s; %s %s", oi.Hostname, oi.OS, oi.Core, oi.Kernel, oi.Platform)
}

func GetExtraInfo() map[string]string {
	osInfo := GetOSInfo()
	exInfo := make(map[string]string)
	exInfo[KeyCore] = osInfo.Core
	exInfo[KeyHostName] = osInfo.Hostname
	exInfo[KeyOsInfo] = osInfo.OS + "-" + osInfo.Kernel + "-" + osInfo.Platform
	if ip, err := GetLocalIP(); err == nil {
		exInfo[KeyLocalIp] = ip
	}
	return exInfo
}

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1", fmt.Errorf("Get local IP error: %v\n", err)
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "127.0.0.1", errors.New("no local IP found")
}
