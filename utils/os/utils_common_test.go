package os

import (
	"fmt"
	"os"
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestOSInfo_String(t *testing.T) {
	osInfo := OSInfo{}
	assert.EqualValues(t, "; ; ;  ", osInfo.String())

	osInfo = OSInfo{
		Kernel:   "Darwin",
		Core:     "17.4.0",
		Platform: "Darwin",
		OS:       "Darwin",
		Hostname: "tst",
	}
	assert.EqualValues(t, "tst; Darwin; 17.4.0; Darwin Darwin", osInfo.String())
}

func TestGetLocalIp(t *testing.T) {
	ip, err := GetLocalIP()
	assert.NoError(t, err)
	fmt.Println(ip)
}

func TestGetExtraInfo(t *testing.T) {
	extraInfo := GetExtraInfo()
	osInfo := GetOSInfo()
	ip, err := GetLocalIP()

	if core, ok := extraInfo[KeyCore]; !ok {
		t.Fatalf("core is not found")
	} else {
		assert.Equal(t, osInfo.Core, core)
	}

	if hostname, ok := extraInfo[KeyHostName]; !ok {
		t.Fatalf("hostname is not found")
	} else {
		assert.Equal(t, osInfo.Hostname, hostname)
	}

	if oi, ok := extraInfo[KeyOsInfo]; !ok {
		t.Fatalf("osInfo is not found")
	} else {
		assert.Equal(t, osInfo.OS+"-"+osInfo.Kernel+"-"+osInfo.Platform, oi)
	}

	if localIp, ok := extraInfo[KeyLocalIp]; err == nil && !ok {
		t.Fatalf("local ip is not found")
	} else if err == nil {
		assert.Equal(t, ip, localIp)
	}
}

func Test_GetInode(t *testing.T) {
	os.Mkdir("abc", 0777)
	fi, _ := os.Stat("abc")
	inode := getInode(fi)
	assert.True(t, inode > 0)
	os.RemoveAll("abc")
}
