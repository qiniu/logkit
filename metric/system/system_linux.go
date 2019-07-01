// +build linux

package system

import (
	"os/exec"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/metric"
)

type LinuxSystemStats struct {
	SystemStats
}

func (s *LinuxSystemStats) Collect() (datas []map[string]interface{}, err error) {
	loadavg, err := load.Avg()
	if err != nil {
		return
	}

	hostinfo, err := host.Info()
	if err != nil {
		return
	}

	users, err := host.Users()
	if err != nil {
		return
	}

	data := map[string]interface{}{
		KeySystemLoad1:        loadavg.Load1,
		KeySystemLoad5:        loadavg.Load5,
		KeySystemLoad15:       loadavg.Load15,
		KeySystemNUsers:       len(users),
		KeySystemNCpus:        runtime.NumCPU(),
		KeySystemUptime:       hostinfo.Uptime,
		KeySystemUptimeFormat: formatUptime(hostinfo.Uptime),
		KeySystemNNetCards:    getNumNetCard(),
		KeySystemNDisks:       getNumDisk(),
		KeySystemNServices:    getNumService(),
	}
	datas = []map[string]interface{}{data}
	return
}

func init() {
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &LinuxSystemStats{}
	})
}

func getNumDisk() (mountsNum int) {
	fDisk, err := exec.LookPath("/sbin/fdisk")
	if err != nil {
		log.Debug("can't find /sbin/fdisk on your system PATH, will not collect")
		return 0
	}
	out, err := exec.Command(fDisk, "-l").Output()
	if err != nil {
		log.Errorf("exec command /sbin/fdisk -l failed: %v", err)
		return 0
	}
	str := string(out)
	sps := strings.Split(str, "\n")
	mountsNum = 0
	for _, v := range sps {
		if strings.Contains(v, "Device") && strings.Contains(v, "Boot") && strings.Contains(v, "Start") && strings.Contains(v, "End") {
			mountsNum++
		}
	}
	return mountsNum
}

func getNumService() int {
	out, err := exec.Command("which", "supervisorctl").Output()
	if err != nil {
		log.Debug("can't find supervisorctl on your system, will not collect service number")
		return 0
	}
	out, err = exec.Command("supervisorctl", "status").Output()
	if err != nil {
		log.Debugf("get service number from supervisor error %v", err)
		return 0
	}
	//去掉首行
	count := len(strings.Split(string(out), "\n"))
	return count - 1
}
