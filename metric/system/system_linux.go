// +build linux

package system

import (
	"os/exec"
	"runtime"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/metric"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
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

//若无法获取磁盘个数，返回挂载点的个数
func getNumDisk() (mountsNum int) {
	defer func() {
		if mountsNum == -1 {
			diskMetrics, ok := metric.Collectors["disk"]
			if !ok {
				log.Errorf("metric disk is not support now")
			}
			mounts, err := diskMetrics().Collect()
			if err != nil {
				log.Error("disk metrics collect have error %v", err)
			}
			mountsNum = len(mounts)
		}
	}()
	fDisk, err := exec.LookPath("/sbin/fdisk")
	if err != nil {
		log.Error("get disk num look /sbin/fdisk have error %v", err)
		return -1
	}
	out, err := exec.Command(fDisk, "-l").Output()
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
		log.Errorf("get service number have error %v", err)
		return 0
	}
	out, err = exec.Command("supervisorctl", "status").Output()
	if err != nil {
		log.Errorf("get service number have error %v", err)
		return 0
	}
	count := len(strings.Split(string(out), "\n"))
	return count - 1
}
