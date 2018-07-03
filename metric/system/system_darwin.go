// +build darwin

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

type DarwinSystemStats struct {
	SystemStats
}

func (s *DarwinSystemStats) Collect() (datas []map[string]interface{}, err error) {
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
		return &DarwinSystemStats{}
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
	diskUtil, err := exec.LookPath("/usr/sbin/diskutil")
	if err != nil {
		log.Errorf("find diskutil have error %v", err)
		return -1
	}
	out, err := exec.Command(diskUtil, "list").Output()
	if err != nil {
		log.Errorf("get disk number have error %v", err)
		return -1
	}
	return getNumFromOutput(string(out))
}

func getNumFromOutput(out string) int {
	num := 0
	prints := strings.Split(out, "\n")
	for _, v := range prints {
		if strings.Contains(v, "/dev/disk") {
			num++
		}
	}
	return num
}

func getNumService() int {
	return 0
}
