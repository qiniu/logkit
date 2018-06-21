// +build darwin

package system

import (
	"runtime"
	"os/exec"
	"strings"
	"strconv"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/mgr"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/qiniu/logkit/metric"
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
	datas = append(datas, data)
	return
}

func init() {
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &DarwinSystemStats{}
	})
}

//若无法获取磁盘个数，返回挂载点的个数
func getNumDisk() int {
	diskMetrics, err := mgr.NewMetric("disk")
	mounts, err := diskMetrics.Collect()
	mountsNum := len(mounts)
	diskutil, err := exec.LookPath("/usr/sbin/diskutil")
	if err != nil {
		log.Errorf(err.Error())
		return mountsNum
	}
	out, err := exec.Command(diskutil, "apfs", "list").Output()
	if err != nil {
		log.Errorf(err.Error())
		return mountsNum
	}
	res := string(out)
	index := strings.Index(res, "APFS Container")
	if index < 0 || index+18 > len(res) {
		return mountsNum
	}
	num, err := strconv.Atoi(res[index+16 : index+17])
	if err != nil {
		log.Errorf(err.Error())
		return mountsNum
	}
	return num
}

func getNumService() int {
	return -1
}
