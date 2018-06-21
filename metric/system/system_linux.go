// +build linux

package system

import (
	"runtime"
	"os/exec"
	"strings"

	"github.com/qiniu/log"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/mgr"
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
	datas = append(datas, data)
	return
}

func init() {
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &LinuxSystemStats{}
	})
}

//若无法获取磁盘个数，返回挂载点的个数
func getNumDisk() int {
	diskMetrics, err := mgr.NewMetric("disk")
	mounts, err := diskMetrics.Collect()
	mountsNum := len(mounts)
	fdisk, err := exec.LookPath("/sbin/fdisk")
	if err != nil {
		log.Errorf(err.Error())
		return mountsNum
	}
	out, err := exec.Command(fdisk, "-l").Output()
	str := string(out)
	index := strings.Index(str, "Device     Boot Start      End  Sectors Size Id Type")
	disks := strings.Split(str[index:], "\n")
	return len(disks) - 2
}

func getNumService() int {
	out, err := exec.Command("which", "supervisorctl").Output()
	if err != nil {
		log.Errorf(err.Error())
		return -1
	}
	out, err = exec.Command("supervisorctl", "status").Output()
	if err != nil {
		log.Errorf(err.Error())
		return -1
	}
	count := len(strings.Split(string(out), "\n"))
	return count - 1
}
