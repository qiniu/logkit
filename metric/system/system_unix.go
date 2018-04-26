// +build !windows

package system

import (
	"runtime"

	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"

	"github.com/qiniu/logkit/metric"
)

type UnixSystemStats struct {
	SystemStats
}

func (s *UnixSystemStats) Collect() (datas []map[string]interface{}, err error) {
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
	}
	datas = append(datas, data)
	return
}

func init() {
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &UnixSystemStats{}
	})
}
