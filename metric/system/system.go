package system

import (
	"bufio"
	"bytes"
	"fmt"
	"runtime"

	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"

	"github.com/qiniu/logkit/metric"
)

type SystemStats struct{}

func (_ *SystemStats) Name() string {
	return "system"
}

func (_ *SystemStats) Collect() (datas []map[string]interface{}, err error) {
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
		"load1":         loadavg.Load1,
		"load5":         loadavg.Load5,
		"load15":        loadavg.Load15,
		"n_users":       len(users),
		"n_cpus":        runtime.NumCPU(),
		"uptime":        hostinfo.Uptime,
		"uptime_format": format_uptime(hostinfo.Uptime),
	}
	datas = append(datas, data)
	return
}

func format_uptime(uptime uint64) string {
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	days := uptime / (60 * 60 * 24)

	if days != 0 {
		s := ""
		if days > 1 {
			s = "s"
		}
		fmt.Fprintf(w, "%d day%s, ", days, s)
	}

	minutes := uptime / 60
	hours := minutes / 60
	hours %= 24
	minutes %= 60

	fmt.Fprintf(w, "%2d:%02d", hours, minutes)

	w.Flush()
	return buf.String()
}

func init() {
	metric.Add("system", func() metric.Collector {
		return &SystemStats{}
	})
}
