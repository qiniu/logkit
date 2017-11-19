package system

import (
	"bufio"
	"bytes"
	"fmt"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/utils"
)

const (
	TypeMetricSystem  = "system"
	MetricSystemUsage = "系统概览(system)"

	// TypeMetricSystem 信息中的字段
	KeySystemLoad1        = "system_load1"
	KeySystemLoad5        = "system_load5"
	KeySystemLoad15       = "system_load15"
	KeySystemNUsers       = "system_n_users"
	KeySystemNCpus        = "system_n_cpus"
	KeySystemUptime       = "system_uptime"
	KeySystemUptimeFormat = "system_update_format"
)

// KeySystemUsages TypeMetricSystem的字段名称
var KeySystemUsages = []utils.KeyValue{
	{KeySystemLoad1, "1分钟平均load值"},
	{KeySystemLoad5, "5分钟平均load值"},
	{KeySystemLoad15, "15分钟平均load值"},
	{KeySystemNUsers, "用户数"},
	{KeySystemNCpus, "CPU核数"},
	{KeySystemUptime, "系统启动时间"},
	{KeySystemUptimeFormat, "格式化的系统启动时间"},
}

type SystemStats struct {
}

func (_ *SystemStats) Name() string {
	return TypeMetricSystem
}

func (_ *SystemStats) Usages() string {
	return MetricSystemUsage
}

func (_ *SystemStats) Tags() []string {
	return []string{}
}

func (s *SystemStats) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []utils.Option{},
		metric.AttributesString: KeySystemUsages,
	}
	return config
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
		KeySystemLoad1:        loadavg.Load1,
		KeySystemLoad5:        loadavg.Load5,
		KeySystemLoad15:       loadavg.Load15,
		KeySystemNUsers:       len(users),
		KeySystemNCpus:        runtime.NumCPU(),
		KeySystemUptime:       hostinfo.Uptime,
		KeySystemUptimeFormat: format_uptime(hostinfo.Uptime),
	}
	now := time.Now().Format(time.RFC3339Nano)
	data[TypeMetricSystem+"_"+metric.Timestamp] = now
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
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &SystemStats{}
	})
}
