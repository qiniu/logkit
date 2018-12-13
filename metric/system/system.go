package system

import (
	"bufio"
	"bytes"
	"fmt"
	"net"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
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
	KeySystemUptimeFormat = "system_uptime_format"
	KeySystemNNetCards    = "system_n_net_cards"
	KeySystemNDisks       = "system_n_disks"
	KeySystemNServices    = "system_n_services"
)

// KeySystemUsages TypeMetricSystem的字段名称
var KeySystemUsages = KeyValueSlice{
	{KeySystemLoad1, "1分钟平均load值", ""},
	{KeySystemLoad5, "5分钟平均load值", ""},
	{KeySystemLoad15, "15分钟平均load值", ""},
	{KeySystemNUsers, "用户数", ""},
	{KeySystemNCpus, "CPU核数", ""},
	{KeySystemUptime, "系统启动时间", ""},
	{KeySystemUptimeFormat, "格式化的系统启动时间", ""},
	{KeySystemNNetCards, "网卡数", ""},
	{KeySystemNDisks, "磁盘数", ""},
	{KeySystemNServices, "总服务数", ""},
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
		metric.OptionString:     []Option{},
		metric.AttributesString: KeySystemUsages,
	}
	return config
}

func (s *SystemStats) Collect() (datas []map[string]interface{}, err error) {
	return
}

func formatUptime(uptime uint64) string {
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

func getNumNetCard() int {
	var macAddress []string
	interfaces, err := net.Interfaces()
	if err != nil {
		log.Errorf("Can not get local interface info:%s", err)
		return 0
	}
	for _, inter := range interfaces {
		if (inter.Flags&net.FlagUp) > 0 && (inter.Flags&net.FlagBroadcast) > 0 {
			macAddress = append(macAddress, inter.HardwareAddr.String())
		}
	}
	return len(macAddress)
}
