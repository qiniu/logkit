package system

import (
	"fmt"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/utils"
	"github.com/shirou/gopsutil/cpu"
)

const (
	TypeMetricCpu   = "cpu"
	MetricCpuUsages = "CPU(Cpu)"

	// TypeMetricCpu 信息中的字段
	// CPU 总量
	CpuTimeUser      = "cpu_time_user"
	CpuTimeSystem    = "cpu_time_system"
	CpuTimeIdle      = "cpu_time_idle"
	CpuTimeNice      = "cpu_time_nice"
	CpuTimeIowait    = "cpu_time_iowait"
	CpuTimeIrq       = "cpu_time_irq"
	CpuTimeSoftirq   = "cpu_time_softirq"
	CpuTimeSteal     = "cpu_time_steal"
	CpuTimeGuest     = "cpu_time_guest"
	CpuTimeGuestNice = "cpu_time_guest_nice"
	CpuTimeCPU       = "cpu_time_cpu"

	// CPU 用量
	CpuUsageUser      = "cpu_usage_user"
	CpuUsageSystem    = "cpu_usage_system"
	CpuUsageIdle      = "cpu_usage_idle"
	CpuUsageNice      = "cpu_usage_nice"
	CpuUsageIowait    = "cpu_usage_iowait"
	CpuUsageIrq       = "cpu_usage_irq"
	CpuUsageSoftirq   = "cpu_usage_softirq"
	CpuUsageSteal     = "cpu_usage_steal"
	CpuUsageGuest     = "cpu_usage_guest"
	CpuUsageGuestNice = "cpu_usage_guest_nice"
	CpuUsageCPU       = "cpu_usage_cpu"
)

// KeyCpuUsages TypeMetricCpu 中的字段名称
var KeyCpuUsages = []utils.KeyValue{
	// CPU 总量
	{CpuTimeUser, "用户进程用时"},
	{CpuTimeSystem, "系统内核用时"},
	{CpuTimeIdle, "空闲CPU用时"},
	{CpuTimeNice, "优先级调度用时"},
	{CpuTimeIowait, "iowait用时"},
	{CpuTimeIrq, "中断用时"},
	{CpuTimeSoftirq, "软中断用时"},
	{CpuTimeSteal, "Steal用时"},
	{CpuTimeGuest, "Guest用时"},
	{CpuTimeGuestNice, "GuestNice用时"},
	{CpuTimeCPU, "CPU序号名称"},

	// CPU 用量
	{CpuUsageUser, "用户用量百分比(0~100)"},
	{CpuUsageSystem, "系统用量百分比(0~100)"},
	{CpuUsageIdle, "空闲百分比(0~100)"},
	{CpuUsageNice, "优先级调度程序用量百分比(0~100)"},
	{CpuUsageIowait, "IOwait时间占比(0~100)"},
	{CpuUsageIrq, "中断时间占比(0~100)"},
	{CpuUsageSoftirq, "软中断时间占比(0~100)"},
	{CpuUsageSteal, "虚拟CPU的竞争等待时间占比(0~100)"},
	{CpuUsageGuest, "虚拟进程的CPU用时占比(0~100)"},
	{CpuUsageGuestNice, "虚拟进程CPU调度用时占比(0~100)"},
	{CpuUsageCPU, "CPU序号名称"},
}

type CPUStats struct {
	ps        PS
	lastStats map[string]cpu.TimesStat

	PerCPU         bool `json:"percpu"`
	TotalCPU       bool `json:"totalcpu"`
	CollectCPUTime bool `json:"collect_cpu_time"`
}

func (_ *CPUStats) Name() string {
	return TypeMetricCpu
}

var sampleConfig = `{
  "percpu": true,
  "totalcpu" : true,
  "collect_cpu_time": false
}`

func (_ *CPUStats) Usages() string {
	return MetricCpuUsages
}

func (_ *CPUStats) Config() []utils.Option {
	return []utils.Option{}
}

func (_ *CPUStats) Attributes() []utils.KeyValue {
	return KeyCpuUsages
}

func (_ *CPUStats) SampleConfig() string {
	return sampleConfig
}

func (s *CPUStats) Collect() (datas []map[string]interface{}, err error) {
	times, err := s.ps.CPUTimes(s.PerCPU, s.TotalCPU)
	if err != nil {
		return nil, fmt.Errorf("error getting CPU info: %s", err)
	}

	for _, cts := range times {

		total := totalCpuTime(cts)

		if s.CollectCPUTime {
			// Add cpu time metrics
			fieldsC := map[string]interface{}{
				CpuTimeUser:      cts.User,
				CpuTimeSystem:    cts.System,
				CpuTimeIdle:      cts.Idle,
				CpuTimeNice:      cts.Nice,
				CpuTimeIowait:    cts.Iowait,
				CpuTimeIrq:       cts.Irq,
				CpuTimeSoftirq:   cts.Softirq,
				CpuTimeSteal:     cts.Steal,
				CpuTimeGuest:     cts.Guest,
				CpuTimeGuestNice: cts.GuestNice,
				CpuTimeCPU:       cts.CPU,
			}
			datas = append(datas, fieldsC)
		}

		// Add in percentage
		if len(s.lastStats) == 0 {
			// If it's the 1st gather, can't get CPU Usage stats yet
			continue
		}
		lastCts, ok := s.lastStats[cts.CPU]
		if !ok {
			continue
		}
		lastTotal := totalCpuTime(lastCts)
		totalDelta := total - lastTotal

		if totalDelta < 0 {
			return nil, fmt.Errorf("Error: current total CPU time is less than previous total CPU time")
		}

		if totalDelta == 0 {
			continue
		}
		fieldsG := map[string]interface{}{
			CpuUsageUser:      100 * (cts.User - lastCts.User - (cts.Guest - lastCts.Guest)) / totalDelta,
			CpuUsageSystem:    100 * (cts.System - lastCts.System) / totalDelta,
			CpuUsageIdle:      100 * (cts.Idle - lastCts.Idle) / totalDelta,
			CpuUsageNice:      100 * (cts.Nice - lastCts.Nice - (cts.GuestNice - lastCts.GuestNice)) / totalDelta,
			CpuUsageIowait:    100 * (cts.Iowait - lastCts.Iowait) / totalDelta,
			CpuUsageIrq:       100 * (cts.Irq - lastCts.Irq) / totalDelta,
			CpuUsageSoftirq:   100 * (cts.Softirq - lastCts.Softirq) / totalDelta,
			CpuUsageSteal:     100 * (cts.Steal - lastCts.Steal) / totalDelta,
			CpuUsageGuest:     100 * (cts.Guest - lastCts.Guest) / totalDelta,
			CpuUsageGuestNice: 100 * (cts.GuestNice - lastCts.GuestNice) / totalDelta,
			CpuUsageCPU:       cts.CPU,
		}
		datas = append(datas, fieldsG)
	}

	s.lastStats = make(map[string]cpu.TimesStat)
	for _, cts := range times {
		s.lastStats[cts.CPU] = cts
	}

	return
}

func totalCpuTime(t cpu.TimesStat) float64 {
	total := t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal +
		t.Idle
	return total
}

func init() {
	metric.Add(TypeMetricCpu, func() metric.Collector {
		return &CPUStats{
			PerCPU:   true,
			TotalCPU: true,
			ps:       newSystemPS(),
		}
	})
}
