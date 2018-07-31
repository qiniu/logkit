package system

import (
	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"

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

	// Config 中的字段
	CpuConfigPerCpu         = "per_cpu"
	CpuConfigTotalCpu       = "total_cpu"
	CpuConfigCollectCpuTime = "collect_cpu_time"
)

// KeyCpuUsages TypeMetricCpu 中的字段名称
var KeyCpuUsages = KeyValueSlice{
	// CPU 总量
	{CpuTimeUser, "用户进程用时", ""},
	{CpuTimeSystem, "系统内核用时", ""},
	{CpuTimeIdle, "空闲CPU用时", ""},
	{CpuTimeNice, "优先级调度用时", ""},
	{CpuTimeIowait, "iowait用时", ""},
	{CpuTimeIrq, "中断用时", ""},
	{CpuTimeSoftirq, "软中断用时", ""},
	{CpuTimeSteal, "Steal用时", ""},
	{CpuTimeGuest, "Guest用时", ""},
	{CpuTimeGuestNice, "GuestNice用时", ""},
	{CpuTimeCPU, "CPU序号名称", ""},

	// CPU 用量
	{CpuUsageUser, "用户用量百分比(0~100)", ""},
	{CpuUsageSystem, "系统用量百分比(0~100)", ""},
	{CpuUsageIdle, "空闲百分比(0~100)", ""},
	{CpuUsageNice, "优先级调度程序用量百分比(0~100)", ""},
	{CpuUsageIowait, "IOwait时间占比(0~100)", ""},
	{CpuUsageIrq, "中断时间占比(0~100)", ""},
	{CpuUsageSoftirq, "软中断时间占比(0~100)", ""},
	{CpuUsageSteal, "虚拟CPU的竞争等待时间占比(0~100)", ""},
	{CpuUsageGuest, "虚拟进程的CPU用时占比(0~100)", ""},
	{CpuUsageGuestNice, "虚拟进程CPU调度用时占比(0~100)", ""},
	{CpuUsageCPU, "CPU序号名称", ""},
}

// ConfigCpuUsages TypeMetricCpu 配置项的描述
var ConfigCpuUsages = KeyValueSlice{
	{CpuConfigPerCpu, "是否收集每个 cpu 的统计数据(" + CpuConfigPerCpu + ")", ""},
	{CpuConfigTotalCpu, "是否收集整个系统的 cpu 统计信息(" + CpuConfigTotalCpu + ")", ""},
	{CpuConfigCollectCpuTime, "是否收集 cpu 时间统计信息(" + CpuConfigCollectCpuTime + ")", ""},
}

type CPUStats struct {
	ps        PS
	lastStats map[string]cpu.TimesStat

	PerCPU         bool `json:"per_cpu"`
	TotalCPU       bool `json:"total_cpu"`
	CollectCPUTime bool `json:"collect_cpu_time"`
}

func (_ *CPUStats) Name() string {
	return TypeMetricCpu
}

func (_ *CPUStats) Usages() string {
	return MetricCpuUsages
}

func (_ *CPUStats) Tags() []string {
	return []string{CpuTimeCPU, CpuUsageCPU}
}

func (_ *CPUStats) Config() map[string]interface{} {
	cpuConfig := make([]Option, 0)
	for _, val := range ConfigCpuUsages {
		opt := Option{
			KeyName:       val.Key,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   val.Value,
			Type:          metric.ConfigTypeBool,
		}
		cpuConfig = append(cpuConfig, opt)
	}
	config := map[string]interface{}{
		metric.OptionString:     cpuConfig,
		metric.AttributesString: KeyCpuUsages,
	}
	return config
}

func init() {
	metric.Add(TypeMetricCpu, func() metric.Collector {
		return &CPUStats{
			PerCPU:         true,
			TotalCPU:       true,
			CollectCPUTime: true,
			ps:             newSystemPS(),
		}
	})
}
