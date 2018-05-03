// +build windows

package system

import (
	"fmt"
)

const (
	MetricCPUTotalKey  = "cpu-total"
	WindowsCPUTotalKey = "_Total"
)

func (s *CPUStats) Collect() (datas []map[string]interface{}, err error) {
	times, err := s.ps.CPUTimes(s.PerCPU, s.TotalCPU)
	if err != nil {
		return nil, fmt.Errorf("error getting CPU info: %s", err)
	}

	for _, cts := range times {

		// windows cpu time stat only for total
		if s.CollectCPUTime && isTotalCpuTimeStat(cts.CPU) {
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
			continue
		}
		// merge "_Total"  to "cpu_total"
		if isTotalCpuUsageStat(cts.CPU) {
			cts.CPU = MetricCPUTotalKey
		}
		fieldsG := map[string]interface{}{
			CpuUsageUser:      cts.User,
			CpuUsageSystem:    cts.System,
			CpuUsageIdle:      cts.Idle,
			CpuUsageNice:      cts.Nice,
			CpuUsageIowait:    cts.Iowait,
			CpuUsageIrq:       cts.Irq,
			CpuUsageSoftirq:   cts.Softirq,
			CpuUsageSteal:     cts.Steal,
			CpuUsageGuest:     cts.Guest,
			CpuUsageGuestNice: cts.GuestNice,
			CpuUsageCPU:       cts.CPU,
		}
		datas = append(datas, fieldsG)
	}

	return
}

func isTotalCpuTimeStat(name string) bool {
	return name == MetricCPUTotalKey
}
func isTotalCpuUsageStat(name string) bool {
	return name == WindowsCPUTotalKey
}
