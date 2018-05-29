// +build !windows

package system

import (
	"fmt"

	"github.com/shirou/gopsutil/cpu"
)

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
			return nil, fmt.Errorf("error: current total CPU time is %v less than previous total CPU time %v", total, lastTotal)
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
