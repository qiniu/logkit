package system

import (
	"fmt"

	"github.com/qiniu/logkit/metric"
	"github.com/shirou/gopsutil/cpu"
)

type CPUStats struct {
	ps        PS
	lastStats []cpu.TimesStat

	PerCPU         bool
	TotalCPU       bool
	CollectCPUTime bool
}

func NewCPUStats(ps PS) *CPUStats {
	return &CPUStats{
		ps:             ps,
		CollectCPUTime: true,
	}
}

func (_ *CPUStats) Name() string {
	return "cpu"
}

var sampleConfig = `
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
`

func (_ *CPUStats) SampleConfig() string {
	return sampleConfig
}

func (s *CPUStats) Collect() (datas []map[string]interface{}, err error) {
	times, err := s.ps.CPUTimes(s.PerCPU, s.TotalCPU)
	if err != nil {
		return nil, fmt.Errorf("error getting CPU info: %s", err)
	}

	for i, cts := range times {

		total := totalCpuTime(cts)

		if s.CollectCPUTime {
			// Add cpu time metrics
			fieldsC := map[string]interface{}{
				"time_user":       cts.User,
				"time_system":     cts.System,
				"time_idle":       cts.Idle,
				"time_nice":       cts.Nice,
				"time_iowait":     cts.Iowait,
				"time_irq":        cts.Irq,
				"time_softirq":    cts.Softirq,
				"time_steal":      cts.Steal,
				"time_guest":      cts.Guest,
				"time_guest_nice": cts.GuestNice,
				"cpu":             cts.CPU,
			}
			datas = append(datas, fieldsC)
		}

		// Add in percentage
		if len(s.lastStats) == 0 {
			// If it's the 1st gather, can't get CPU Usage stats yet
			continue
		}
		lastCts := s.lastStats[i]
		lastTotal := totalCpuTime(lastCts)
		totalDelta := total - lastTotal

		if totalDelta < 0 {
			s.lastStats = times
			return nil, fmt.Errorf("Error: current total CPU time is less than previous total CPU time")
		}

		if totalDelta == 0 {
			continue
		}
		fieldsG := map[string]interface{}{
			"usage_user":       100 * (cts.User - lastCts.User - (cts.Guest - lastCts.Guest)) / totalDelta,
			"usage_system":     100 * (cts.System - lastCts.System) / totalDelta,
			"usage_idle":       100 * (cts.Idle - lastCts.Idle) / totalDelta,
			"usage_nice":       100 * (cts.Nice - lastCts.Nice - (cts.GuestNice - lastCts.GuestNice)) / totalDelta,
			"usage_iowait":     100 * (cts.Iowait - lastCts.Iowait) / totalDelta,
			"usage_irq":        100 * (cts.Irq - lastCts.Irq) / totalDelta,
			"usage_softirq":    100 * (cts.Softirq - lastCts.Softirq) / totalDelta,
			"usage_steal":      100 * (cts.Steal - lastCts.Steal) / totalDelta,
			"usage_guest":      100 * (cts.Guest - lastCts.Guest) / totalDelta,
			"usage_guest_nice": 100 * (cts.GuestNice - lastCts.GuestNice) / totalDelta,
			"cpu":              cts.CPU,
		}
		datas = append(datas, fieldsG)
	}

	s.lastStats = times

	return
}

func totalCpuTime(t cpu.TimesStat) float64 {
	total := t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal +
		t.Idle
	return total
}

func init() {
	metric.Add("cpu", func() metric.Collector {
		return &CPUStats{
			PerCPU:   true,
			TotalCPU: true,
			ps:       newSystemPS(),
		}
	})
}
