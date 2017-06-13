package system

import (
	"fmt"

	"github.com/qiniu/logkit/metric"
)

type MemStats struct {
	ps PS
}

func (_ *MemStats) Name() string {
	return "mem"
}

func (s *MemStats) Collect() (datas []map[string]interface{}, err error) {
	vm, err := s.ps.VMStat()
	if err != nil {
		return nil, fmt.Errorf("error getting virtual memory info: %s", err)
	}

	fields := map[string]interface{}{
		"total":             vm.Total,
		"available":         vm.Available,
		"used":              vm.Used,
		"free":              vm.Free,
		"cached":            vm.Cached,
		"buffered":          vm.Buffers,
		"active":            vm.Active,
		"inactive":          vm.Inactive,
		"used_percent":      100 * float64(vm.Used) / float64(vm.Total),
		"available_percent": 100 * float64(vm.Available) / float64(vm.Total),
	}
	datas = append(datas, fields)
	return
}

type SwapStats struct {
	ps PS
}

func (_ *SwapStats) Name() string {
	return "swap"
}

func (s *SwapStats) Collect() (datas []map[string]interface{}, err error) {
	swap, err := s.ps.SwapStat()
	if err != nil {
		return nil, fmt.Errorf("error getting swap memory info: %s", err)
	}

	fieldsG := map[string]interface{}{
		"total":        swap.Total,
		"used":         swap.Used,
		"free":         swap.Free,
		"used_percent": swap.UsedPercent,
	}
	datas = append(datas, fieldsG)
	fieldsC := map[string]interface{}{
		"in":  swap.Sin,
		"out": swap.Sout,
	}
	datas = append(datas, fieldsC)
	return
}

func init() {
	ps := newSystemPS()
	metric.Add("mem", func() metric.Collector {
		return &MemStats{ps: ps}
	})

	metric.Add("swap", func() metric.Collector {
		return &SwapStats{ps: ps}
	})
}
