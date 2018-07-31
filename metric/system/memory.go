package system

import (
	"fmt"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	TypeMetricMem   = "mem"
	MetricMemUsages = "内存(mem)"

	// TypeMetricMem 信息中的字段
	KeyMemTotal            = "mem_total"
	KeyMemAvailable        = "mem_available"
	KeyMemUsed             = "mem_used"
	KeyMemFree             = "mem_free"
	KeyMemCached           = "mem_cached"
	KeyMemBuffered         = "mem_buffered"
	KeyMemActive           = "mem_active"
	KeyMemInactive         = "mem_inactive"
	KeyMemUsedPercent      = "mem_used_percent"
	KeyMemAvailablePercent = "mem_available_percent"
)

// KeyMemUsages TypeMetricMem 中的字段名称
var KeyMemUsages = KeyValueSlice{
	{KeyMemTotal, "内存总数", ""},
	{KeyMemAvailable, "可用内存数", ""},
	{KeyMemUsed, "已用内存数", ""},
	{KeyMemFree, "空闲内存", ""},
	{KeyMemCached, "用于缓存的内存", ""},
	{KeyMemBuffered, "文件buffer内存", ""},
	{KeyMemActive, "活跃使用的内存总数(包括cache和buffer内存)", ""},
	{KeyMemInactive, "空闲的内存数(包括free和avalible的内存)", ""},
	{KeyMemUsedPercent, "内存已用百分比(0~100)", ""},
	{KeyMemAvailablePercent, "内存剩余百分比(0~100)", ""},
}

type MemStats struct {
	ps PS
}

func (_ *MemStats) Name() string {
	return TypeMetricMem
}

func (_ *MemStats) Usages() string {
	return MetricMemUsages
}

func (_ *MemStats) Tags() []string {
	return []string{}
}

func (_ *MemStats) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: KeyMemUsages,
	}
	return config
}

func (s *MemStats) Collect() (datas []map[string]interface{}, err error) {
	vm, err := s.ps.VMStat()
	if err != nil {
		return nil, fmt.Errorf("error getting virtual memory info: %s", err)
	}

	fields := map[string]interface{}{
		KeyMemTotal:            vm.Total,
		KeyMemAvailable:        vm.Available,
		KeyMemUsed:             vm.Used,
		KeyMemFree:             vm.Free,
		KeyMemCached:           vm.Cached,
		KeyMemBuffered:         vm.Buffers,
		KeyMemActive:           vm.Active,
		KeyMemInactive:         vm.Inactive,
		KeyMemUsedPercent:      100 * float64(vm.Used) / float64(vm.Total),
		KeyMemAvailablePercent: 100 * float64(vm.Available) / float64(vm.Total),
	}
	datas = append(datas, fields)
	return
}

const (
	TypeMetricSwap   = "swap"
	MetricSwapUsages = "内存(Swap)"

	// TypeMetricSwap 中的字段
	KeySwapTotal       = "swap_total"
	KeySwapUsed        = "swap_used"
	KeySwapFree        = "swap_free"
	KeySwapIn          = "swap_in"
	KeySwapOut         = "swap_out"
	KeySwapUsedPercent = "swap_used_percent"
)

// KeySwapUsages TypeMetricSwap 中的字段名称
var KeySwapUsages = KeyValueSlice{
	{KeySwapTotal, "Swap空间总量", ""},
	{KeySwapUsed, "Swap已使用空间", ""},
	{KeySwapFree, "Swap空闲空间", ""},
	{KeySwapUsedPercent, "Swap使用空间占比", ""},
	{KeySwapIn, "Swap空间换入数据量", ""},
	{KeySwapOut, "Swap空间换出数据量", ""},
}

type SwapStats struct {
	ps PS
}

func (_ *SwapStats) Name() string {
	return TypeMetricSwap
}

func (_ *SwapStats) Usages() string {
	return MetricSwapUsages
}

func (_ *SwapStats) Tags() []string {
	return []string{}
}

func (_ *SwapStats) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: KeySwapUsages,
	}
	return config
}

func (s *SwapStats) Collect() (datas []map[string]interface{}, err error) {
	swap, err := s.ps.SwapStat()
	if err != nil {
		return nil, fmt.Errorf("error getting swap memory info: %s", err)
	}

	fieldsG := map[string]interface{}{
		KeySwapIn:          swap.Sin,
		KeySwapOut:         swap.Sout,
		KeySwapTotal:       swap.Total,
		KeySwapUsed:        swap.Used,
		KeySwapFree:        swap.Free,
		KeySwapUsedPercent: swap.UsedPercent,
	}
	datas = append(datas, fieldsG)
	return
}

func init() {
	ps := newSystemPS()
	metric.Add(TypeMetricMem, func() metric.Collector {
		return &MemStats{ps: ps}
	})

	metric.Add(TypeMetricSwap, func() metric.Collector {
		return &SwapStats{ps: ps}
	})
}
