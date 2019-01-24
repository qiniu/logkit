// +build windows

// 扩展github.com/shirou/gopsutil，避免修改vendor
package utils

import (
	"context"
	"unsafe"

	"github.com/StackExchange/wmi"
	"github.com/shirou/gopsutil/cpu"
	"golang.org/x/sys/windows"
)

var (
	Modkernel32 = windows.NewLazyDLL("kernel32.dll")

	ProcGetSystemTimes = Modkernel32.NewProc("GetSystemTimes")
)

type FILETIME struct {
	DwLowDateTime  uint32
	DwHighDateTime uint32
}

// 兼容windows 2008之前版本，
// 修改Win32_PerfFormattedData_Counters_ProcessorInformation为Win32_PerfFormattedData_PerfOS_Processor
// Win32_PerfFormattedData_Counters_ProcessorInformation stores instance value of the perf counters
type Win32_PerfFormattedData_PerfOS_Processor struct {
	Name                  string
	PercentDPCTime        uint64
	PercentIdleTime       uint64
	PercentUserTime       uint64
	PercentProcessorTime  uint64
	PercentInterruptTime  uint64
	PercentPrivilegedTime uint64
	InterruptsPerSec      uint32
	DPCRate               uint32
}

// Overide Times in github.com/shirou/gopsutil/cpu/cpu_windows
func Times(percpu bool) ([]cpu.TimesStat, error) {
	return TimesWithContext(context.Background(), percpu)
}

func TimesWithContext(ctx context.Context, percpu bool) ([]cpu.TimesStat, error) {
	if percpu {
		return perCPUTimes()
	}

	var ret []cpu.TimesStat
	var lpIdleTime FILETIME
	var lpKernelTime FILETIME
	var lpUserTime FILETIME
	r, _, _ := ProcGetSystemTimes.Call(
		uintptr(unsafe.Pointer(&lpIdleTime)),
		uintptr(unsafe.Pointer(&lpKernelTime)),
		uintptr(unsafe.Pointer(&lpUserTime)))
	if r == 0 {
		return ret, windows.GetLastError()
	}

	LOT := float64(0.0000001)
	HIT := (LOT * 4294967296.0)
	idle := ((HIT * float64(lpIdleTime.DwHighDateTime)) + (LOT * float64(lpIdleTime.DwLowDateTime)))
	user := ((HIT * float64(lpUserTime.DwHighDateTime)) + (LOT * float64(lpUserTime.DwLowDateTime)))
	kernel := ((HIT * float64(lpKernelTime.DwHighDateTime)) + (LOT * float64(lpKernelTime.DwLowDateTime)))
	system := (kernel - idle)

	ret = append(ret, cpu.TimesStat{
		CPU:    "cpu-total",
		Idle:   float64(idle),
		User:   float64(user),
		System: float64(system),
	})
	return ret, nil
}

// perCPUTimes returns times stat per cpu, per core and overall for all CPUs
func perCPUTimes() ([]cpu.TimesStat, error) {
	var ret []cpu.TimesStat
	stats, err := PerfInfo()
	if err != nil {
		return nil, err
	}
	for _, v := range stats {
		c := cpu.TimesStat{
			CPU:    v.Name,
			User:   float64(v.PercentUserTime),
			System: float64(v.PercentPrivilegedTime),
			Idle:   float64(v.PercentIdleTime),
			Irq:    float64(v.PercentInterruptTime),
		}
		ret = append(ret, c)
	}
	return ret, nil
}

// PerfInfo returns the performance counter's instance value for ProcessorInformation.
// Name property is the key by which overall, per cpu and per core metric is known.
// 已知问题： windows xp PercentIdleTime 为0
func PerfInfo() ([]Win32_PerfFormattedData_PerfOS_Processor, error) {
	return PerfInfoWithContext(context.Background())
}

func PerfInfoWithContext(ctx context.Context) ([]Win32_PerfFormattedData_PerfOS_Processor, error) {
	var ret []Win32_PerfFormattedData_PerfOS_Processor

	q := wmi.CreateQuery(&ret, "")
	err := WMIQueryWithContext(ctx, q, &ret)
	if err != nil {
		return []Win32_PerfFormattedData_PerfOS_Processor{}, err
	}

	return ret, err
}

// WMIQueryWithContext - wraps wmi.Query with a timed-out context to avoid hanging
func WMIQueryWithContext(ctx context.Context, query string, dst interface{}, connectServerArgs ...interface{}) error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- wmi.Query(query, dst, connectServerArgs...)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}
