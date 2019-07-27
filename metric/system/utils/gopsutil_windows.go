// +build windows

// 扩展github.com/shirou/gopsutil，避免修改vendor
package utils

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/StackExchange/wmi"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/net"
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

func LoadPercentage() (uint16, error) {
	var dst []cpu.Win32_Processor
	var lp uint16
	q := wmi.CreateQuery(&dst, "")
	if err := WMIQueryWithContext(context.Background(), q, &dst); err != nil {
		return lp, err
	}
	if len(dst) > 0 {
		for _, d := range dst {
			lp = lp + *d.LoadPercentage
		}
		return lp, nil
	}
	return lp, errors.New("No Processor LoadPercentage Found.")
}

var (
	modiphlpapi           = windows.NewLazyDLL("iphlpapi.dll")
	procGetIcmpStatistics = modiphlpapi.NewProc("GetIcmpStatistics")
	procGetTcpStatistics  = modiphlpapi.NewProc("GetTcpStatistics")
	procGetUdpStatistics  = modiphlpapi.NewProc("GetUdpStatistics")
)
var netProtocols = []string{"tcp", "udp"}
var netProtocolsObjs = map[string]StatsInterface{
	"tcp": &MIB_TCPSTATS{},
	"udp": &MIB_UDPSTATS{},
}

type DWORD uint32

type MIB_TCPROW struct {
	State        MIB_TCP_STATE
	DwLocalAddr  DWORD
	DwLocalPort  DWORD
	DwRemoteAddr DWORD
	DwRemotePort DWORD
}
type MIB_TCP_STATE int32

// copied from https://msdn.microsoft.com/en-us/library/windows/desktop/aa366020(v=vs.85).aspx
type MIB_TCPSTATS struct {
	dwRtoAlgorithm DWORD `json:"RtoAlgorithm"`
	dwRtoMin       DWORD `json:"RtoMin"`
	dwRtoMax       DWORD `json:"RtoMax"`
	dwMaxConn      DWORD `json:"MaxConn"`
	dwActiveOpens  DWORD `json:"ActiveOpens"`
	dwPassiveOpens DWORD `json:"PassiveOpens"`
	dwAttemptFails DWORD `json:"AttemptFails"`
	dwEstabResets  DWORD `json:"EstabResets"`
	dwCurrEstab    DWORD `json:"CurrEstab"`
	dwInSegs       DWORD `json:"InSegs"`
	dwOutSegs      DWORD `json:"OutSegs"`
	dwRetransSegs  DWORD `json:"RetransSegs"`
	dwInErrs       DWORD `json:"InErrs"`
	dwOutRstsv     DWORD `json:"OutRsts"`
	dwNumConns     DWORD `json:"NumConns"`
}
type PMIB_TCPSTATS *MIB_TCPSTATS

func (t *MIB_TCPSTATS) GetStatsFunc() DWORD {
	return GetTcpStatistics(t)
}
func (t *MIB_TCPSTATS) Name() string {
	return "tcp"
}

// copied from https://msdn.microsoft.com/en-us/library/windows/desktop/aa366929(v=vs.85).aspx
type MIB_UDPSTATS struct {
	dwInDatagrams  DWORD `json:"InDatagrams"`
	dwNoPorts      DWORD `json:"NoPorts"`
	dwInErrors     DWORD `json:"InErrors"`
	dwOutDatagrams DWORD `json:"OutDatagrams"`
	dwNumAddrs     DWORD `json:"NumAddrs"`
}
type PMIB_UDPSTATS *MIB_UDPSTATS

func (u *MIB_UDPSTATS) GetStatsFunc() DWORD {
	return GetUdpStatistics(u)
}
func (u *MIB_UDPSTATS) Name() string {
	return "udp"
}

type StatsInterface interface {
	GetStatsFunc() DWORD
	Name() string
}

func GetTcpStatistics(pStats PMIB_TCPSTATS) DWORD {
	ret, _, _ := procGetTcpStatistics.Call(
		uintptr(unsafe.Pointer(pStats)))
	return DWORD(ret)
}

func GetUdpStatistics(pStats PMIB_UDPSTATS) DWORD {
	ret, _, _ := procGetUdpStatistics.Call(
		uintptr(unsafe.Pointer(pStats)))
	return DWORD(ret)
}

// NetProtoCounters returns network statistics for the entire system
// If protocols is empty then all protocols are returned, otherwise
// just the protocols in the list are returned.
func ProtoCounters(protocols []string) ([]net.ProtoCountersStat, error) {
	return ProtoCountersWithContext(context.Background(), protocols)
}

func ProtoCountersWithContext(ctx context.Context, protocols []string) ([]net.ProtoCountersStat, error) {
	if len(protocols) == 0 {
		protocols = netProtocols
	}
	var pcs []net.ProtoCountersStat
	var err error
	for _, proto := range protocols {
		if o, ok := netProtocolsObjs[proto]; ok {
			pc, err := getProtoCountersStat(o)
			if err != nil {
				err = fmt.Errorf("%v %s stat err: ", err.Error(), o.Name())
			}
			if pc != nil {
				pcs = append(pcs, *pc)
			}
		}
	}
	return pcs, err
}

func StatsToProtoCountersStat(stats interface{}) map[string]int64 {
	t := reflect.TypeOf(stats).Elem()
	val := reflect.ValueOf(stats).Elem()
	ret := make(map[string]int64, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		ret[string(sf.Tag)] = int64(val.FieldByName(sf.Name).Uint())
	}
	return ret
}

func getProtoCountersStat(i StatsInterface) (*net.ProtoCountersStat, error) {
	if ret := i.GetStatsFunc(); ret != 0 {
		return nil, fmt.Errorf("get stat failed, errCode: %v", ret)
	}
	return &net.ProtoCountersStat{
		Protocol: i.Name(),
		Stats:    StatsToProtoCountersStat(i),
	}, nil
}
