// +build windows

package net

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"unsafe"

	"github.com/shirou/gopsutil/internal/common"
	"golang.org/x/sys/windows"
)

var (
	modiphlpapi             = windows.NewLazyDLL("iphlpapi.dll")
	procGetExtendedTCPTable = modiphlpapi.NewProc("GetExtendedTcpTable")
	procGetExtendedUDPTable = modiphlpapi.NewProc("GetExtendedUdpTable")
	procGetTcpTable         = modiphlpapi.NewProc("GetTcpTable")
)

const (
	TCPTableBasicListener = iota
	TCPTableBasicConnections
	TCPTableBasicAll
	TCPTableOwnerPIDListener
	TCPTableOwnerPIDConnections
	TCPTableOwnerPIDAll
	TCPTableOwnerModuleListener
	TCPTableOwnerModuleConnections
	TCPTableOwnerModuleAll
)
const ANY_SIZE = 100

type DWORD uint32
type MIB_TCPTABLE struct {
	DwNumEntries DWORD
	Table        [ANY_SIZE]MIB_TCPROW // TODO: pass array to dll func
}
type PMIB_TCPTABLE *MIB_TCPTABLE
type MIB_TCPROW struct {
	State        MIB_TCP_STATE
	DwLocalAddr  DWORD
	DwLocalPort  DWORD
	DwRemoteAddr DWORD
	DwRemotePort DWORD
}
type MIB_TCP_STATE int32

const (
	MIB_TCP_STATE_CLOSED     MIB_TCP_STATE = 1
	MIB_TCP_STATE_LISTEN                   = 2
	MIB_TCP_STATE_SYN_SENT                 = 3
	MIB_TCP_STATE_SYN_RCVD                 = 4
	MIB_TCP_STATE_ESTAB                    = 5
	MIB_TCP_STATE_FIN_WAIT1                = 6
	MIB_TCP_STATE_FIN_WAIT2                = 7
	MIB_TCP_STATE_CLOSE_WAIT               = 8
	MIB_TCP_STATE_CLOSING                  = 9
	MIB_TCP_STATE_LAST_ACK                 = 10
	MIB_TCP_STATE_TIME_WAIT                = 11
	MIB_TCP_STATE_DELETE_TCB               = 12
)

var TcpStateMap = map[MIB_TCP_STATE]string{
	MIB_TCP_STATE_CLOSED:     "CLOSE",
	MIB_TCP_STATE_LISTEN:     "LISTEN",
	MIB_TCP_STATE_SYN_SENT:   "SYN_SENT",
	MIB_TCP_STATE_SYN_RCVD:   "SYN_RECV",
	MIB_TCP_STATE_ESTAB:      "ESTABLISHED",
	MIB_TCP_STATE_FIN_WAIT1:  "FIN_WAIT1",
	MIB_TCP_STATE_FIN_WAIT2:  "FIN_WAIT2",
	MIB_TCP_STATE_CLOSE_WAIT: "CLOSE_WAIT",
	MIB_TCP_STATE_CLOSING:    "CLOSING",
	MIB_TCP_STATE_LAST_ACK:   "LAST_ACK",
	MIB_TCP_STATE_TIME_WAIT:  "TIME_WAIT",
}

func GetTcpTable(tcpTable PMIB_TCPTABLE, sizePointer *uint32, order bool) uint32 {
	ret1, _, _ := procGetTcpTable.Call(
		uintptr(unsafe.Pointer(tcpTable)),
		uintptr(unsafe.Pointer(sizePointer)),
		getUintptrFromBool(order))
	return uint32(ret1)
}

func IOCounters(pernic bool) ([]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), pernic)
}

func IOCountersWithContext(ctx context.Context, pernic bool) ([]IOCountersStat, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var ret []IOCountersStat

	for _, ifi := range ifs {
		c := IOCountersStat{
			Name: ifi.Name,
		}

		row := windows.MibIfRow{Index: uint32(ifi.Index)}
		e := windows.GetIfEntry(&row)
		if e != nil {
			return nil, os.NewSyscallError("GetIfEntry", e)
		}
		c.BytesSent = uint64(row.OutOctets)
		c.BytesRecv = uint64(row.InOctets)
		c.PacketsSent = uint64(row.OutUcastPkts)
		c.PacketsRecv = uint64(row.InUcastPkts)
		c.Errin = uint64(row.InErrors)
		c.Errout = uint64(row.OutErrors)
		c.Dropin = uint64(row.InDiscards)
		c.Dropout = uint64(row.OutDiscards)

		ret = append(ret, c)
	}

	if pernic == false {
		return getIOCountersAll(ret)
	}
	return ret, nil
}

// NetIOCountersByFile is an method which is added just a compatibility for linux.
func IOCountersByFile(pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCountersByFileWithContext(context.Background(), pernic, filename)
}

func IOCountersByFileWithContext(ctx context.Context, pernic bool, filename string) ([]IOCountersStat, error) {
	return IOCounters(pernic)
}

// Return a list of network connections opened by a process
func Connections(kind string) ([]ConnectionStat, error) {
	return ConnectionsWithContext(context.Background(), kind)
}

func ConnectionsWithContext(ctx context.Context, kind string) ([]ConnectionStat, error) {
	var ret []ConnectionStat
	var tcpTable PMIB_TCPTABLE
	var sizePointer uint32 = 0
	resCode := GetTcpTable(tcpTable, &sizePointer, true)
	//ERROR_INSUFFICIENT_BUFFER(122)
	//The data area passed to a system call is too small.
	if resCode != 122 {
		return ret, fmt.Errorf("Call win func GetTcpTable failed, errCode: %v", resCode)
	}
	tcpTable = &MIB_TCPTABLE{}
	// call twice
	resCode = GetTcpTable(tcpTable, &sizePointer, true)
	if resCode != 0 {
		return ret, fmt.Errorf("Call win func GetTcpTable failed, errCode: %v", resCode)
	}
	for i := 0; DWORD(i) < tcpTable.DwNumEntries; i++ {
		cs := ConnectionStat{
			Status: TcpStateMap[tcpTable.Table[i].State],
		}
		ret = append(ret, cs)
	}
	return ret, nil
}

// Return a list of network connections opened returning at most `max`
// connections for each running process.
func ConnectionsMax(kind string, max int) ([]ConnectionStat, error) {
	return ConnectionsMaxWithContext(context.Background(), kind, max)
}

func ConnectionsMaxWithContext(ctx context.Context, kind string, max int) ([]ConnectionStat, error) {
	return []ConnectionStat{}, common.ErrNotImplementedError
}

func FilterCounters() ([]FilterStat, error) {
	return FilterCountersWithContext(context.Background())
}

func FilterCountersWithContext(ctx context.Context) ([]FilterStat, error) {
	return nil, errors.New("NetFilterCounters not implemented for windows")
}

// NetProtoCounters returns network statistics for the entire system
// If protocols is empty then all protocols are returned, otherwise
// just the protocols in the list are returned.
// Not Implemented for Windows
func ProtoCounters(protocols []string) ([]ProtoCountersStat, error) {
	return ProtoCountersWithContext(context.Background(), protocols)
}

func ProtoCountersWithContext(ctx context.Context, protocols []string) ([]ProtoCountersStat, error) {
	return nil, errors.New("NetProtoCounters not implemented for windows")
}

func getUintptrFromBool(b bool) uintptr {
	if b {
		return 1
	} else {
		return 0
	}
}
