package system

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	TypeMetricNetstat   = "netstat"
	MetricNetstatUsages = "网络连接情况(netstat)"

	// TypeMetricNetstat 信息中的字段
	KeyNetstatTcpEstablished = "netstat_tcp_established"
	KeyNetstatTcpSynSent     = "netstat_tcp_syn_sent"
	KeyNetstatTcpSynRecv     = "netstat_tcp_syn_recv"
	KeyNetstatTcpFinWait1    = "netstat_tcp_fin_wait1"
	KeyNetstatTcpFinWait2    = "netstat_tcp_fin_wait2"
	KeyNetstatTcpTimeWait    = "netstat_tcp_time_wait"
	KeyNetstatTcpClose       = "netstat_tcp_close"
	KeyNetstatTcpCloseWait   = "netstat_tcp_close_wait"
	KeyNetstatTcpLastAck     = "netstat_tcp_last_ack"
	KeyNetstatTcpListen      = "netstat_tcp_listen"
	KeyNetstatTcpClosing     = "netstat_tcp_closing"
	KeyNetstatTcpNone        = "netstat_tcp_none"
	KeyNetstatUdpSocket      = "netstat_udp_socket"
)

// KeyNetStatUsages TypeMetricNetstat 的字段名称
var KeyNetStatUsages = KeyValueSlice{
	{KeyNetstatTcpEstablished, "ESTABLISHED状态的网络链接数", ""},
	{KeyNetstatTcpSynSent, "SYN_SENT状态的网络链接数", ""},
	{KeyNetstatTcpSynRecv, "SYN_RECV状态的网络链接数", ""},
	{KeyNetstatTcpFinWait1, "FIN_WAIT1状态的网络链接数", ""},
	{KeyNetstatTcpFinWait2, "FIN_WAIT2状态的网络链接数", ""},
	{KeyNetstatTcpTimeWait, "TIME_WAIT状态的网络链接数", ""},
	{KeyNetstatTcpClose, "CLOSE状态的网络链接数", ""},
	{KeyNetstatTcpCloseWait, "CLOSE_WAIT状态的网络链接数", ""},
	{KeyNetstatTcpLastAck, "LAST_ACK状态的网络链接数", ""},
	{KeyNetstatTcpListen, "LISTEN状态的网络链接数", ""},
	{KeyNetstatTcpClosing, "CLOSING状态的网络链接数", ""},
	{KeyNetstatTcpNone, "NONE状态的网络链接数", ""},
	{KeyNetstatUdpSocket, "UDP状态的网络链接数", ""},
}

type NetStats struct {
	ps PS
}

func (_ *NetStats) Name() string {
	return TypeMetricNetstat
}

func (_ *NetStats) Usages() string {
	return MetricNetstatUsages
}

func (_ *NetStats) Tags() []string {
	return []string{}
}

func (_ *NetStats) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: KeyNetStatUsages,
	}
	return config
}

func (s *NetStats) Collect() (datas []map[string]interface{}, err error) {
	if runtime.GOOS == "windows" {
		return s.winCollect()
	}
	netconns, err := s.ps.NetConnections()
	if err != nil {
		return nil, fmt.Errorf("error getting net connections info: %s", err)
	}

	counts := make(map[string]int)
	counts["UDP"] = 0

	// TODO: add family to results
	for _, netcon := range netconns {
		if netcon.Type == syscall.SOCK_DGRAM {
			counts["UDP"] += 1
			continue // UDP has no status
		}
		c, ok := counts[netcon.Status]
		if !ok {
			counts[netcon.Status] = 0
		}
		counts[netcon.Status] = c + 1
	}

	fields := map[string]interface{}{
		KeyNetstatTcpEstablished: counts["ESTABLISHED"],
		KeyNetstatTcpSynSent:     counts["SYN_SENT"],
		KeyNetstatTcpSynRecv:     counts["SYN_RECV"],
		KeyNetstatTcpFinWait1:    counts["FIN_WAIT1"],
		KeyNetstatTcpFinWait2:    counts["FIN_WAIT2"],
		KeyNetstatTcpTimeWait:    counts["TIME_WAIT"],
		KeyNetstatTcpClose:       counts["CLOSE"],
		KeyNetstatTcpCloseWait:   counts["CLOSE_WAIT"],
		KeyNetstatTcpLastAck:     counts["LAST_ACK"],
		KeyNetstatTcpListen:      counts["LISTEN"],
		KeyNetstatTcpClosing:     counts["CLOSING"],
		KeyNetstatTcpNone:        counts["NONE"],
		KeyNetstatUdpSocket:      counts["UDP"],
	}
	datas = append(datas, fields)
	return
}
func (s *NetStats) winCollect() (datas []map[string]interface{}, err error) {
	// only TCP here
	out, err := exec.Command("cmd", "/c", "netstat -anp TCP").Output()
	if err != nil {
		return nil, fmt.Errorf("exec cmd failed, error: %v", err.Error())
	}
	outStr := string(out)
	fields := map[string]interface{}{
		KeyNetstatTcpEstablished: strings.Count(outStr, "ESTABLISHED"),
		KeyNetstatTcpSynSent:     strings.Count(outStr, "SYN_SENT"),
		KeyNetstatTcpSynRecv:     strings.Count(outStr, "SYN_RECV"),
		KeyNetstatTcpFinWait1:    strings.Count(outStr, "FIN_WAIT1"),
		KeyNetstatTcpFinWait2:    strings.Count(outStr, "FIN_WAIT2"),
		KeyNetstatTcpTimeWait:    strings.Count(outStr, "TIME_WAIT"),
		KeyNetstatTcpClose:       strings.Count(outStr, "CLOSE"),
		KeyNetstatTcpCloseWait:   strings.Count(outStr, "CLOSE_WAIT"),
		KeyNetstatTcpLastAck:     strings.Count(outStr, "LAST_ACK"),
		KeyNetstatTcpListen:      strings.Count(outStr, "LISTEN"),
		KeyNetstatTcpClosing:     strings.Count(outStr, "CLOSING"),
	}
	return append(datas, fields), nil
}

func init() {
	metric.Add(TypeMetricNetstat, func() metric.Collector {
		return &NetStats{ps: newSystemPS()}
	})
}
