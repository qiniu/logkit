package system

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	TypeMetricNet   = "net"
	MetricNetUsages = "网络设备状态(net)"

	// TypeMetricNet 信息中的字段
	KeyNetBytesSent       = "net_bytes_sent"
	KeyNetBytesSentPerSec = "net_bytes_sent_per_sec"
	KeyNetBytesRecv       = "net_bytes_recv"
	KeyNetBytesRecvPerSec = "net_bytes_recv_per_sec"
	KeyNetPacketsSent     = "net_packets_sent"
	KeyNetPacketsRecv     = "net_packets_recv"
	KeyNetErrIn           = "net_err_in"
	KeyNetErrOut          = "net_err_out"
	KeyNetDropIn          = "net_drop_in"
	KeyNetDropOut         = "net_drop_out"
	KeyNetInterface       = "net_interface"
)

// KeyNetUsages TypeMetricNet 中的字段名称
var KeyNetUsages = KeyValueSlice{
	{KeyNetBytesSent, "网卡发包总数(bytes)", ""},
	{KeyNetBytesSentPerSec, "网卡发包速率(bytes/s)", ""},
	{KeyNetBytesRecv, "网卡收包总数(bytes)", ""},
	{KeyNetBytesRecvPerSec, "网卡收包速率(bytes/s)", ""},
	{KeyNetPacketsSent, "网卡发包数量", ""},
	{KeyNetPacketsRecv, "网卡收包数量", ""},
	{KeyNetErrIn, "网卡收包错误数量", ""},
	{KeyNetErrOut, "网卡发包错误数量", ""},
	{KeyNetDropIn, "网卡收 丢包数量", ""},
	{KeyNetDropOut, "网卡发 丢包数量", ""},
	{KeyNetInterface, "网卡设备名称", ""},
}

type CollectInfo struct {
	timestamp time.Time
	BytesSent uint64
	BytesRecv uint64
}

type NetIOStats struct {
	ps          PS
	lastCollect map[string]CollectInfo

	skipChecks     bool
	skipProtoState bool     `json:"skip_protocols_state"`
	Interfaces     []string `json:"interfaces"`
}

func (_ *NetIOStats) Name() string {
	return TypeMetricNet
}

func (_ *NetIOStats) Usages() string {
	return MetricNetUsages
}

func (_ *NetIOStats) Tags() []string {
	return []string{KeyNetInterface}
}

func (_ *NetIOStats) Config() map[string]interface{} {
	configOption := []Option{
		{
			KeyName:      "interfaces",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "收集特定网卡的信息,用','分隔(interfaces)",
			Type:         metric.ConfigTypeArray,
		},
		{
			KeyName:       "skip_protocols_state",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "是否忽略各个网络协议的状态信息",
			Type:          metric.ConfigTypeBool,
		},
	}
	config := map[string]interface{}{
		metric.OptionString:     configOption,
		metric.AttributesString: KeyNetUsages,
	}
	return config
}

func (s *NetIOStats) Collect() (datas []map[string]interface{}, err error) {
	netio, err := s.ps.NetIO()
	if err != nil {
		return nil, fmt.Errorf("error getting net io info: %s", err)
	}

	for _, io := range netio {
		if len(s.Interfaces) != 0 {
			var found bool

			for _, name := range s.Interfaces {
				if name == io.Name {
					found = true
					break
				}
			}

			if !found {
				continue
			}
		} else if !s.skipChecks {
			iface, err := net.InterfaceByName(io.Name)
			if err != nil {
				continue
			}

			if iface.Flags&net.FlagLoopback == net.FlagLoopback {
				continue
			}

			if iface.Flags&net.FlagUp == 0 {
				continue
			}
		}

		fields := map[string]interface{}{
			KeyNetBytesSent:   io.BytesSent,
			KeyNetBytesRecv:   io.BytesRecv,
			KeyNetPacketsSent: io.PacketsSent,
			KeyNetPacketsRecv: io.PacketsRecv,
			KeyNetErrIn:       io.Errin,
			KeyNetErrOut:      io.Errout,
			KeyNetDropIn:      io.Dropin,
			KeyNetDropOut:     io.Dropout,
			KeyNetInterface:   io.Name,
		}
		thisTime := time.Now()
		if info, ok := s.lastCollect[io.Name]; ok {
			dur := thisTime.Sub(info.timestamp)
			sentBytesDur := io.BytesSent - info.BytesSent
			recvBytesDur := io.BytesRecv - info.BytesRecv
			secs := float64(dur) / float64(time.Second)
			if secs > 0 {
				fields[KeyNetBytesSentPerSec] = uint64(float64(sentBytesDur) / secs)
				fields[KeyNetBytesRecvPerSec] = uint64(float64(recvBytesDur) / secs)
			}
		}
		s.lastCollect[io.Name] = CollectInfo{
			timestamp: thisTime,
			BytesRecv: io.BytesRecv,
			BytesSent: io.BytesSent,
		}
		datas = append(datas, fields)
	}

	if !s.skipProtoState {
		// Get system wide stats for different network protocols
		// (ignore these stats if the call fails)
		netprotos, _ := s.ps.NetProto()
		fields := make(map[string]interface{})
		for _, proto := range netprotos {
			for stat, value := range proto.Stats {
				name := TypeMetricNet + "_" + strings.ToLower(proto.Protocol) + "_" + strings.ToLower(stat)
				fields[name] = value
			}
		}
		fields[KeyNetInterface] = "all"
		datas = append(datas, fields)
	}
	return
}

func init() {
	metric.Add(TypeMetricNet, func() metric.Collector {
		return &NetIOStats{
			ps:          newSystemPS(),
			lastCollect: make(map[string]CollectInfo),
		}
	})
}
