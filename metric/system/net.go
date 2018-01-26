package system

import (
	"fmt"
	"net"
	"strings"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	TypeMetricNet   = "net"
	MetricNetUsages = "网络设备状态(net)"

	// TypeMetricNet 信息中的字段
	KeyNetBytesSent   = "net_bytes_sent"
	KeyNetBytesRecv   = "net_bytes_recv"
	KeyNetPacketsSent = "net_packets_sent"
	KeyNetPacketsRecv = "net_packets_recv"
	KeyNetErrIn       = "net_err_in"
	KeyNetErrOut      = "net_err_out"
	KeyNetDropIn      = "net_drop_in"
	KeyNetDropOut     = "net_drop_out"
	KeyNetInterface   = "net_interface"
)

// KeyNetUsages TypeMetricNet 中的字段名称
var KeyNetUsages = []KeyValue{
	{KeyNetBytesSent, "网卡发包总数(bytes)"},
	{KeyNetBytesRecv, "网卡收包总数(bytes)"},
	{KeyNetPacketsSent, "网卡发包数量"},
	{KeyNetPacketsRecv, "网卡收包数量"},
	{KeyNetErrIn, "网卡收包错误数量"},
	{KeyNetErrOut, "网卡发包错误数量"},
	{KeyNetDropIn, "网卡收 丢包数量"},
	{KeyNetDropOut, "网卡发 丢包数量"},
	{KeyNetInterface, "网卡设备名称"},
}

type NetIOStats struct {
	ps PS

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
		return &NetIOStats{ps: newSystemPS()}
	})
}
