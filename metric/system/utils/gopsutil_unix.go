// +build linux darwin

package utils

import (
	"github.com/shirou/gopsutil/net"
)

// ProtoCounters wrapper
func ProtoCounters(protocols []string) ([]net.ProtoCountersStat, error) {
	return net.ProtoCounters(protocols)
}
