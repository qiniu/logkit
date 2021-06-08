package utils

import (
	"fmt"

	"github.com/shirou/gopsutil/net"
)

// ProtoCounters wrapper
func ProtoCounters(protocols []string) ([]net.ProtoCountersStat, error) {
	return nil, fmt.Errorf("unsupport solaris")
}
