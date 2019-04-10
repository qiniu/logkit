// +build !windows

package system

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/shirou/gopsutil/cpu"
)

func Test_totalCpuTime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		timeStats cpu.TimesStat
		expect    float64
	}{
		{
			timeStats: cpu.TimesStat{},
			expect:    0,
		},
		{
			timeStats: cpu.TimesStat{
				User:    1,
				System:  1,
				Idle:    1,
				Nice:    1,
				Iowait:  1,
				Irq:     1,
				Softirq: 1,
				Steal:   1,
			},
			expect: 8,
		},
	}

	for _, test := range tests {
		assert.EqualValues(t, test.expect, totalCpuTime(test.timeStats))
	}
}
