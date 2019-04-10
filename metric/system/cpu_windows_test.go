// +build windows

package system

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_isTotalCpuTimeStat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		expect bool
	}{
		{
			name:   "",
			expect: false,
		},
		{
			name:   WindowsCPUTotalKey,
			expect: false,
		},
		{
			name:   MetricCPUTotalKey,
			expect: true,
		},
	}

	for _, test := range tests {
		assert.EqualValues(t, test.expect, isTotalCpuTimeStat(test.name))
	}
}

func Test_isTotalCpuUsageStat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		expect bool
	}{
		{
			name:   "",
			expect: false,
		},
		{
			name:   WindowsCPUTotalKey,
			expect: true,
		},
		{
			name:   MetricCPUTotalKey,
			expect: false,
		},
	}

	for _, test := range tests {
		assert.EqualValues(t, test.expect, isTotalCpuUsageStat(test.name))
	}
}
