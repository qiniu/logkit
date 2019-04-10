package system

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/metric/system/utils"

	"github.com/qiniu/logkit/utils/os"
)

func Test_runCommandIdName(t *testing.T) {
	t.Parallel()
	var comm string
	osInfo := os.GetOSInfo()
	if osInfo.Kernel == GoOSMac {
		comm = "ps x -o pid=,command= -r | head -n 10"
	} else if osInfo.Kernel == GoOSLinux {
		comm = "ps -Ao pid=,cmd= --sort=-pcpu | head -n 10"
	}

	processes := make(map[utils.PID]ProcessInfo)
	err := runCommandIdName(processes, comm)
	assert.Nil(t, err)
	assert.NotNil(t, processes)
}

func Test_runCommand(t *testing.T) {
	t.Parallel()
	var comm string
	osInfo := os.GetOSInfo()
	switch osInfo.Kernel {
	case GoOSMac:
		comm = "ps x -o pid=,command= -r | head -n 10"
	case GoOSLinux:
		comm = "ps -Ao pid=,cmd= --sort=-pcpu | head -n 10"
	default:
		comm = "ps -Ao pid=,cmd= --sort=-pcpu | head -n 10"
	}

	processes := make(map[utils.PID]ProcessInfo)
	err := runCommand(processes, comm)
	assert.Error(t, err)
	assert.NotNil(t, processes)
}

func Test_getBool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		config map[string]interface{}
		key    string
		expect bool
	}{
		{
			config: map[string]interface{}{},
			key:    "",
			expect: false,
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "b",
			expect: false,
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "a",
			expect: false,
		},
		{
			config: map[string]interface{}{
				"a": true,
			},
			key:    "a",
			expect: true,
		},
		{
			config: map[string]interface{}{
				"a": false,
			},
			key:    "a",
			expect: false,
		},
	}

	for _, test := range tests {
		assert.EqualValues(t, test.expect, getBool(test.config, test.key))
	}
}

func Test_getString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		config map[string]interface{}
		key    string
		expect string
	}{
		{
			config: map[string]interface{}{},
			key:    "",
			expect: "",
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "b",
			expect: "",
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "a",
			expect: "b",
		},
		{
			config: map[string]interface{}{
				"a": true,
			},
			key:    "a",
			expect: "",
		},
	}

	for _, test := range tests {
		assert.EqualValues(t, test.expect, getString(test.config, test.key))
	}
}

func Test_getBoolOr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		config map[string]interface{}
		key    string
		dft    bool
		expect bool
	}{
		{
			config: map[string]interface{}{},
			key:    "",
			dft:    true,
			expect: true,
		},
		{
			config: map[string]interface{}{},
			key:    "",
			dft:    false,
			expect: false,
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "b",
			dft:    true,
			expect: true,
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "b",
			dft:    false,
			expect: false,
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "a",
			dft:    false,
			expect: false,
		},
		{
			config: map[string]interface{}{
				"a": "b",
			},
			key:    "a",
			dft:    true,
			expect: true,
		},
		{
			config: map[string]interface{}{
				"a": true,
			},
			key:    "a",
			dft:    false,
			expect: true,
		},
		{
			config: map[string]interface{}{
				"a": false,
			},
			key:    "a",
			dft:    true,
			expect: false,
		},
	}

	for _, test := range tests {
		assert.EqualValues(t, test.expect, getBoolOr(test.config, test.key, test.dft))
	}
}
