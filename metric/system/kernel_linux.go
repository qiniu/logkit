// +build linux

package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

// /proc/stat file line prefixes to gather stats on:
var (
	interrupts       = []byte("intr")
	context_switches = []byte("ctxt")
	processes_forked = []byte("processes")
	disk_pages       = []byte("page")
	boot_time        = []byte("btime")
)

const (
	TypeMetricKernel   = "kernel"
	MetricKernelUsages = "内核(kernel)"

	// TypeMetricKernel 信息中的字段
	KernelInterrupts      = "kernel_interrupts"
	KernelContextSwitches = "kernel_context_switches"
	KernelProcessesForked = "kernel_processes_forked"
	KernelBootTime        = "kernel_boot_time"
	KernelDiskPagesIn     = "kernel_disk_pages_in"
	KernelDiskPagesOut    = "kernel_disk_pages_out"
)

// KeyKernelUsages TypeMetricKernel 中的字段名称
var KeyKernelUsages = KeyValueSlice{
	{KernelInterrupts, "内核中断次数", ""},
	{KernelContextSwitches, "内核上下文切换次数", ""},
	{KernelProcessesForked, "fork的进程数", ""},
	{KernelBootTime, "内核启动时间", ""},
	{KernelDiskPagesIn, "磁盘换入数量", ""},
	{KernelDiskPagesOut, "磁盘换出数量", ""},
}

type Kernel struct {
	statFile string
}

func (k *Kernel) Name() string {
	return TypeMetricKernel
}

func (k *Kernel) Usages() string {
	return MetricKernelUsages
}

func (_ *Kernel) Tags() []string {
	return []string{}
}

func (k *Kernel) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: KeyKernelUsages,
	}
	return config
}

func (k *Kernel) Collect() (datas []map[string]interface{}, err error) {
	data, err := k.getProcStat()
	if err != nil {
		return nil, err
	}

	fields := make(map[string]interface{})
	dataFields := bytes.Fields(data)
	for i, field := range dataFields {
		switch {
		case bytes.Equal(field, interrupts):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields[KernelInterrupts] = int64(m)
		case bytes.Equal(field, context_switches):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields[KernelContextSwitches] = int64(m)
		case bytes.Equal(field, processes_forked):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields[KernelProcessesForked] = int64(m)
		case bytes.Equal(field, boot_time):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields[KernelBootTime] = int64(m)
		case bytes.Equal(field, disk_pages):
			in, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			out, err := strconv.ParseInt(string(dataFields[i+2]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields[KernelDiskPagesIn] = int64(in)
			fields[KernelDiskPagesOut] = int64(out)
		}
	}
	datas = append(datas, fields)
	return
}

func (k *Kernel) getProcStat() ([]byte, error) {
	if _, err := os.Stat(k.statFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("kernel: %s does not exist!", k.statFile)
	} else if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(k.statFile)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func init() {
	metric.Add(TypeMetricKernel, func() metric.Collector {
		return &Kernel{
			statFile: "/proc/stat",
		}
	})
}
