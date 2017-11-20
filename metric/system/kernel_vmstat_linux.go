// +build linux

package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/utils"
)

const (
	TypeMetricKernelVmstat   = "kernel_vmstat"
	MetricKernelVmstatUsages = "内核(kernel_vmstat)"
)

type KernelVmstat struct {
	statFile string
}

func (k *KernelVmstat) Name() string {
	return TypeMetricKernelVmstat
}

func (k *KernelVmstat) Usages() string {
	return MetricKernelVmstatUsages
}

func (k *KernelVmstat) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []utils.Option{},
		metric.AttributesString: []utils.KeyValue{},
	}
	return config
}

func (k *KernelVmstat) Collect() (datas []map[string]interface{}, err error) {
	data, err := k.getProcVmstat()
	if err != nil {
		return nil, err
	}

	now := time.Now().Format(time.RFC3339Nano)
	fields := make(map[string]interface{})
	dataFields := bytes.Fields(data)
	for i, field := range dataFields {

		// dataFields is an array of {"stat1_name", "stat1_value", "stat2_name",
		// "stat2_value", ...}
		// We only want the even number index as that contain the stat name.
		if i%2 == 0 {
			// Convert the stat value into an integer.
			m, err := strconv.Atoi(string(dataFields[i+1]))
			if err != nil {
				return nil, err
			}
			key := "vmstat_" + string(field)
			fields[key] = int64(m)
		}
	}
	fields["vmstat_"+metric.Timestamp] = now
	datas = append(datas, fields)
	return
}

func (k *KernelVmstat) getProcVmstat() ([]byte, error) {
	if _, err := os.Stat(k.statFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("kernel_vmstat: %s does not exist!", k.statFile)
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
	metric.Add(TypeMetricKernelVmstat, func() metric.Collector {
		return &KernelVmstat{
			statFile: "/proc/vmstat",
		}
	})
}
