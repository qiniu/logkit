// +build linux

package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/qiniu/logkit/metric"
)

// /proc/stat file line prefixes to gather stats on:
var (
	interrupts       = []byte("intr")
	context_switches = []byte("ctxt")
	processes_forked = []byte("processes")
	disk_pages       = []byte("page")
	boot_time        = []byte("btime")
)

type Kernel struct {
	statFile string
}

func (k *Kernel) Name() string {
	return "kernel"
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
			fields["interrupts"] = int64(m)
		case bytes.Equal(field, context_switches):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields["context_switches"] = int64(m)
		case bytes.Equal(field, processes_forked):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields["processes_forked"] = int64(m)
		case bytes.Equal(field, boot_time):
			m, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields["boot_time"] = int64(m)
		case bytes.Equal(field, disk_pages):
			in, err := strconv.ParseInt(string(dataFields[i+1]), 10, 64)
			if err != nil {
				return nil, err
			}
			out, err := strconv.ParseInt(string(dataFields[i+2]), 10, 64)
			if err != nil {
				return nil, err
			}
			fields["disk_pages_in"] = int64(in)
			fields["disk_pages_out"] = int64(out)
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
	metric.Add("kernel", func() metric.Collector {
		return &Kernel{
			statFile: "/proc/stat",
		}
	})
}
