package system

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/qiniu/logkit/metric"
)

type DiskStats struct {
	ps PS

	MountPoints []string `json:"mount_points"`
	IgnoreFS    []string `json:"ignore_fs"`
}

func (_ *DiskStats) Name() string {
	return "disk"
}

var diskSampleConfig = `{
  "mount_points": ["/"],
  "ignore_fs":["tmpfs", "devtmpfs", "devfs"]
}`

func (_ *DiskStats) SampleConfig() string {
	return diskSampleConfig
}

func (s *DiskStats) Collect() (datas []map[string]interface{}, err error) {
	disks, partitions, err := s.ps.DiskUsage(s.MountPoints, s.IgnoreFS)
	if err != nil {
		return nil, fmt.Errorf("error getting disk usage info: %s", err)
	}

	for i, du := range disks {
		if du.Total == 0 {
			// Skip dummy filesystem (procfs, cgroupfs, ...)
			continue
		}
		var used_percent float64
		if du.Used+du.Free > 0 {
			used_percent = float64(du.Used) /
				(float64(du.Used) + float64(du.Free)) * 100
		}

		fields := map[string]interface{}{
			"path":         du.Path,
			"device":       strings.Replace(partitions[i].Device, "/dev/", "", -1),
			"fstype":       du.Fstype,
			"total":        du.Total,
			"free":         du.Free,
			"used":         du.Used,
			"used_percent": used_percent,
			"inodes_total": du.InodesTotal,
			"inodes_free":  du.InodesFree,
			"inodes_used":  du.InodesUsed,
		}
		datas = append(datas, fields)
	}

	return
}

type DiskIOStats struct {
	ps PS

	Devices          []string
	DeviceTags       []string
	NameTemplates    []string
	SkipSerialNumber bool

	infoCache map[string]diskInfoCache
}

func (_ *DiskIOStats) Name() string {
	return "diskio"
}

var diskIoSampleConfig = `

`

func (s *DiskIOStats) Collect() (datas []map[string]interface{}, err error) {
	diskio, err := s.ps.DiskIO(s.Devices)
	if err != nil {
		return nil, fmt.Errorf("error getting disk io info: %s", err)
	}

	for _, io := range diskio {
		fields := map[string]interface{}{
			"reads":            io.ReadCount,
			"writes":           io.WriteCount,
			"read_bytes":       io.ReadBytes,
			"write_bytes":      io.WriteBytes,
			"read_time":        io.ReadTime,
			"write_time":       io.WriteTime,
			"io_time":          io.IoTime,
			"iops_in_progress": io.IopsInProgress,
			"name":             s.diskName(io.Name),
		}
		for t, v := range s.diskTags(io.Name) {
			fields[t] = v
		}
		if !s.SkipSerialNumber {
			if len(io.SerialNumber) != 0 {
				fields["serial"] = io.SerialNumber
			} else {
				fields["serial"] = "unknown"
			}
		}
		datas = append(datas, fields)
	}
	return
}

var varRegex = regexp.MustCompile(`\$(?:\w+|\{\w+\})`)

func (s *DiskIOStats) diskName(devName string) string {
	di, err := s.diskInfo(devName)
	if err != nil {
		// discard error :-(
		// We can't return error because it's non-fatal to the Gather().
		// And we have no logger, so we can't log it.
		return devName
	}
	if di == nil {
		return devName
	}

	for _, nt := range s.NameTemplates {
		miss := false
		name := varRegex.ReplaceAllStringFunc(nt, func(sub string) string {
			sub = sub[1:] // strip leading '$'
			if sub[0] == '{' {
				sub = sub[1 : len(sub)-1] // strip leading & trailing '{' '}'
			}
			if v, ok := di[sub]; ok {
				return v
			}
			miss = true
			return ""
		})

		if !miss {
			return name
		}
	}

	return devName
}

func (s *DiskIOStats) diskTags(devName string) map[string]string {
	di, err := s.diskInfo(devName)
	if err != nil {
		// discard error :-(
		// We can't return error because it's non-fatal to the Gather().
		// And we have no logger, so we can't log it.
		return nil
	}
	if di == nil {
		return nil
	}

	tags := map[string]string{}
	for _, dt := range s.DeviceTags {
		if v, ok := di[dt]; ok {
			tags[dt] = v
		}
	}

	return tags
}

func init() {
	ps := newSystemPS()
	metric.Add("disk", func() metric.Collector {
		return &DiskStats{ps: ps}
	})

	metric.Add("diskio", func() metric.Collector {
		return &DiskIOStats{ps: ps, SkipSerialNumber: true}
	})
}
