package system

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/utils"
)

const (
	TypeMetricDisk   = "disk"
	MetricDiskUsages = "磁盘(disk)"

	// TypeMetricDisk 信息中的字段
	KeyDiskPath        = "disk_path"
	KeyDiskDevice      = "disk_device"
	KeyDiskFstype      = "disk_fstype"
	KeyDiskTotal       = "disk_total"
	KeyDiskFree        = "disk_free"
	KeyDiskUsed        = "disk_used"
	KeyDiskUsedPercent = "disk_used_percent"
	KeyDiskInodesTotal = "disk_inodes_total"
	KeyDiskInodesFree  = "disk_inodes_fress"
	KeyDiskInodesUsed  = "disk_inodes_used"
)

// KeyDiskUsages TypeMetricDisk 字段名称
var KeyDiskUsages = []utils.KeyValue{
	{KeyDiskPath, "磁盘路径"},
	{KeyDiskDevice, "磁盘设备名"},
	{KeyDiskFstype, "文件系统类型"},
	{KeyDiskTotal, "磁盘总大小"},
	{KeyDiskFree, "磁盘剩余大小"},
	{KeyDiskUsed, "磁盘用量"},
	{KeyDiskUsedPercent, "磁盘已用百分比"},
	{KeyDiskInodesTotal, "总inode数量"},
	{KeyDiskInodesFree, "空闲的inode数量"},
	{KeyDiskInodesUsed, "适用的inode数量"},
}

type DiskStats struct {
	ps PS

	MountPoints []string `json:"mount_points"`
	IgnoreFS    []string `json:"ignore_fs"`
}

func (_ *DiskStats) Name() string {
	return TypeMetricDisk
}

func (_ *DiskStats) Usages() string {
	return MetricDiskUsages
}

func (_ *DiskStats) Config() []utils.Option {
	return []utils.Option{}
}

func (_ *DiskStats) Attributes() []utils.KeyValue {
	return KeyDiskUsages
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
			KeyDiskPath:        du.Path,
			KeyDiskDevice:      strings.Replace(partitions[i].Device, "/dev/", "", -1),
			KeyDiskFstype:      du.Fstype,
			KeyDiskTotal:       du.Total,
			KeyDiskFree:        du.Free,
			KeyDiskUsed:        du.Used,
			KeyDiskUsedPercent: used_percent,
			KeyDiskInodesTotal: du.InodesTotal,
			KeyDiskInodesFree:  du.InodesFree,
			KeyDiskInodesUsed:  du.InodesUsed,
		}
		datas = append(datas, fields)
	}

	return
}

const (
	TypeMetricDiskio   = "diskio"
	MetricDiskioUsages = "磁盘IO(diskIo)"

	// TypeMetricDiskio 信息中的字段
	KeyDiskioReads          = "diskio_reads"
	KeyDiskioWrites         = "diskio_writes"
	KeyDiskioReadBytes      = "diskio_read_bytes"
	KeyDiskioWriteBytes     = "diskio_write_bytes"
	KeyDiskioReadTime       = "diskio_read_time"
	KeyDiskioWriteTime      = "diskio_write_time"
	KeyDiskioIoTime         = "diskio_io_time"
	KeyDiskioIopsInProgress = "diskio_iops_in_progress"
	KeyDiskioName           = "diskio_name"
	KeyDiskioSerial         = "serial"
)

// KeyDiskioUsages TypeMetricDiskio 中的字段名称
var KeyDiskioUsages = []utils.KeyValue{
	{KeyDiskioReads, "磁盘被读的总次数"},
	{KeyDiskioWrites, "磁盘被写的总次数"},
	{KeyDiskioReadBytes, "读取的总数据量"},
	{KeyDiskioWriteBytes, "写入的总数据量"},
	{KeyDiskioReadTime, "磁盘读取总用时"},
	{KeyDiskioWriteTime, "磁盘写入总用时"},
	{KeyDiskioIoTime, "io总时间"},
	{KeyDiskioIopsInProgress, "运行中的每秒IO数据量"},
	{KeyDiskioName, "磁盘名称"},
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
	return TypeMetricDiskio
}

var diskIoSampleConfig = `

`

func (_ *DiskIOStats) Usages() string {
	return MetricDiskioUsages
}

func (_ *DiskIOStats) Config() []utils.Option {
	return []utils.Option{}
}

func (_ *DiskIOStats) Attributes() []utils.KeyValue {
	return KeyDiskioUsages
}

func (s *DiskIOStats) Collect() (datas []map[string]interface{}, err error) {
	diskio, err := s.ps.DiskIO(s.Devices)
	if err != nil {
		return nil, fmt.Errorf("error getting disk io info: %s", err)
	}

	for _, io := range diskio {
		fields := map[string]interface{}{
			KeyDiskioReads:          io.ReadCount,
			KeyDiskioWrites:         io.WriteCount,
			KeyDiskioReadBytes:      io.ReadBytes,
			KeyDiskioWriteBytes:     io.WriteBytes,
			KeyDiskioReadTime:       io.ReadTime,
			KeyDiskioWriteTime:      io.WriteTime,
			KeyDiskioIoTime:         io.IoTime,
			KeyDiskioIopsInProgress: io.IopsInProgress,
			KeyDiskioName:           s.diskName(io.Name),
		}
		for t, v := range s.diskTags(io.Name) {
			fields[t] = v
		}
		if !s.SkipSerialNumber {
			if len(io.SerialNumber) != 0 {
				fields[KeyDiskioSerial] = io.SerialNumber
			} else {
				fields[KeyDiskioSerial] = "unknown"
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
	metric.Add(TypeMetricDisk, func() metric.Collector {
		return &DiskStats{ps: ps}
	})

	metric.Add(TypeMetricDiskio, func() metric.Collector {
		return &DiskIOStats{ps: ps, SkipSerialNumber: true}
	})
}
