package system

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
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

	// Config 中的字段
	ConfigDiskIgnoreFs    = "ignore_fs"
	ConfigDiskMountPoints = "mount_points"
)

// KeyDiskUsages TypeMetricDisk 字段名称
var KeyDiskUsages = KeyValueSlice{
	{KeyDiskPath, "磁盘路径", ""},
	{KeyDiskDevice, "磁盘设备名", ""},
	{KeyDiskFstype, "文件系统类型", ""},
	{KeyDiskTotal, "磁盘总大小", ""},
	{KeyDiskFree, "磁盘剩余大小", ""},
	{KeyDiskUsed, "磁盘用量", ""},
	{KeyDiskUsedPercent, "磁盘已用百分比", ""},
	{KeyDiskInodesTotal, "总inode数量", ""},
	{KeyDiskInodesFree, "空闲的inode数量", ""},
	{KeyDiskInodesUsed, "使用的inode数量", ""},
}

// ConfigDiskUsages TypeMetricDisk config 中的字段描述
var ConfigDiskUsages = KeyValueSlice{
	{ConfigDiskIgnoreFs, "忽略的挂载点,用','分隔(" + ConfigDiskIgnoreFs + ")", ""},
	{ConfigDiskMountPoints, "收集特定挂载点信息,默认收集所有挂载点,用','分隔(" + ConfigDiskMountPoints + ")", ""},
}

var diskMux sync.Mutex

type DiskStats struct {
	ps PS

	MountPoints []string `json:"mount_points"`
	IgnoreFS    []string `json:"ignore_fs"`
}

func (*DiskStats) Name() string {
	return TypeMetricDisk
}

func (*DiskStats) Usages() string {
	return MetricDiskUsages
}

func (*DiskStats) Tags() []string {
	return []string{KeyDiskFstype, KeyDiskPath, KeyDiskDevice}
}

func (*DiskStats) Config() map[string]interface{} {
	configOptions := make([]Option, 0)
	for _, val := range ConfigDiskUsages {
		option := Option{
			KeyName:      val.Key,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  val.Value,
			Type:         metric.ConfigTypeArray,
		}
		configOptions = append(configOptions, option)
	}
	config := map[string]interface{}{
		metric.OptionString:     configOptions,
		metric.AttributesString: KeyDiskUsages,
	}
	return config
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
		var usedPercent float64
		if du.Used+du.Free > 0 {
			usedPercent = float64(du.Used) /
				(float64(du.Used) + float64(du.Free)) * 100
		}

		fields := map[string]interface{}{
			KeyDiskPath:        du.Path,
			KeyDiskDevice:      strings.Replace(partitions[i].Device, "/dev/", "", -1),
			KeyDiskFstype:      du.Fstype,
			KeyDiskTotal:       du.Total,
			KeyDiskFree:        du.Free,
			KeyDiskUsed:        du.Used,
			KeyDiskUsedPercent: usedPercent,
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
	KeyDiskioReads                  = "diskio_reads"
	KeyDiskioWrites                 = "diskio_writes"
	KeyDiskioReadsPerSec            = "diskio_reads_per_sec"
	KeyDiskioWritesPerSec           = "diskio_writes_per_sec"
	KeyDiskioMergedReadCount        = "diskio_merged_read_count"
	KeyDiskioMergedWriteCount       = "diskio_merged_write_count"
	KeyDiskioMergedReadCountPerSec  = "diskio_merged_read_count_per_sec"
	KeyDiskioMergedWriteCountPerSec = "diskio_merged_write_count_per_sec"
	KeyDiskioReadBytes              = "diskio_read_bytes"
	KeyDiskioWriteBytes             = "diskio_write_bytes"
	KeyDiskioReadBytesPerSec        = "diskio_read_bytes_per_sec"
	KeyDiskioWriteBytesPerSec       = "diskio_write_bytes_per_sec"
	KeyDiskioReadTime               = "diskio_read_time"
	KeyDiskioWriteTime              = "diskio_write_time"
	KeyDiskioReadAWait              = "diskio_read_await"
	KeyDiskioWriteAWait             = "diskio_write_await"
	KeyDiskioAWait                  = "diskio_await"
	KeyDiskioIoTime                 = "diskio_io_time"
	KeyDiskioIoUtil                 = "diskio_io_util"
	KeyDiskioIopsInProgress         = "diskio_iops_in_progress"
	KeyDiskioName                   = "diskio_name"
	KeyDiskioSerial                 = "diskio_serial"

	// Config 字段
	ConfigDiskioDevices          = "devices"
	ConfigDiskioDeviceTags       = "device_tags"
	ConfigDiskioNameTemplates    = "name_templates"
	ConfigDiskioSkipSerialNumber = "skip_serial_number"
)

// KeyDiskioUsages TypeMetricDiskio 中的字段名称
var KeyDiskioUsages = KeyValueSlice{
	{KeyDiskioReads, "磁盘被读的总次数", ""},
	{KeyDiskioWrites, "磁盘被写的总次数", ""},
	{KeyDiskioReadsPerSec, "每秒磁盘被读的次数", ""},
	{KeyDiskioWritesPerSec, "每秒磁盘被写的次数", ""},
	{KeyDiskioMergedReadCount, "磁盘合并读总次数", ""},
	{KeyDiskioMergedWriteCount, "磁盘合并写的总次数", ""},
	{KeyDiskioMergedReadCountPerSec, "每秒磁盘合并读次数", ""},
	{KeyDiskioMergedWriteCountPerSec, "每秒磁盘合并写的次数", ""},
	{KeyDiskioReadBytes, "读取的总数据量", ""},
	{KeyDiskioWriteBytes, "写入的总数据量", ""},
	{KeyDiskioReadBytesPerSec, "每秒读取的数据量(bytes/s)", ""},
	{KeyDiskioWriteBytesPerSec, "每秒写入的数据量(bytes/s)", ""},
	{KeyDiskioReadTime, "磁盘读取总用时", ""},
	{KeyDiskioWriteTime, "磁盘写入总用时", ""},
	{KeyDiskioReadAWait, "每个读操作平均所需的时间", ""},
	{KeyDiskioWriteAWait, "每个写操作平均所需的时间", ""},
	{KeyDiskioAWait, "每个I/O平均所需的时间", ""},
	{KeyDiskioIoTime, "io总时间", ""},
	{KeyDiskioIoUtil, "设备的繁忙比率", ""},
	{KeyDiskioIopsInProgress, "运行中的每秒IO数据量", ""},
	{KeyDiskioName, "磁盘名称", ""},
	{KeyDiskioSerial, "磁盘序列号", ""},
}

// ConfigDiskioUsages TypeMetricDiskio 配置项描述
var ConfigDiskioUsages = KeyValueSlice{
	{ConfigDiskioDevices, "获取特定设备的信息,用','隔开(" + ConfigDiskioDevices + ")", ""},
	{ConfigDiskioDeviceTags, "采集磁盘某些tag的信息,用','隔开(" + ConfigDiskioDeviceTags + ")", ""},
	{ConfigDiskioNameTemplates, "一些尝试加入设备的模板列表,用','隔开(" + ConfigDiskioNameTemplates + ")", ""},
	{ConfigDiskioSkipSerialNumber, "是否忽略磁盘序列号(" + ConfigDiskioSkipSerialNumber + ")", ""},
}

type DiskioCollectInfo struct {
	timestamp        time.Time
	ReadCount        uint64
	WriteCount       uint64
	MergedReadCount  uint64
	MergedWriteCount uint64
	ReadBytes        uint64
	WriteBytes       uint64
	ReadTime         uint64
	WriteTime        uint64
	IoTime           uint64
}

type DiskIOStats struct {
	ps          PS
	lastCollect map[string]DiskioCollectInfo

	Devices          []string `json:"devices"`
	DeviceTags       []string `json:"device_tags"`
	NameTemplates    []string `json:"name_templates"`
	SkipSerialNumber bool     `json:"skip_serial_number"`

	infoCache map[string]diskInfoCache
}

func (*DiskIOStats) Name() string {
	return TypeMetricDiskio
}

func (*DiskIOStats) Usages() string {
	return MetricDiskioUsages
}

func (*DiskIOStats) Tags() []string {
	return []string{KeyDiskioName, KeyDiskioSerial}
}

func (*DiskIOStats) Config() map[string]interface{} {
	configOptions := make([]Option, 0)
	for i := 0; i < 3; i++ {
		option := Option{
			KeyName:      ConfigDiskioUsages[i].Key,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  ConfigDiskioUsages[i].Value,
			Type:         metric.ConfigTypeArray,
		}
		configOptions = append(configOptions, option)
	}
	option := Option{
		KeyName:       ConfigDiskioUsages[3].Key,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		DefaultNoUse:  false,
		Description:   ConfigDiskioUsages[3].Value,
		Type:          metric.ConfigTypeBool,
	}
	configOptions = append(configOptions, option)

	config := map[string]interface{}{
		metric.OptionString:     configOptions,
		metric.AttributesString: KeyDiskioUsages,
	}
	return config
}

func (s *DiskIOStats) Collect() (datas []map[string]interface{}, err error) {
	//this lock is for fix panic, as multiple metric runner work here
	//signal arrived during cgo execution
	diskMux.Lock()
	defer diskMux.Unlock()
	diskio, err := s.ps.DiskIO(s.Devices)
	if err != nil {
		return nil, fmt.Errorf("error getting disk io info: %s", err)
	}

	for _, io := range diskio {
		fields := map[string]interface{}{
			KeyDiskioReads:                  io.ReadCount,
			KeyDiskioWrites:                 io.WriteCount,
			KeyDiskioReadsPerSec:            0,
			KeyDiskioWritesPerSec:           0,
			KeyDiskioMergedReadCount:        io.MergedReadCount,
			KeyDiskioMergedWriteCount:       io.MergedWriteCount,
			KeyDiskioMergedReadCountPerSec:  0,
			KeyDiskioMergedWriteCountPerSec: 0,
			KeyDiskioReadBytes:              io.ReadBytes,
			KeyDiskioWriteBytes:             io.WriteBytes,
			KeyDiskioReadBytesPerSec:        0,
			KeyDiskioWriteBytesPerSec:       0,
			KeyDiskioReadTime:               io.ReadTime,
			KeyDiskioWriteTime:              io.WriteTime,
			KeyDiskioReadAWait:              0,
			KeyDiskioWriteAWait:             0,
			KeyDiskioAWait:                  0,
			KeyDiskioIoTime:                 io.IoTime,
			KeyDiskioIoUtil:                 0,
			KeyDiskioIopsInProgress:         io.IopsInProgress,
			KeyDiskioName:                   s.diskName(io.Name),
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
		thisTime := time.Now()
		if info, ok := s.lastCollect[io.Name]; ok {
			dur := thisTime.Sub(info.timestamp)
			// 当前时间获取的数据有问题，本次的 ReadBytes或者WriteBytes 比上次的小，本次的数据和上一次的数据有一个有问题，这里都清理掉，重新开始采集
			if io.ReadBytes < info.ReadBytes || io.WriteBytes < info.WriteBytes {
				log.Warnf("error getting disk io info failed curReadBytes[%v] < "+
					"lastReadBytes[%v] || curWriteBytes[%v] < lastWriteBytes[%v]", io.ReadBytes, info.ReadBytes,
					io.WriteBytes, info.WriteBytes)
				delete(s.lastCollect, io.Name)
				continue
			}
			readsDur := io.ReadCount - info.ReadCount
			writesDur := io.WriteCount - info.WriteCount
			mergeReadDur := io.MergedReadCount - info.MergedReadCount
			mergeWriteDur := io.MergedWriteCount - info.MergedReadCount
			readBytesDur := io.ReadBytes - info.ReadBytes
			writeBytesDur := io.WriteBytes - info.WriteBytes
			readTimeDur := io.ReadTime - info.ReadTime
			writeTimeDur := io.WriteTime - info.WriteTime
			utilDur := io.IoTime - info.IoTime
			secs := float64(dur) / float64(time.Second)

			if secs > 0 {
				fields[KeyDiskioReadsPerSec] = uint64(float64(readsDur) / secs)
				fields[KeyDiskioWritesPerSec] = uint64(float64(writesDur) / secs)
				fields[KeyDiskioMergedReadCount] = uint64(float64(mergeReadDur) / secs)
				fields[KeyDiskioMergedWriteCount] = uint64(float64(mergeWriteDur) / secs)
				fields[KeyDiskioReadBytesPerSec] = uint64(float64(readBytesDur) / secs)
				fields[KeyDiskioWriteBytesPerSec] = uint64(float64(writeBytesDur) / secs)
				if readsDur > 0 {
					fields[KeyDiskioReadAWait] = float64(readTimeDur) / float64(readsDur)
				}
				if writesDur > 0 {
					fields[KeyDiskioWriteAWait] = float64(writeTimeDur) / float64(writesDur)
				}
				if val := readsDur + writesDur; val > 0 {
					fields[KeyDiskioAWait] = float64(readTimeDur+writeTimeDur) / float64(val)
				}
				fields[KeyDiskioIoUtil] = uint64(float64(utilDur) / 10 / secs)
			}
		}
		s.lastCollect[io.Name] = DiskioCollectInfo{
			timestamp:        thisTime,
			ReadCount:        io.ReadCount,
			WriteCount:       io.WriteCount,
			MergedReadCount:  io.MergedReadCount,
			MergedWriteCount: io.MergedWriteCount,
			ReadBytes:        io.ReadBytes,
			WriteBytes:       io.WriteBytes,
			ReadTime:         io.ReadTime,
			WriteTime:        io.WriteTime,
			IoTime:           io.IoTime,
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
	ps1 := newSystemPS()
	metric.Add(TypeMetricDisk, func() metric.Collector {
		return &DiskStats{ps: ps1}
	})
	ps2 := newSystemPS()
	metric.Add(TypeMetricDiskio, func() metric.Collector {
		return &DiskIOStats{ps: ps2,
			lastCollect:      make(map[string]DiskioCollectInfo),
			SkipSerialNumber: true}
	})
}
