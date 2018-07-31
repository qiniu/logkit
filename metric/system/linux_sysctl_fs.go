// +build linux

package system

import (
	"bytes"
	"io/ioutil"
	"strconv"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

// https://www.kernel.org/doc/Documentation/sysctl/fs.txt
type SysctlFS struct {
	path string
}

const (
	TypeLinuxSysctlFs        = "linux_sysctl_fs"
	MetricLinuxSysctlFsUsage = "linux内核信息(linux_sysctl_fs)"

	KeyLinuxSysctlFsAioNr           = "aio-nr"
	KeyLinuxSysctlFsAioMaxNr        = "aio-max-nr"
	KeyLinuxSysctlFsDquotNr         = "dquot-nr"
	KeyLinuxSysctlFsDquotMax        = "dquot-max"
	KeyLinuxSysctlFsSuperNr         = "super-nr"
	KeyLinuxSysctlFsSuperMax        = "super-max"
	KeyLinuxSysctlFsInodeNr         = "inode-nr"
	KeyLinuxSysctlFsInodeFreeNr     = "inode-free-nr"
	KeyLinuxSysctlFsInodePreNr      = "inode-preshrink-nr"
	KeyLinuxSysctlFsDentryNr        = "dentry-nr"
	KeyLinuxSysctlFsDentryUnNr      = "dentry-unused-nr"
	KeyLinuxSysctlFsDetryAgeLimit   = "dentry-age-limit"
	KeyLinuxSysctlFsDentryWantPages = "dentry-want-pages"
	KeyLinuxSysctlFsFileNr          = "file-nr"
	KeyLinuxSysctlFsFileMax         = "file-max"
)

var KeySysctlFsFieldNameMap = map[string]string{
	KeyLinuxSysctlFsAioNr:           "linux_sysctl_fs_aio_nr",
	KeyLinuxSysctlFsAioMaxNr:        "linux_sysctl_fs_aio_max_nr",
	KeyLinuxSysctlFsDquotNr:         "linux_sysctl_fs_dquot_nr",
	KeyLinuxSysctlFsDquotMax:        "linux_sysctl_fs_dquot_max",
	KeyLinuxSysctlFsSuperNr:         "linux_sysctl_fs_super_nr",
	KeyLinuxSysctlFsSuperMax:        "linux_sysctl_fs_super_max",
	KeyLinuxSysctlFsInodeNr:         "linux_sysctl_fs_inode_nr",
	KeyLinuxSysctlFsInodeFreeNr:     "linux_sysctl_fs_inode_free_nr",
	KeyLinuxSysctlFsInodePreNr:      "linux_sysctl_fs_inode_preshrink_nr",
	KeyLinuxSysctlFsDentryNr:        "linux_sysctl_fs_dentry_nr",
	KeyLinuxSysctlFsDentryUnNr:      "linux_sysctl_fs_dentry_unused_nr",
	KeyLinuxSysctlFsDetryAgeLimit:   "linux_sysctl_fs_dentry_age_limit",
	KeyLinuxSysctlFsDentryWantPages: "linux_sysctl_fs_dentry_want_pages",
	KeyLinuxSysctlFsFileNr:          "linux_sysctl_fs_file_nr",
	KeyLinuxSysctlFsFileMax:         "linux_sysctl_fs_file_max",
}

var KeyLinuxSysctlFsUsage = KeyValueSlice{
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsAioNr], "当前 aio 请求数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsAioMaxNr], "最大允许的 aio 请求", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsDquotNr], "分配的磁盘配额项及空余项", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsDquotMax], "缓存的磁盘配额的最大值", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsSuperNr], "已分配的 super block 数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsSuperMax], "系统能够分配的 super block 数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsInodeNr], "分配的 inode 数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsInodeFreeNr], "空闲的 inode 数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsInodePreNr], "inode 预缩减数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsDentryNr], "当前分配的 dentry 缓存数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsDentryUnNr], "未使用的 dentry 缓存数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsDetryAgeLimit], "dentry 缓存被创建以来的时长", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsDentryWantPages], "系统需要的页面数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsFileNr], "已分配、使用的和最大的文件句柄数", ""},
	{KeySysctlFsFieldNameMap[KeyLinuxSysctlFsFileMax], "内核支持的最大file handle数量", ""},
}

func (_ *SysctlFS) Name() string {
	return TypeLinuxSysctlFs
}

func (_ *SysctlFS) Usages() string {
	return MetricLinuxSysctlFsUsage
}

func (_ *SysctlFS) Tags() []string {
	return []string{}
}

func (_ *SysctlFS) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: KeyLinuxSysctlFsUsage,
	}
	return config
}

func (sfs *SysctlFS) gatherList(file string, fields map[string]interface{}, fieldNames ...string) error {
	bs, err := ioutil.ReadFile(sfs.path + "/" + file)
	if err != nil {
		return err
	}

	bsplit := bytes.Split(bytes.TrimRight(bs, "\n"), []byte{'\t'})
	for i, name := range fieldNames {
		if i >= len(bsplit) {
			break
		}
		if name == "" {
			continue
		}

		v, err := strconv.ParseUint(string(bsplit[i]), 10, 64)
		if err != nil {
			return err
		}
		fields[KeySysctlFsFieldNameMap[name]] = v
	}

	return nil
}

func (sfs *SysctlFS) gatherOne(name string, fields map[string]interface{}) error {
	bs, err := ioutil.ReadFile(sfs.path + "/" + name)
	if err != nil {
		return err
	}

	v, err := strconv.ParseUint(string(bytes.TrimRight(bs, "\n")), 10, 64)
	if err != nil {
		return err
	}

	fields[KeySysctlFsFieldNameMap[name]] = v
	return nil
}

func (sfs *SysctlFS) Collect() (datas []map[string]interface{}, err error) {
	fields := map[string]interface{}{}

	for _, n := range []string{"aio-nr", "aio-max-nr", "dquot-nr", "dquot-max", "super-nr", "super-max"} {
		sfs.gatherOne(n, fields)
	}

	sfs.gatherList("inode-state", fields, "inode-nr", "inode-free-nr", "inode-preshrink-nr")
	sfs.gatherList("dentry-state", fields, "dentry-nr", "dentry-unused-nr", "dentry-age-limit", "dentry-want-pages")
	sfs.gatherList("file-nr", fields, "file-nr", "", "file-max")

	datas = append(datas, fields)
	return
}

func init() {
	metric.Add(TypeLinuxSysctlFs, func() metric.Collector {
		return &SysctlFS{
			path: "/proc/sys/fs",
		}
	})
}
