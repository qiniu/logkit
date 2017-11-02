package system

import (
	"bytes"
	"io/ioutil"
	"strconv"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/utils"
)

// https://www.kernel.org/doc/Documentation/sysctl/fs.txt
type SysctlFS struct {
	path string
}

func (_ SysctlFS) Name() string {
	return "linux_sysctl_fs"
}

func (_ SysctlFS) Usages() string {
	return "linux_sysctl_fs"
}

func (_ SysctlFS) Config() []utils.Option {
	return []utils.Option{}
}

func (_ SysctlFS) Attributes() []utils.KeyValue {
	return []utils.KeyValue{}
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
		fields[name] = v
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

	fields[name] = v
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
	metric.Add("linux_sysctl_fs", func() metric.Collector {
		return &SysctlFS{
			path: "/proc/sys/fs",
		}
	})
}
