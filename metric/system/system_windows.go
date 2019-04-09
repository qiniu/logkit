package system

import (
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/host"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
)

// KeySystemUsages TypeMetricSystem的字段名称
var WinSystemUsages = KeyValueSlice{
	{KeySystemLoad1, "1分钟平均load值", ""},
	{KeySystemLoad5, "5分钟平均load值", ""},
	{KeySystemLoad15, "15分钟平均load值", ""},
	//{KeySystemNUsers, "用户数", ""},
	{KeySystemNCpus, "CPU核数", ""},
	{KeySystemUptime, "系统启动时间", ""},
	{KeySystemUptimeFormat, "格式化的系统启动时间", ""},
}

type WinSystemStats struct {
	SystemStats
}

func (s *WinSystemStats) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: WinSystemUsages,
	}
	return config
}

func (*WinSystemStats) Collect() (datas []map[string]interface{}, err error) {
	lp, err := LoadPercentage()
	if err != nil {
		return
	}
	uptime, err := host.Uptime()
	if err != nil {
		log.Errorf("Get Uptime failed, error: %v", err)
	}
	data := map[string]interface{}{
		KeySystemLoad1:  lp,
		KeySystemLoad5:  lp,
		KeySystemLoad15: lp,
		//KeySystemNUsers:       len(users),
		KeySystemNCpus:        runtime.NumCPU(),
		KeySystemUptime:       uptime,
		KeySystemUptimeFormat: formatUptime(uptime),
		KeySystemNNetCards:    getNumNetCard(),
		KeySystemNDisks:       getNumDisk(),
		KeySystemNServices:    getNumService(),
	}
	datas = []map[string]interface{}{data}
	return
}

func init() {
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &WinSystemStats{}
	})
}

//若无法获取磁盘个数，返回挂载点的个数
func getNumDisk() (mountsNum int) {
	err := ioutil.WriteFile("diskpart.txt", []byte("list disk"), os.ModeAppend)
	if err != nil {
		log.Debugf("write file diskpart.txt failed %v, will not collect disknum", err)
		return 0
	}
	out, err := exec.Command("diskpart", "/S", "diskpart.txt").Output()
	if err != nil {
		log.Debugf("get disk number error %v", err)
		return 0
	}
	str := string(out)
	index := strings.Index(str, "--------  -------------  -------  -------  ---  ---")
	if index < 0 {
		return 0
	}
	disks := strings.Split(str[index:], "\n")
	return len(disks) - 2
}

func getNumService() int {
	return 0
}
