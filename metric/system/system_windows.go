package system

import (
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/w32"
)

// KeySystemUsages TypeMetricSystem的字段名称
var WinSystemUsages = []KeyValue{
	{KeySystemLoad1, "1分钟平均load值"},
	{KeySystemLoad5, "5分钟平均load值"},
	{KeySystemLoad15, "15分钟平均load值"},
	//{KeySystemNUsers, "用户数"},
	{KeySystemNCpus, "CPU核数"},
	{KeySystemUptime, "系统启动时间"},
	{KeySystemUptimeFormat, "格式化的系统启动时间"},
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

func (_ *WinSystemStats) Collect() (datas []map[string]interface{}, err error) {
	lp, err := cpu.LoadPercentage()
	if err != nil {
		return
	}
	uptime := w32.GetTickCount64() / 1000 //second
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
	defer func() {
		if mountsNum == -1 {
			diskMetrics, ok := metric.Collectors["disk"]
			if !ok {
				log.Errorf("metric disk is not support now")
			}
			mounts, err := diskMetrics().Collect()
			if err != nil {
				log.Error("disk metrics collect have error %v", err)
			}
			mountsNum = len(mounts)
		}
	}()
	err := ioutil.WriteFile("diskpart.txt", []byte("list disk"), os.ModeAppend)
	if err != nil {
		log.Error("write file diskpart.txt have error %v", err)
	}
	out, err := exec.Command("diskpart", "/S", "diskpart.txt").Output()
	if err != nil {
		log.Error("get disk number have error %v", err)
		return -1
	}
	str := string(out)
	index := strings.Index(str, "--------  -------------  -------  -------  ---  ---")
	disks := strings.Split(str[index:], "\n")
	return len(disks) - 2
}

func getNumService() int {
	return 0
}
