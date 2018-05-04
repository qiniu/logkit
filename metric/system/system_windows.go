package system

import (
	"runtime"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/w32"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"
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
	}
	datas = append(datas, data)
	return
}

func init() {
	metric.Add(TypeMetricSystem, func() metric.Collector {
		return &WinSystemStats{}
	})
}
