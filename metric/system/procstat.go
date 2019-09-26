// +build !windows

package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/process"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/metric/system/utils"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

const (
	TypeMetricProcstat  = "procstat"
	MetricProcstatUsage = "详细进程信息(procstat)"

	// TypeMetricProcstat 信息中的字段
	KeyProcstatProcessName      = "procstat_process_name"
	KeyProcstatPid              = "procstat_pid"
	KeyProcstatStatus           = "procstat_status"
	KeyProcstatThreadsNum       = "procstat_threads_num"
	KeyProcstatFdsNum           = "procstat_fds_num"
	KeyProcstatVolConSwitches   = "procstat_voluntary_context_switches"
	KeyProcstatInVolConSwitches = "procstat_involuntary_context_switches"
	KeyProcstatReadCount        = "procstat_read_count"
	KeyProcstatWriteCount       = "procstat_write_count"
	KeyProcstatReadBytes        = "procstat_read_bytes"
	KeyProcstatWriteBytes       = "procstat_write_bytes"

	KeyProcstatCpuTime          = "procstat_cpu_time"
	KeyProcstatCpuUsage         = "procstat_cpu_usage"
	KeyProcstatCpuTimeUser      = "procstat_cpu_time_user"
	KeyProcstatCpuTimeSystem    = "procstat_cpu_time_system"
	KeyProcstatCpuTimeIdle      = "procstat_cpu_time_idle"
	KeyProcstatCpuTimeNice      = "procstat_cpu_time_nice"
	KeyProcstatCpuTimeIoWait    = "procstat_cpu_time_iowait"
	KeyProcstatCpuTimeIrq       = "procstat_cpu_time_irq"
	KeyProcstatCpuTimeSoftirq   = "procstat_cpu_time_softirq"
	KeyProcstatCpuTimeSteal     = "procstat_cpu_time_steal"
	KeyProcstatCpuTimeStolen    = "procstat_cpu_time_stolen"
	KeyProcstatCpuTimeGuest     = "procstat_cpu_time_guest"
	KeyProcstatCpuTimeGuestNice = "procstat_cpu_time_guest_nice"

	KeyProcstatMemRss    = "procstat_mem_rss"
	KeyProcstatMemVms    = "procstat_mem_vms"
	KeyProcstatMemSwap   = "procstat_mem_swap"
	KeyProcstatMemData   = "procstat_mem_data"
	KeyProcstatMemStack  = "procstat_mem_stack"
	KeyProcstatMemLocked = "procstat_mem_locked"

	KeyProcstatFileLocks        = "procstat_file_locks"
	KeyProcstatSignalsPending   = "procstat_signals_pending"
	KeyProcstatNicePriority     = "procstat_nice_priority"
	KeyProcstatRealtimePriority = "procstat_realtime_priority"

	KeyProcstatRlimitCpuTimeSoft          = "procstat_rlimit_cpu_time_soft"
	KeyProcstatRlimitMemRssSoft           = "procstat_rlimit_mem_rss_soft"
	KeyProcstatRlimitMemVmsSoft           = "procstat_rlimit_mem_vms_soft"
	KeyProcstatRlimitFdsNumSoft           = "procstat_rlimit_mem_swap_soft"
	KeyProcstatRlimitMemDataSoft          = "procstat_rlimit_mem_data_soft"
	KeyProcstatRlimitMemStackSoft         = "procstat_rlimit_mem_stack_soft"
	KeyProcstatRlimitMemLockedSoft        = "procstat_rlimit_mem_locked_soft"
	KeyProcstatRlimitSignalsPendingSoft   = "procstat_rlimit_signals_pending_soft"
	KeyProcstatRlimitFileLocksSoft        = "procstat_rlimit_file_locks_soft"
	KeyProcstatRlimitNicePrioritySoft     = "procstat_rlimit_nice_priority_soft"
	KeyProcstatRlimitRealtimePrioritySoft = "procstat_rlimit_realtime_priority_soft"

	KeyProcstatRlimitCpuTimeHard          = "procstat_rlimit_cpu_time_hard"
	KeyProcstatRlimitMemRssHard           = "procstat_rlimit_mem_rss_hard"
	KeyProcstatRlimitMemVmsHard           = "procstat_rlimit_mem_vms_hard"
	KeyProcstatRlimitFdsNumHard           = "procstat_rlimit_mem_swap_hard"
	KeyProcstatRlimitMemDataHard          = "procstat_rlimit_mem_data_hard"
	KeyProcstatRlimitMemStackHard         = "procstat_rlimit_mem_stack_hard"
	KeyProcstatRlimitMemLockedHard        = "procstat_rlimit_mem_locked_hard"
	KeyProcstatRlimitSignalsPendingHard   = "procstat_rlimit_signals_pending_hard"
	KeyProcstatRlimitFileLocksHard        = "procstat_rlimit_file_locks_hard"
	KeyProcstatRlimitNicePriorityHard     = "procstat_rlimit_nice_priority_hard"
	KeyProcstatRlimitRealtimePriorityHard = "procstat_rlimit_realtime_priority_hard"
)

const (
	GoOSMac     = "Darwin"
	GoOSLinux   = "Linux"
	GoOSWindows = "windows"
)

var processStat = map[string]int{
	"RUNNING":  0, // 默认或者非 supervisor 监控的进程都会是正在运行，所以选择 0 表示running
	"STARTING": 1,
	"STOPPING": 2,
	"STOPPED":  3,
	"EXITED":   4,
	"FATAL":    5,
}

var (
	defaultPIDFinder = NewPgrep
	defaultProcess   = NewProc
)

type ProcessInfo struct {
	Pid    PID
	name   string
	Status int
	Process
}

// linux 和 mac 使用原来的会截断，例如 java 和 python 脚本程序只会显示java和python
// 现在top 和 supervised的程序会自己设置name，没有再走原来的逻辑
func (p *ProcessInfo) Name() (string, error) {
	return p.name, nil
}

type Procstat struct {
	PidTag      bool
	Prefix      string
	ProcessName string
	config      map[string]interface{}

	MemRelated      bool `json:"mem_related"`
	IoRelated       bool `json:"io_related"`
	CtxSwitRelated  bool `json:"context_switch_related"`
	ThreadsRelated  bool `json:"threads_related"`
	FileDescRelated bool `json:"file_descriptor_related"`
	CpuTimeRelated  bool `json:"cpu_time_related"`
	CpuUsageRelated bool `json:"cpu_usage_related"`
	ResourceLimits  bool `json:"resource_limits"`

	CpuTop10    bool `json:"cpu_top_10"`
	MemTop10    bool `json:"mem_top_10"`
	Supervisord bool `json:"supervisord"`
	DaemonTools bool `json:"daemon_tools"`

	PidFile     string `json:"pid_file"`
	Exe         string `json:"exe"`
	Pattern     string `json:"pattern"`
	User        string `json:"user"`
	SystemdUnit string `json:"system_unit"`
	CGroup      string `json:"cgroup"`

	kernel          string
	pidFinder       PIDFinder
	createPIDFinder func() (PIDFinder, error)
	procs           map[string]ProcessInfo
	createProcess   func(PID) (Process, error)
}

// KeyProcUsages TypeMetricProc 中的字段名称
var KeyProcUsages = models.KeyValueSlice{}

const (
	MemRelated      = "mem_related"
	IoRelated       = "io_related"
	CtxSwitRelated  = "context_switch_related"
	ThreadsRelated  = "threads_related"
	FileDescRelated = "file_descriptor_related"
	CpuTimeRelated  = "cpu_time_related"
	CpuUsageRelated = "cpu_usage_related"
	ResourceLimits  = "resource_limits"
	CpuTop10        = "cpu_top_10"
	MemTop10        = "mem_top_10"
	Supervisord     = "supervisord"
	DaemonTools     = "daemon_tools"
	PidFile         = "pid_file"
	Exe             = "exe"
	Pattern         = "pattern"
	User            = "user"
	SystemdUnit     = "system_unit"
	CGroup          = "cgroup"
)

// ConfigProcUsages TypeMetricProc 配置项的描述
var ConfigProcUsages = []models.Option{
	{
		KeyName:       "cpu_usage_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集Cpu用量相关的信息(cpu_usage_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "mem_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集内存相关的信息(mem_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "cpu_time_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集CPU时间相关的信息(cpu_time_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "io_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集IO相关的信息(io_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "threads_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集线程数量(threads_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "context_switch_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集内核上下文切换次数(context_switch_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "file_descriptor_related",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集文件描述相关信息(file_descriptor_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "resource_limits",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集资源限制相关的信息(resource_limits)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "cpu_top_10",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集CPU利用率前10的进程(cpu_top_10)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "mem_top_10",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集内存占用前10的进程(mem_top_10)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "supervisord",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集supervisord管理的进程(supervisord)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "daemon_tools",
		Element:       models.Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集daemontools管理的进程(daemon_tools)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:      "pid_file",
		DefaultNoUse: false,
		ChooseOnly:   false,
		Default:      "",
		Description:  "填写 pid 文件路径(pid_file)",
	},
	{
		KeyName:      "exe",
		DefaultNoUse: false,
		ChooseOnly:   false,
		Default:      "",
		Description:  "填写可执行文件名称(pgrep xx)(exe)",
	},
	{
		KeyName:      "pattern",
		DefaultNoUse: false,
		ChooseOnly:   false,
		Default:      "",
		Description:  "填写命令行表达式(pgrep -f xx)(pattern)",
	},
	{
		KeyName:      "user",
		DefaultNoUse: false,
		ChooseOnly:   false,
		Default:      "",
		Description:  "填写用户名(pgrep -u xx)(user)",
	},
	{
		KeyName:      "system_unit",
		DefaultNoUse: false,
		ChooseOnly:   false,
		Default:      "",
		Description:  "填写 system unit 名称(systemctl show xx)(system_unit)",
	},
	{
		KeyName:      "cgroup",
		DefaultNoUse: false,
		ChooseOnly:   false,
		Default:      "",
		Description:  "填写 cgroup name/path(cgroup)",
	},
}

func (p *Procstat) Name() string {
	return TypeMetricProcstat
}

func (p *Procstat) Usages() string {
	return MetricProcstatUsage
}

func (p *Procstat) Config() map[string]interface{} {
	opts := ConfigProcUsages
	for _, opt := range opts {
		if v, ok := p.config[opt.KeyName]; ok {
			opt.Default = v
		}
	}
	config := map[string]interface{}{
		metric.OptionString:     opts,
		metric.AttributesString: KeyProcUsages,
	}
	return config
}

func (p *Procstat) SyncConfig(config map[string]interface{}, meta *reader.Meta) error {
	osInfo := utilsos.GetOSInfo()
	p.config = config
	p.MemRelated = getBoolOr(config, MemRelated, true)
	p.IoRelated = getBoolOr(config, IoRelated, true)
	p.CtxSwitRelated = getBool(config, CtxSwitRelated)
	p.ThreadsRelated = getBool(config, ThreadsRelated)
	p.FileDescRelated = getBool(config, FileDescRelated)
	p.CpuTimeRelated = getBool(config, CpuTimeRelated)
	p.CpuUsageRelated = getBoolOr(config, CpuUsageRelated, true)
	p.ResourceLimits = getBool(config, ResourceLimits)

	p.CpuTop10 = getBoolOr(config, CpuTop10, true)
	p.MemTop10 = getBoolOr(config, MemTop10, true)
	p.Supervisord = getBoolOr(config, Supervisord, true)
	p.DaemonTools = getBool(config, DaemonTools)

	p.PidFile = getString(config, PidFile)
	p.Exe = getString(config, Exe)
	p.Pattern = getString(config, Pattern)
	p.User = getString(config, User)
	p.SystemdUnit = getString(config, SystemdUnit)
	p.CGroup = getString(config, CGroup)

	p.kernel = osInfo.Kernel
	return nil
}

func (p *Procstat) Tags() []string {
	return []string{KeyProcstatProcessName, KeyProcstatPid}
}

func (p *Procstat) Collect() (datas []map[string]interface{}, err error) {
	datas = make([]map[string]interface{}, 0)
	if p.createPIDFinder == nil {
		p.createPIDFinder = defaultPIDFinder
	}
	if p.createProcess == nil {
		p.createProcess = defaultProcess
	}

	procs, err := p.updateProcesses(p.procs)
	if err != nil {
		return
	}
	p.procs = procs

	for _, proc := range p.procs {
		data := p.collectMetrics(proc)
		datas = append(datas, data)
	}
	return
}

func (p *Procstat) collectMetrics(proc ProcessInfo) map[string]interface{} {
	fields := map[string]interface{}{}
	if name, err := proc.Name(); err == nil {
		fields[KeyProcstatProcessName] = name
	}
	fields[KeyProcstatStatus] = proc.Status
	if proc.Process != nil {
		fields[KeyProcstatPid] = int32(proc.PID())
	} else {
		pid := int32(proc.Pid)
		if pid != 0 {
			fields[KeyProcstatPid] = pid
		}
	}

	if p.ThreadsRelated && proc.Process != nil {
		if threadsNum, err := proc.NumThreads(); err == nil {
			fields[KeyProcstatThreadsNum] = threadsNum
		}
	}

	if p.FileDescRelated && proc.Process != nil {
		if fds, err := proc.NumFDs(); err == nil {
			fields[KeyProcstatFdsNum] = fds
		}
	}

	if p.CtxSwitRelated && proc.Process != nil {
		if ctx, err := proc.NumCtxSwitches(); err == nil {
			fields[KeyProcstatVolConSwitches] = ctx.Voluntary
			fields[KeyProcstatInVolConSwitches] = ctx.Involuntary
		}
	}

	if p.IoRelated && proc.Process != nil {
		if io, err := proc.IOCounters(); err == nil {
			fields[KeyProcstatReadCount] = io.ReadCount
			fields[KeyProcstatWriteCount] = io.WriteCount
			fields[KeyProcstatReadBytes] = io.ReadBytes
			fields[KeyProcstatWriteBytes] = io.WriteBytes
		}
	}

	if p.CpuTimeRelated && proc.Process != nil {
		if cpuTime, err := proc.Times(); err == nil {
			fields[KeyProcstatCpuTimeUser] = cpuTime.User
			fields[KeyProcstatCpuTimeSystem] = cpuTime.System
			fields[KeyProcstatCpuTimeIdle] = cpuTime.Idle
			fields[KeyProcstatCpuTimeNice] = cpuTime.Nice
			fields[KeyProcstatCpuTimeIoWait] = cpuTime.Iowait
			fields[KeyProcstatCpuTimeIrq] = cpuTime.Irq
			fields[KeyProcstatCpuTimeSoftirq] = cpuTime.Softirq
			fields[KeyProcstatCpuTimeSteal] = cpuTime.Steal
			fields[KeyProcstatCpuTimeStolen] = cpuTime.Stolen
			fields[KeyProcstatCpuTimeGuest] = cpuTime.Guest
			fields[KeyProcstatCpuTimeGuestNice] = cpuTime.GuestNice
		}
	}

	if p.CpuUsageRelated && proc.Process != nil {
		if cpuPerc, err := proc.Percent(time.Duration(0)); err == nil {
			fields[KeyProcstatCpuUsage] = cpuPerc
		}
	}

	if p.MemRelated && proc.Process != nil {
		if mem, err := proc.MemoryInfo(); err == nil {
			fields[KeyProcstatMemRss] = mem.RSS
			fields[KeyProcstatMemVms] = mem.VMS
			fields[KeyProcstatMemSwap] = mem.Swap
			fields[KeyProcstatMemData] = mem.Data
			fields[KeyProcstatMemStack] = mem.Stack
			fields[KeyProcstatMemLocked] = mem.Locked
		}
	}

	if p.ResourceLimits && proc.Process != nil {
		if rlims, err := proc.RlimitUsage(true); err == nil {
			for _, rlim := range rlims {
				var name, nameSoft, nameHard string
				switch rlim.Resource {
				case process.RLIMIT_CPU:
					name = KeyProcstatCpuTime
					nameSoft = KeyProcstatRlimitCpuTimeSoft
					nameHard = KeyProcstatRlimitCpuTimeHard
				case process.RLIMIT_DATA:
					name = KeyProcstatMemData
					nameSoft = KeyProcstatRlimitMemDataSoft
					nameHard = KeyProcstatRlimitMemDataHard
				case process.RLIMIT_STACK:
					name = KeyProcstatMemStack
					nameSoft = KeyProcstatRlimitMemStackSoft
					nameHard = KeyProcstatRlimitMemStackHard
				case process.RLIMIT_RSS:
					name = KeyProcstatMemRss
					nameSoft = KeyProcstatRlimitMemRssSoft
					nameHard = KeyProcstatRlimitMemRssHard
				case process.RLIMIT_NOFILE:
					name = KeyProcstatFdsNum
					nameSoft = KeyProcstatRlimitFdsNumSoft
					nameHard = KeyProcstatRlimitFdsNumHard
				case process.RLIMIT_MEMLOCK:
					name = KeyProcstatMemLocked
					nameSoft = KeyProcstatRlimitMemLockedSoft
					nameHard = KeyProcstatRlimitMemLockedHard
				case process.RLIMIT_AS:
					name = KeyProcstatMemVms
					nameSoft = KeyProcstatRlimitMemVmsSoft
					nameHard = KeyProcstatRlimitMemVmsHard
				case process.RLIMIT_LOCKS:
					name = KeyProcstatFileLocks
					nameSoft = KeyProcstatRlimitFileLocksSoft
					nameHard = KeyProcstatRlimitFileLocksHard
				case process.RLIMIT_SIGPENDING:
					name = KeyProcstatSignalsPending
					nameSoft = KeyProcstatRlimitSignalsPendingSoft
					nameHard = KeyProcstatRlimitSignalsPendingHard
				case process.RLIMIT_NICE:
					name = KeyProcstatNicePriority
					nameSoft = KeyProcstatRlimitNicePrioritySoft
					nameHard = KeyProcstatRlimitNicePriorityHard
				case process.RLIMIT_RTPRIO:
					name = KeyProcstatRealtimePriority
					nameSoft = KeyProcstatRlimitRealtimePrioritySoft
					nameHard = KeyProcstatRlimitRealtimePriorityHard
				default:
					continue
				}

				fields[nameSoft] = rlim.Soft
				fields[nameHard] = rlim.Hard
				if name != KeyProcstatFileLocks { // gopsutil doesn't currently track the used file locks count
					fields[name] = rlim.Used
				}
			}
		}
	}
	return fields
}

func (p *Procstat) updateProcesses(prevInfo map[string]ProcessInfo) (map[string]ProcessInfo, error) {
	processInfos, err := p.findPids()
	if err != nil && len(processInfos) == 0 {
		return nil, err
	}
	procs := make(map[string]ProcessInfo, len(prevInfo))
	for name, processInfo := range processInfos {
		info, ok := prevInfo[name]
		if ok {
			procs[name] = info
		} else {
			if processInfo.Pid != 0 && processInfo.Status == 0 {
				if proc, err := p.createProcess(processInfo.Pid); err == nil {
					processInfo.Process = proc
					if processInfo.name == "" {
						processInfo.name, _ = proc.Name()
					}
					procs[processInfo.name] = processInfo
				}
			} else {
				procs[name] = ProcessInfo{name: name, Status: processInfo.Status, Pid: processInfo.Pid}
			}
		}
	}
	return procs, nil
}

func (p *Procstat) getPIDFinder() (PIDFinder, error) {
	if p.pidFinder == nil {
		f, err := p.createPIDFinder()
		if err != nil {
			return nil, err
		}
		p.pidFinder = f
	}
	return p.pidFinder, nil
}

func (p *Procstat) findPids() (map[string]ProcessInfo, error) {
	var ps []PID
	process := make(map[string]ProcessInfo)
	var err error

	f, err := p.getPIDFinder()
	if err != nil {
		return nil, err
	}

	if p.PidFile != "" {
		if ps, err = f.PidFile(p.PidFile); err != nil {
			log.Warnf("get pidfile %v error %v", p.PidFile, err)
		} else {
			pid2ProcessInfo(process, ps)
		}
	}
	if p.Exe != "" {
		if ps, err = f.Pattern(p.Exe); err != nil {
			log.Warnf("get pids by exec 'pgrep %v' error %v", p.Exe, err)
		} else {
			pid2ProcessInfo(process, ps)
		}
	}
	if p.Pattern != "" {
		if ps, err = f.FullPattern(p.Pattern); err != nil {
			log.Warnf("get pids by exec 'pgrep -f %v error %v", p.Pattern, err)
		} else {
			pid2ProcessInfo(process, ps)
		}
	}
	if p.User != "" {
		if ps, err = f.Uid(p.User); err != nil {
			log.Warnf("get pids by exec 'pgrep -u %v error %v", p.User, err)
		} else {
			pid2ProcessInfo(process, ps)
		}
	}
	if p.SystemdUnit != "" {
		if err = p.systemdUnitPIDs(process); err != nil {
			log.Warnf("get pids by exec 'systemctl show %v error %v", p.SystemdUnit, err)
		}
	}
	if p.CGroup != "" {
		if err = p.cgroupPIDs(process); err != nil {
			log.Warnf("cgroup: %s get pids by cgroup error %v", p.CGroup, err)
		}
	}
	if p.CpuTop10 {
		if err = p.PCpuTop10(process); err != nil {
			log.Warnf("get pids of cpu usage top 10 error %v", err)
		}
	}
	if p.MemTop10 {
		if err = p.PMemTop10(process); err != nil {
			log.Warnf("get pids of mem usage top 10 error %v", err)
		}
	}
	if p.Supervisord {
		if err = p.SupervisordStat(process); err != nil {
			log.Warnf("get pids of supervisord managed process error %v", err)
		}
	}
	if p.DaemonTools {
		if err = p.childProcess(process); err != nil {
			log.Warnf("get pids of daemontools managed process error %v", err)
		}
	}
	return process, err
}

func pid2ProcessInfo(process map[string]ProcessInfo, pids []PID) {
	for _, pid := range pids {
		processInfo := ProcessInfo{
			Pid: pid,
		}
		status, err := getProcStat(int32(pid))
		if err != nil {
			log.Errorf("get process %d status failed: %v", pid, err)
			processInfo.Status = 6
		} else {
			processInfo.Status = status
		}
		pidStr := strconv.FormatInt(int64(pid), 10)
		process[pidStr] = processInfo
	}
	return
}

func getProcStat(pid int32) (stat int, err error) {
	pidStr := fmt.Sprintf("%d", pid)
	stdout, err := exec.Command("bash", "-c", "ps -ax | grep "+pidStr+" | grep -v grep").Output()
	if err != nil {
		return 6, fmt.Errorf("exec command 'ps -ax | grep %s' status error [%v]: %s", pidStr, err, string(stdout))
	}
	if len(string(stdout)) == 0 {
		return 4, nil
	}
	for _, str := range strings.Split(string(stdout), "\n") {
		fields := strings.Fields(str)
		if len(fields) < 4 {
			continue
		}
		if fields[0] != pidStr {
			continue
		}
		return 0, nil
	}
	return 4, nil
}

func (p *Procstat) systemdUnitPIDs(process map[string]ProcessInfo) error {
	cmd := ExecCommand("systemctl", "show", p.SystemdUnit)
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	for _, line := range bytes.Split(out, []byte{'\n'}) {
		kv := bytes.SplitN(line, []byte{'='}, 2)
		if len(kv) != 2 {
			continue
		}
		if !bytes.Equal(kv[0], []byte("MainPID")) {
			continue
		}
		if len(kv[1]) == 0 {
			return nil
		}
		pid, err := strconv.Atoi(string(kv[1]))
		if err != nil {
			return fmt.Errorf("invalid pid '%s'", kv[1])
		}
		process[string(kv[1])] = ProcessInfo{
			Pid: PID(pid),
		}
	}
	return nil
}

func (p *Procstat) cgroupPIDs(process map[string]ProcessInfo) error {
	procsPath := p.CGroup
	if procsPath[0] != '/' {
		procsPath = "/sys/fs/cgroup/" + procsPath
	}
	procsPath = filepath.Join(procsPath, "cgroup.procs")
	out, err := ioutil.ReadFile(procsPath)
	if err != nil {
		return err
	}
	for _, pidBS := range bytes.Split(out, []byte{'\n'}) {
		if len(pidBS) == 0 {
			continue
		}
		pid, err := strconv.Atoi(string(pidBS))
		if err != nil {
			return fmt.Errorf("invalid pid '%s'", pidBS)
		}
		process[string(pidBS)] = ProcessInfo{
			Pid: PID(pid),
		}
	}

	return nil
}

func (p *Procstat) PCpuTop10(process map[string]ProcessInfo) (err error) {
	var comm string
	if p.kernel == GoOSMac {
		comm = "ps x -o pid=,command= -r | head -n 10"
	} else if p.kernel == GoOSLinux {
		comm = "ps -Ao pid=,cmd= --sort=-pcpu | head -n 10"
	} else {
		log.Warnf("not support kernel %v, ignored it", p.kernel)
		return
	}
	err = runCommandIdName(process, comm)
	return
}

func (p *Procstat) PMemTop10(process map[string]ProcessInfo) (err error) {
	var comm string
	if p.kernel == GoOSMac {
		comm = "ps x -o pid=,command= -m | head -n 10"
	} else if p.kernel == GoOSLinux {
		comm = "ps -Ao pid=,cmd= --sort=-pmem | head -n 10"
	} else {
		log.Warnf("not support kernel %v, ignored it", p.kernel)
		return
	}
	return runCommandIdName(process, comm)
}

func (p *Procstat) SupervisordStat(process map[string]ProcessInfo) (err error) {
	stdout, err := exec.Command("bash", "-c", "supervisorctl status").Output()
	if err != nil {
		return fmt.Errorf("exec command supervisorctl status error [%v]: %s", err, string(stdout))
	}
	for _, str := range strings.Split(string(stdout), "\n") {
		fields := strings.Fields(str)
		if len(fields) < 2 {
			continue
		}
		info := ProcessInfo{
			name: fields[0],
		}
		info.Status, _ = processStat[fields[1]]

		if len(fields) < 3 || fields[2] != "pid" {
			process[fields[0]] = info
			continue
		}

		pidStr := strings.Trim(fields[3], ",")
		if pid, err := strconv.Atoi(pidStr); err != nil {
			log.Debugf("parse %s of %s ERROR: %v", strings.Trim(fields[3], ","), str, err)
			pidStr = fields[0]
		} else {
			info.Pid = PID(pid)
		}
		process[pidStr] = info
	}
	return
}

func (p *Procstat) childProcess(process map[string]ProcessInfo) (err error) {
	comm := `ps -ef | grep daemontools | grep -v 'grep ' | awk '{print $2}'|xargs ps -o pid= --ppid`
	return runCommand(process, comm)
}

func runCommandIdName(process map[string]ProcessInfo, comm string) (err error) {
	out, err := exec.Command("sh", "-c", comm).Output()
	if err != nil {
		return fmt.Errorf("exec command %v error [%v]: %s", comm, err, string(out))
	}
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			return err
		}

		info := ProcessInfo{
			Pid:  PID(pid),
			name: strings.Join(fields[1:], " "),
		}
		process[fields[0]] = info
	}
	return nil
}

func runCommand(process map[string]ProcessInfo, comm string) (err error) {
	out, err := exec.Command("bash", "-c", comm).Output()
	if err != nil {
		return fmt.Errorf("exec command %v error [%v]: %s", comm, err, string(out))
	}
	var pids []PID
	if pids, err = ParseOutput(string(out)); err != nil {
		return
	}

	var pidStr string
	for _, pid := range pids {
		pidStr = strconv.FormatInt(int64(pid), 32)
		process[pidStr] = ProcessInfo{
			Pid: pid,
		}
	}
	return
}

func getBool(config map[string]interface{}, key string) bool {
	if v, ok := config[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func getString(config map[string]interface{}, key string) string {
	if v, ok := config[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return ""
}

func getBoolOr(config map[string]interface{}, key string, dv bool) bool {
	if v, ok := config[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return dv
}

func NewCreator() metric.Collector {
	osInfo := utilsos.GetOSInfo()
	return &Procstat{
		config:          map[string]interface{}{},
		MemRelated:      true,
		IoRelated:       true,
		CpuUsageRelated: true,
		MemTop10:        true,
		CpuTop10:        true,
		Supervisord:     true,
		kernel:          osInfo.Kernel,
	}
}

func init() {
	metric.Add(TypeMetricProcstat, NewCreator)
}
