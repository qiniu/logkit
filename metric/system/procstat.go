package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/metric/system/utils"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/utils/models"

	"github.com/shirou/gopsutil/process"
)

const (
	TypeMetricProcstat  = "procstat"
	MetricProcstatUsage = "详细进程信息(procstat)"

	// TypeMetricProcstat 信息中的字段
	KeyProcstatProcessName      = "procstat_process_name"
	KeyProcstatPid              = "procstat_pid"
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
	GoosMac     = "Darwin"
	GoosLinux   = "Linux"
	GoosWindows = "windows"
)

var (
	defaultPIDFinder = NewPgrep
	defaultProcess   = NewProc
)

type Procstat struct {
	PidTag      bool
	Prefix      string
	ProcessName string

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
	procs           map[PID]Process
	createProcess   func(PID) (Process, error)
}

// KeyProcUsages TypeMetricProc 中的字段名称
var KeyProcUsages = []models.KeyValue{}

// ConfigProcUsages TypeMetricProc 配置项的描述
var ConfigProcUsages = []models.Option{
	{
		KeyName:       "cpu_usage_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集Cpu用量相关的信息(cpu_usage_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "mem_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集内存相关的信息(mem_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "cpu_time_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集CPU时间相关的信息(cpu_time_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "io_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "true",
		Description:   "收集IO相关的信息(io_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "threads_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集线程数量(threads_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "context_switch_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集内核上下文切换次数(context_switch_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "file_descriptor_related",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集文件描述相关信息(file_descriptor_related)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "resource_limits",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集资源限制相关的信息(resource_limits)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "cpu_top_10",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集CPU利用率前10的进程(cpu_top_10)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "mem_top_10",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集内存占用前10的进程(mem_top_10)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "supervisord",
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Description:   "收集supervisord管理的进程(supervisord)",
		Type:          metric.ConfigTypeBool,
	},
	{
		KeyName:       "daemon_tools",
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
	config := map[string]interface{}{
		metric.OptionString:     ConfigProcUsages,
		metric.AttributesString: KeyProcUsages,
	}
	return config
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

func (p *Procstat) collectMetrics(proc Process) map[string]interface{} {
	fields := map[string]interface{}{}
	if name, err := proc.Name(); err == nil {
		fields[KeyProcstatProcessName] = name
	}
	fields[KeyProcstatPid] = int32(proc.PID())

	if p.ThreadsRelated {
		if threadsNum, err := proc.NumThreads(); err == nil {
			fields[KeyProcstatThreadsNum] = threadsNum
		}
	}

	if p.FileDescRelated {
		if fds, err := proc.NumFDs(); err == nil {
			fields[KeyProcstatFdsNum] = fds
		}
	}

	if p.CtxSwitRelated {
		if ctx, err := proc.NumCtxSwitches(); err == nil {
			fields[KeyProcstatVolConSwitches] = ctx.Voluntary
			fields[KeyProcstatInVolConSwitches] = ctx.Involuntary
		}
	}

	if p.IoRelated {
		if io, err := proc.IOCounters(); err == nil {
			fields[KeyProcstatReadCount] = io.ReadCount
			fields[KeyProcstatWriteCount] = io.WriteCount
			fields[KeyProcstatReadBytes] = io.ReadBytes
			fields[KeyProcstatWriteBytes] = io.WriteBytes
		}
	}

	if p.CpuTimeRelated {
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

	if p.CpuUsageRelated {
		if cpuPerc, err := proc.Percent(time.Duration(0)); err == nil {
			fields[KeyProcstatCpuUsage] = cpuPerc
		}
	}

	if p.MemRelated {
		if mem, err := proc.MemoryInfo(); err == nil {
			fields[KeyProcstatMemRss] = mem.RSS
			fields[KeyProcstatMemVms] = mem.VMS
			fields[KeyProcstatMemSwap] = mem.Swap
			fields[KeyProcstatMemData] = mem.Data
			fields[KeyProcstatMemStack] = mem.Stack
			fields[KeyProcstatMemLocked] = mem.Locked
		}
	}

	if p.ResourceLimits {
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

func (p *Procstat) updateProcesses(prevInfo map[PID]Process) (map[PID]Process, error) {
	pids, err := p.findPids()
	if err != nil {
		return nil, err
	}
	procs := make(map[PID]Process, len(prevInfo))
	for _, pid := range pids {
		info, ok := prevInfo[pid]
		if ok {
			procs[pid] = info
		} else {
			if proc, err := p.createProcess(pid); err == nil {
				procs[pid] = proc
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

func (p *Procstat) findPids() ([]PID, error) {
	var ps []PID
	var pids []PID
	var err error

	f, err := p.getPIDFinder()
	if err != nil {
		return nil, err
	}

	if p.PidFile != "" {
		if ps, err = f.PidFile(p.PidFile); err != nil {
			log.Warnf("get pidfile %v error %v", p.PidFile, err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.Exe != "" {
		if ps, err = f.Pattern(p.Exe); err != nil {
			log.Warnf("get pids by exec 'pgrep %v' error %v", p.Exe, err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.Pattern != "" {
		if ps, err = f.FullPattern(p.Pattern); err != nil {
			log.Warnf("get pids by exec 'pgrep -f %v error %v", p.Pattern, err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.User != "" {
		if ps, err = f.Uid(p.User); err != nil {
			log.Warnf("get pids by exec 'pgrep -u %v error %v", p.User, err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.SystemdUnit != "" {
		if ps, err = p.systemdUnitPIDs(); err != nil {
			log.Warnf("get pids by exec 'systemctl show %v error %v", p.SystemdUnit, err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.CGroup != "" {
		if ps, err = p.cgroupPIDs(); err != nil {
			log.Warnf("get pids by cgroup error %v", p.CGroup, err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.CpuTop10 {
		if ps, err = p.PCpuTop10(); err != nil {
			log.Warnf("get pids of cpu usage top 10 error %v", err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.MemTop10 {
		if ps, err = p.PMemTop10(); err != nil {
			log.Warnf("get pids of mem usage top 10 error %v", err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.Supervisord {
		if ps, err = p.childProcess("supervisord"); err != nil {
			log.Warnf("get pids of supervisord managed process error %v", err)
		} else {
			pids = append(pids, ps...)
		}
	}
	if p.DaemonTools {
		if ps, err = p.childProcess("daemontools"); err != nil {
			log.Warnf("get pids of daemontools managed process error %v", err)
		} else {
			pids = append(pids, ps...)
		}
	}
	return pids, err
}

func (p *Procstat) systemdUnitPIDs() ([]PID, error) {
	var pids []PID
	cmd := ExecCommand("systemctl", "show", p.SystemdUnit)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
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
			return nil, nil
		}
		pid, err := strconv.Atoi(string(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid pid '%s'", kv[1])
		}
		pids = append(pids, PID(pid))
	}
	return pids, nil
}

func (p *Procstat) cgroupPIDs() ([]PID, error) {
	var pids []PID

	procsPath := p.CGroup
	if procsPath[0] != '/' {
		procsPath = "/sys/fs/cgroup/" + procsPath
	}
	procsPath = filepath.Join(procsPath, "cgroup.procs")
	out, err := ioutil.ReadFile(procsPath)
	if err != nil {
		return nil, err
	}
	for _, pidBS := range bytes.Split(out, []byte{'\n'}) {
		if len(pidBS) == 0 {
			continue
		}
		pid, err := strconv.Atoi(string(pidBS))
		if err != nil {
			return nil, fmt.Errorf("invalid pid '%s'", pidBS)
		}
		pids = append(pids, PID(pid))
	}

	return pids, nil
}

func (p *Procstat) PCpuTop10() (pids []PID, err error) {
	var comm string
	if p.kernel == GoosMac {
		comm = "ps x -o pid= -r | head -n 10"
	} else if p.kernel == GoosLinux {
		comm = "ps -Ao pid= --sort=-pcpu | head -n 10"
	} else {
		log.Warnf("not support kernel %v, ignored it", p.kernel)
		return
	}
	pids, err = runCommand(comm)
	return
}

func (p *Procstat) PMemTop10() (pids []PID, err error) {
	var comm string
	if p.kernel == GoosMac {
		comm = "ps x -o pid= -m | head -n 10"
	} else if p.kernel == GoosLinux {
		comm = "ps -Ao pid= --sort=-pmem | head -n 10"
	} else {
		log.Warnf("not support kernel %v, ignored it", p.kernel)
		return
	}
	return runCommand(comm)
}

func (p *Procstat) childProcess(name string) (pids []PID, err error) {
	comm := `ps -ef | grep ` + name + ` | grep -v 'grep ' | awk '{print $2}'|xargs ps -o pid= --ppid`
	return runCommand(comm)
}

func runCommand(comm string) (pids []PID, err error) {
	out, err := exec.Command("bash", "-c", comm).Output()
	if err != nil {
		return pids, fmt.Errorf("exec command %v error %v", comm, err)
	}
	return ParseOutput(string(out))
}

func init() {
	metric.Add(TypeMetricProcstat, func() metric.Collector {
		osInfo := utils.GetOSInfo()
		return &Procstat{
			IoRelated:       true,
			MemRelated:      true,
			CpuUsageRelated: true,
			kernel:          osInfo.Kernel,
		}
	})
}
