package system

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/shirou/gopsutil/process"
)

const (
	TypeMetricProcesses  = "processes"
	MetricProcessesUsage = "系统进程(processes)"

	// TypeMetricProcesses 信息中的字段
	KeyProcessesBlocked      = "processes_blocked"
	KeyProcessesZombies      = "processes_zombies"
	KeyProcessesStopped      = "processes_stopped"
	KeyProcessesRunning      = "processes_running"
	KeyProcessesSleeping     = "processes_sleeping"
	KeyProcessesTotal        = "processes_total"
	KeyProcessesUnknown      = "processes_unknown"
	KeyProcessesIdle         = "processes_idle"
	KeyProcessesWait         = "processes_wait"
	KeyProcessesDead         = "processes_dead"
	KeyProcessesPaging       = "processes_paging"
	KeyProcessesTotalThreads = "processes_total_threads"
)

// KeyProcessesUsages TypeMetricProcesses 的字段名称
var KeyProcessesUsages = []KeyValue{
	{KeyProcessesBlocked, "不可中断的睡眠状态下的进程数('U','D','L')"},
	{KeyProcessesZombies, "僵尸态进程数('Z')"},
	{KeyProcessesStopped, "暂停状态进程数('T')"},
	{KeyProcessesRunning, "运行中的进程数('R')"},
	{KeyProcessesSleeping, "可中断进程数('S')"},
	{KeyProcessesTotal, "总进程数"},
	{KeyProcessesIdle, "挂起的空闲进程数('I')"},
	{KeyProcessesWait, "等待中的进程数('W')"},
	{KeyProcessesDead, "回收中的进程数('X')"},
	{KeyProcessesPaging, "等待中的进程数('W')"},
	{KeyProcessesTotalThreads, "总线程数"},
	{KeyProcessesUnknown, "未知状态进程数"},
}

type Processes struct {
	execPS       func() ([]byte, error)
	readProcFile func(filename string) ([]byte, error)

	forcePS   bool
	forceProc bool
}

func (p *Processes) Name() string {
	return TypeMetricProcesses
}

func (p *Processes) Usages() string {
	return MetricProcessesUsage
}

func (_ *Processes) Tags() []string {
	return []string{}
}

func (_ *Processes) Config() map[string]interface{} {
	config := map[string]interface{}{
		metric.OptionString:     []Option{},
		metric.AttributesString: KeyProcessesUsages,
	}
	return config
}

func (p *Processes) Collect() (datas []map[string]interface{}, err error) {
	// Get an empty map of metric fields
	fields := getEmptyFields()

	// Collect windows process info
	if runtime.GOOS == "windows" {
		if err := p.getWinStat(fields); err != nil {
			return nil, fmt.Errorf("collect windows processes error: %v", err.Error())
		}
		return append(datas, fields), nil
	}

	// Decide if we will use 'ps' to get stats (use procfs otherwise)
	usePS := true
	if runtime.GOOS == "linux" {
		usePS = false
	}
	if p.forcePS {
		usePS = true
	} else if p.forceProc {
		usePS = false
	}

	// Gather stats from 'ps' or procfs
	if usePS {
		if err := p.gatherFromPS(fields); err != nil {
			return nil, err
		}
	} else {
		if err := p.gatherFromProc(fields); err != nil {
			return nil, err
		}
	}
	datas = append(datas, fields)
	return
}

// Gets empty fields of metrics based on the OS
func getEmptyFields() map[string]interface{} {
	fields := map[string]interface{}{
		KeyProcessesBlocked:  int64(0),
		KeyProcessesZombies:  int64(0),
		KeyProcessesStopped:  int64(0),
		KeyProcessesRunning:  int64(0),
		KeyProcessesSleeping: int64(0),
		KeyProcessesTotal:    int64(0),
		KeyProcessesUnknown:  int64(0),
	}
	switch runtime.GOOS {
	case "freebsd":
		fields[KeyProcessesIdle] = int64(0)
		fields[KeyProcessesWait] = int64(0)
	case "darwin":
		fields[KeyProcessesIdle] = int64(0)
	case "openbsd":
		fields[KeyProcessesIdle] = int64(0)
	case "linux":
		fields[KeyProcessesDead] = int64(0)
		fields[KeyProcessesPaging] = int64(0)
		fields[KeyProcessesTotalThreads] = int64(0)
	case "windows":
		fields[KeyProcessesTotalThreads] = int64(0)
	}
	return fields
}

// exec `ps` to get all process states
func (p *Processes) gatherFromPS(fields map[string]interface{}) error {
	out, err := p.execPS()
	if err != nil {
		return err
	}

	for i, status := range bytes.Fields(out) {
		if i == 0 && string(status) == "STAT" {
			// This is a header, skip it
			continue
		}
		switch status[0] {
		case 'W':
			fields[KeyProcessesWait] = fields[KeyProcessesWait].(int64) + int64(1)
		case 'U', 'D', 'L':
			// Also known as uninterruptible sleep or disk sleep
			fields[KeyProcessesBlocked] = fields[KeyProcessesBlocked].(int64) + int64(1)
		case 'Z':
			fields[KeyProcessesZombies] = fields[KeyProcessesZombies].(int64) + int64(1)
		case 'X':
			fields[KeyProcessesDead] = fields[KeyProcessesDead].(int64) + int64(1)
		case 'T':
			fields[KeyProcessesStopped] = fields[KeyProcessesStopped].(int64) + int64(1)
		case 'R':
			fields[KeyProcessesRunning] = fields[KeyProcessesRunning].(int64) + int64(1)
		case 'S':
			fields[KeyProcessesSleeping] = fields[KeyProcessesSleeping].(int64) + int64(1)
		case 'I':
			fields[KeyProcessesIdle] = fields[KeyProcessesIdle].(int64) + int64(1)
		case '?':
			fields[KeyProcessesUnknown] = fields[KeyProcessesUnknown].(int64) + int64(1)
		default:
			log.Printf("I! processes: Unknown state [ %s ] from ps",
				string(status[0]))
		}
		fields[KeyProcessesTotal] = fields[KeyProcessesTotal].(int64) + int64(1)
	}
	return nil
}

// get process states from /proc/(pid)/stat files
func (p *Processes) gatherFromProc(fields map[string]interface{}) error {
	filenames, err := filepath.Glob("/proc/[0-9]*/stat")
	if err != nil {
		return err
	}

	for _, filename := range filenames {
		_, err := os.Stat(filename)

		data, err := p.readProcFile(filename)
		if err != nil {
			return err
		}
		if data == nil {
			continue
		}

		// Parse out data after (<cmd name>)
		i := bytes.LastIndex(data, []byte(")"))
		if i == -1 {
			continue
		}
		data = data[i+2:]

		stats := bytes.Fields(data)
		if len(stats) < 3 {
			return fmt.Errorf("Something is terribly wrong with %s", filename)
		}
		switch stats[0][0] {
		case 'R':
			fields[KeyProcessesRunning] = fields[KeyProcessesRunning].(int64) + int64(1)
		case 'S':
			fields[KeyProcessesSleeping] = fields[KeyProcessesSleeping].(int64) + int64(1)
		case 'D':
			fields[KeyProcessesBlocked] = fields[KeyProcessesBlocked].(int64) + int64(1)
		case 'Z':
			fields[KeyProcessesZombies] = fields[KeyProcessesZombies].(int64) + int64(1)
		case 'X':
			fields[KeyProcessesDead] = fields[KeyProcessesDead].(int64) + int64(1)
		case 'T', 't':
			fields[KeyProcessesStopped] = fields[KeyProcessesStopped].(int64) + int64(1)
		case 'W':
			fields[KeyProcessesPaging] = fields[KeyProcessesPaging].(int64) + int64(1)
		default:
			log.Printf("I! processes: Unknown state [ %s ] in file %s",
				string(stats[0][0]), filename)
		}
		fields[KeyProcessesTotal] = fields[KeyProcessesTotal].(int64) + int64(1)

		threads, err := strconv.Atoi(string(stats[17]))
		if err != nil {
			log.Printf("I! processes: Error parsing thread count: %s", err)
			continue
		}
		fields[KeyProcessesTotalThreads] = fields[KeyProcessesTotalThreads].(int64) + int64(threads)
	}
	return nil
}

//  For windows, get all process states
func (p *Processes) getWinStat(fields map[string]interface{}) error {
	pids, err := process.Pids()
	if err != nil {
		return fmt.Errorf("Get all processes pids failed, error: %v", err.Error())
	}
	// total processes
	fields[KeyProcessesTotal] = int64(len(pids))
	for _, pid := range pids {
		threads := int32(0)
		p, _ := process.NewProcess(pid)
		if threads, err = p.NumThreads(); err != nil {
			log.Errorf("Get process threads failed, error: %v", err.Error())
			continue
		}
		fields[KeyProcessesTotalThreads] = fields[KeyProcessesTotalThreads].(int64) + int64(threads)

	}
	return nil
}

func readProcFile(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		// Reading from /proc/<PID> fails with ESRCH if the process has
		// been terminated between open() and read().
		if perr, ok := err.(*os.PathError); ok && perr.Err == syscall.ESRCH {
			return nil, nil
		}

		return nil, err
	}

	return data, nil
}

func execPS() ([]byte, error) {
	bin, err := exec.LookPath("ps")
	if err != nil {
		return nil, err
	}

	out, err := exec.Command(bin, "axo", "state").Output()
	if err != nil {
		return nil, err
	}

	return out, err
}

func init() {
	metric.Add(TypeMetricProcesses, func() metric.Collector {
		return &Processes{
			execPS:       execPS,
			readProcFile: readProcFile,
		}
	})
}
