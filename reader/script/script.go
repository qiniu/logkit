package script

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.Reader       = &Reader{}
)

func init() {
	reader.RegisterConstructor(reader.ModeScript, NewReader)
}

type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32
	/*
		Note: 原子操作，用于表示获取数据的线程运行状态

		- StatusInit: 当前没有任务在执行
		- StatusRunning: 当前有任务正在执行
		- StatusStopping: 数据管道已经由上层关闭，执行中的任务完成时直接退出无需再处理
	*/
	routineStatus int32

	stopChan chan struct{}
	readChan chan []byte
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	realpath   string // 处理文件路径
	originpath string
	scripttype string

	isLoop       bool
	loopDuration time.Duration
	execOnStart  bool
	Cron         *cron.Cron //定时任务
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	path, _ := conf.GetStringOr(reader.KeyLogPath, "")
	originPath := path

	var err error
	path, err = checkPath(meta, path)
	if err != nil {
		return nil, err
	}

	cronSchedule, _ := conf.GetStringOr(reader.KeyScriptCron, "")
	execOnStart, _ := conf.GetBoolOr(reader.KeyScriptExecOnStart, true)
	scriptType, _ := conf.GetStringOr(reader.KeyExecInterpreter, "bash")
	r := &Reader{
		meta:          meta,
		status:        reader.StatusInit,
		routineStatus: reader.StatusInit,
		stopChan:      make(chan struct{}),
		readChan:      make(chan []byte),
		errChan:       make(chan error),
		originpath:    originPath,
		realpath:      path,
		scripttype:    scriptType,
		execOnStart:   execOnStart,
		Cron:          cron.New(),
	}

	// 定时任务配置串
	if len(cronSchedule) > 0 {
		cronSchedule = strings.ToLower(cronSchedule)
		if strings.HasPrefix(cronSchedule, reader.Loop) {
			r.isLoop = true
			r.loopDuration, err = reader.ParseLoopDuration(cronSchedule)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", r.meta.RunnerName, r.Name(), err)
			}
			if r.loopDuration.Nanoseconds() <= 0 {
				r.loopDuration = 1 * time.Second
			}
		} else {
			err = r.Cron.AddFunc(cronSchedule, r.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron job added with schedule <%v>", r.meta.RunnerName, r.Name(), cronSchedule)
		}
	}
	return r, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopped
}

func (r *Reader) Name() string {
	return "ScriptFile: " + r.originpath
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("script reader does not support read mode")
}

func (r *Reader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	if r.isLoop {
		go func() {
			ticker := time.NewTicker(r.loopDuration)
			defer ticker.Stop()
			for {
				r.run()

				select {
				case <-r.stopChan:
					atomic.StoreInt32(&r.status, reader.StatusStopped)
					log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
					return
				case <-ticker.C:
				}
			}
		}()

	} else {
		if r.execOnStart {
			go r.run()
		}
		r.Cron.Start()
	}
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return r.originpath
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case data := <-r.readChan:
		return string(data), nil
	case err := <-r.errChan:
		return "", err
	case <-timer.C:
	}

	return "", nil
}

func (r *Reader) Status() StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

func (r *Reader) SyncMeta() {}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	r.Cron.Stop()

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
}

func (r *Reader) run() {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			errMsg := fmt.Sprintf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
			log.Error(errMsg)
			if !r.isLoop {
				// 通知上层 Cron 执行间隔可能过短或任务执行时间过长
				r.sendError(errors.New(errMsg))
			}
		}
		return
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			if atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusRunning, reader.StatusStopping) {
				close(r.readChan)
				close(r.errChan)
			}
			return
		}
		atomic.StoreInt32(&r.routineStatus, reader.StatusInit)
	}()

	// 如果执行失败，最多重试 10 次
	for i := 1; i <= 10; i++ {
		// 判断上层是否已经关闭，先判断 routineStatus 再判断 status 可以保证同时只有一个 r.run 会运行到此处
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, task is interrupted", r.meta.RunnerName, r.Name())
			return
		}

		err := r.exec()
		if err == nil {
			log.Infof("Runner[%v] %q task has been successfully executed", r.meta.RunnerName, r.Name())
			return
		}

		log.Errorf("Runner[%v] %q task execution failed: %v ", r.meta.RunnerName, r.Name(), err)
		r.setStatsError(err.Error())
		r.sendError(err)

		if r.isLoop {
			return // 循环执行的任务上层逻辑已经等同重试
		}
		time.Sleep(3 * time.Second)
	}
	log.Errorf("Runner[%v] %q task execution failed and gave up after 10 tries", r.meta.RunnerName, r.Name())
}

func (r *Reader) exec() error {
	res, err := exec.Command(r.scripttype, r.realpath).Output()
	if err != nil {
		return err
	}
	r.readChan <- res
	return nil
}

func checkPath(meta *reader.Meta, path string) (string, error) {
	for {
		realPath, fileInfo, err := GetRealPath(path)
		if err != nil {
			log.Warnf("Runner[%v] %s - utils.GetRealPath failed, err:%v", meta.RunnerName, path, err)
			time.Sleep(1 * time.Minute)
			continue
		}

		if fileInfo == nil {
			log.Warnf("Runner[%v] %s - utils.GetRealPath file info nil ", meta.RunnerName, path)
			time.Sleep(1 * time.Minute)
			continue
		}

		fileMode := fileInfo.Mode()
		if !fileMode.IsRegular() {
			err = fmt.Errorf("Runner[%v] %s - file failed, err: file is not regular ", meta.RunnerName, path)
			return "", err
		}

		err = CheckFileMode(realPath, fileMode)
		if err != nil {
			err = fmt.Errorf("Runner[%v] %s - file failed, err: %v ", meta.RunnerName, path, err)
			return "", err
		}

		return realPath, nil
	}
}
