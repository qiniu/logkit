package reader

import (
	"errors"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"

	"github.com/robfig/cron"
)

type ScriptReader struct {
	realpath   string // 处理文件路径
	originpath string
	scripttype string

	Cron *cron.Cron //定时任务

	readChan chan []byte

	meta *Meta

	status  int32
	mux     sync.Mutex
	started bool

	execOnStart  bool
	loop         bool
	loopDuration time.Duration

	stats     utils.StatsInfo
	statsLock sync.RWMutex
}

func NewScriptReader(meta *Meta, conf conf.MapConf) (sr *ScriptReader, err error) {
	path, _ := conf.GetStringOr(KeyLogPath, "")
	originPath := path

	for {
		path, err = checkPath(meta, path)
		if err != nil {
			continue
		}
		break
	}

	cronSchedule, _ := conf.GetStringOr(KeyScriptCron, "")
	execOnStart, _ := conf.GetBoolOr(KeyScriptExecOnStart, true)
	scriptType, _ := conf.GetStringOr(KeyExecInterpreter, "bash")
	sr = &ScriptReader{
		originpath:  originPath,
		realpath:    path,
		scripttype:  scriptType,
		Cron:        cron.New(),
		readChan:    make(chan []byte),
		meta:        meta,
		status:      StatusInit,
		mux:         sync.Mutex{},
		started:     false,
		execOnStart: execOnStart,
		statsLock:   sync.RWMutex{},
	}

	//schedule    string     //定时任务配置串
	if len(cronSchedule) > 0 {
		cronSchedule = strings.ToLower(cronSchedule)
		if strings.HasPrefix(cronSchedule, Loop) {
			sr.loop = true
			sr.loopDuration, err = parseLoopDuration(cronSchedule)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", sr.meta.RunnerName, sr.Name(), err)
				err = nil
			}
		} else {
			err = sr.Cron.AddFunc(cronSchedule, sr.run)
			if err != nil {
				return
			}
			log.Infof("Runner[%v] %v Cron job added with schedule <%v>", sr.meta.RunnerName, sr.Name(), cronSchedule)
		}
	}
	return sr, nil
}

func (sr *ScriptReader) ReadLine() (data string, err error) {
	if !sr.started {
		sr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-sr.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

//Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
func (sr *ScriptReader) Start() {
	sr.mux.Lock()
	defer sr.mux.Unlock()
	if sr.started {
		return
	}
	if sr.loop {
		go sr.LoopRun()
	} else {
		sr.Cron.Start()
		if sr.execOnStart {
			go sr.run()
		}
	}
	sr.started = true
	log.Infof("Runner[%v] %v pull data deamon started", sr.meta.RunnerName, sr.Name())
}

func (sr *ScriptReader) Name() string {
	return "ScriptFile:" + sr.originpath
}

func (sr *ScriptReader) Source() string {
	return sr.originpath
}

func (sr *ScriptReader) SetMode(mode string, v interface{}) error {
	return errors.New("ScriptReader not support readmode")
}

func (sr *ScriptReader) SyncMeta() {}

func (sr *ScriptReader) Close() (err error) {
	sr.Cron.Stop()
	if atomic.CompareAndSwapInt32(&sr.status, StatusRunning, StatusStopping) {
		log.Infof("Runner[%v] %v stopping", sr.meta.RunnerName, sr.Name())
	} else {
		close(sr.readChan)
	}
	return
}

func (sr *ScriptReader) LoopRun() {
	for {
		if atomic.LoadInt32(&sr.status) == StatusStopped {
			return
		}
		//run 函数里面处理stopping的逻辑
		sr.run()
		time.Sleep(sr.loopDuration)
	}
}

func (sr *ScriptReader) run() {
	var err error
	// 防止并发run
	for {
		if atomic.LoadInt32(&sr.status) == StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&sr.status, StatusInit, StatusRunning) {
			break
		}
	}

	// running时退出 状态改为Init，以便 cron 调度下次运行
	// stopping时推出改为 stopped，不再运行
	defer func() {
		atomic.CompareAndSwapInt32(&sr.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&sr.status, StatusStopping, StatusStopped) {
			close(sr.readChan)
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", sr.meta.RunnerName, sr.Name())
		}
	}()

	// 开始work逻辑
	for {
		if atomic.LoadInt32(&sr.status) == StatusStopping {
			log.Warnf("Runner[%v] %v stopped from running", sr.meta.RunnerName, sr.Name())
			return
		}
		err = sr.exec()
		if err == nil {
			log.Infof("Runner[%v] %v successfully exec", sr.meta.RunnerName, sr.Name())
			return
		}
		log.Error("Runner[%v] %v execute script error [%v]", sr.meta.RunnerName, sr.Name(), err)
		sr.setStatsError(err.Error())
		time.Sleep(3 * time.Second)
	}
}

func (sr *ScriptReader) exec() (err error) {
	sr.mux.Lock()
	defer sr.mux.Unlock()
	command := exec.Command(sr.scripttype, sr.realpath) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return err
	}
	sr.readChan <- res
	return nil
}

func (sr *ScriptReader) setStatsError(err string) {
	sr.statsLock.Lock()
	defer sr.statsLock.Unlock()
	sr.stats.LastError = err
}

func checkPath(meta *Meta, path string) (string, error) {
	for {
		realPath, fileInfo, err := utils.GetRealPath(path)
		if err != nil || fileInfo == nil {
			log.Warnf("Runner[%v] %s - utils.GetRealPath failed, err:%v", meta.RunnerName, path, err)
			time.Sleep(time.Minute)
		}

		fileMode := fileInfo.Mode()
		if !fileMode.IsRegular() {
			log.Warnf("Runner[%v] %s - file failed, err: file is not regular", meta.RunnerName, path)
			time.Sleep(time.Minute)
			continue
		}
		utils.CheckFileMode(realPath, fileMode)
		return realPath, nil
	}
}
