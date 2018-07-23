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

func init() {
	reader.RegisterConstructor(reader.ModeScript, NewReader)
}

type Reader struct {
	realpath   string // 处理文件路径
	originpath string
	scripttype string

	Cron *cron.Cron //定时任务

	readChan chan []byte
	errChan  chan error

	meta *reader.Meta

	status  int32
	mux     sync.Mutex
	started bool

	execOnStart  bool
	loop         bool
	loopDuration time.Duration

	stats     StatsInfo
	statsLock sync.RWMutex
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (sr reader.Reader, err error) {
	path, _ := conf.GetStringOr(reader.KeyLogPath, "")
	originPath := path

	path, err = checkPath(meta, path)
	if err != nil {
		return nil, err
	}

	cronSchedule, _ := conf.GetStringOr(reader.KeyScriptCron, "")
	execOnStart, _ := conf.GetBoolOr(reader.KeyScriptExecOnStart, true)
	scriptType, _ := conf.GetStringOr(reader.KeyExecInterpreter, "bash")
	ssr := &Reader{
		originpath:  originPath,
		realpath:    path,
		scripttype:  scriptType,
		Cron:        cron.New(),
		readChan:    make(chan []byte),
		errChan:     make(chan error),
		meta:        meta,
		status:      reader.StatusInit,
		mux:         sync.Mutex{},
		started:     false,
		execOnStart: execOnStart,
		statsLock:   sync.RWMutex{},
	}

	//schedule    string     //定时任务配置串
	if len(cronSchedule) > 0 {
		cronSchedule = strings.ToLower(cronSchedule)
		if strings.HasPrefix(cronSchedule, reader.Loop) {
			ssr.loop = true
			ssr.loopDuration, err = reader.ParseLoopDuration(cronSchedule)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", ssr.meta.RunnerName, ssr.Name(), err)
				err = nil
			}
		} else {
			err = ssr.Cron.AddFunc(cronSchedule, ssr.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron job added with schedule <%v>", ssr.meta.RunnerName, ssr.Name(), cronSchedule)
		}
	}
	return ssr, nil
}

func (sr *Reader) ReadLine() (data string, err error) {
	if !sr.started {
		sr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-sr.readChan:
		data = string(dat)
	case err = <-sr.errChan:
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (s *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %s panic, recovered from %v", s.Name(), rec)
		}
	}()
	s.errChan <- err
}

//Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
func (sr *Reader) Start() {
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

func (sr *Reader) Name() string {
	return "ScriptFile:" + sr.originpath
}

func (sr *Reader) Source() string {
	return sr.originpath
}

func (sr *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("ScriptReader not support readmode")
}

func (sr *Reader) SyncMeta() {}

func (sr *Reader) Close() (err error) {
	sr.Cron.Stop()
	if atomic.CompareAndSwapInt32(&sr.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] %v stopping", sr.meta.RunnerName, sr.Name())
	} else {
		close(sr.readChan)
		close(sr.errChan)
	}
	return
}

func (sr *Reader) LoopRun() {
	for {
		if atomic.LoadInt32(&sr.status) == reader.StatusStopped {
			return
		}
		//run 函数里面处理stopping的逻辑
		sr.run()
		time.Sleep(sr.loopDuration)
	}
}

func (sr *Reader) run() {
	var err error
	// 防止并发run
	for {
		if atomic.LoadInt32(&sr.status) == reader.StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&sr.status, reader.StatusInit, reader.StatusRunning) {
			break
		}
	}

	// running时退出 状态改为Init，以便 cron 调度下次运行
	// stopping时推出改为 stopped，不再运行
	defer func() {
		atomic.CompareAndSwapInt32(&sr.status, reader.StatusRunning, reader.StatusInit)
		if atomic.CompareAndSwapInt32(&sr.status, reader.StatusStopping, reader.StatusStopped) {
			close(sr.readChan)
			close(sr.errChan)
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", sr.meta.RunnerName, sr.Name())
		}
	}()

	// 开始work逻辑
	for {
		if atomic.LoadInt32(&sr.status) == reader.StatusStopping {
			log.Warnf("Runner[%v] %v stopped from running", sr.meta.RunnerName, sr.Name())
			return
		}
		err = sr.exec()
		if err == nil {
			log.Infof("Runner[%v] %v successfully exec", sr.meta.RunnerName, sr.Name())
			return
		}
		log.Errorf("Runner[%v] %v execute script error [%v]", sr.meta.RunnerName, sr.Name(), err)
		sr.setStatsError(err.Error())
		sr.sendError(err)
		time.Sleep(3 * time.Second)
	}
}

func (sr *Reader) exec() (err error) {
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

func (sr *Reader) setStatsError(err string) {
	sr.statsLock.Lock()
	defer sr.statsLock.Unlock()
	sr.stats.LastError = err
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
