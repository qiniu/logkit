package mgr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/plugin"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/json-iterator/go"
)

type PluginConfig struct {
	PluginType string `json:"type"`
	//Cycle	   		 int					`json:"cycle"`
	//BatchCount 	 int  					`json:"batchCount"`
	LogPath string                 `json:"log_path"`
	Config  map[string]interface{} `json:"config,omitempty"`
}

const DefaultBatchCount = 10

type PluginRunner struct {
	RunnerName       string `json:"name"`
	PluginType       string
	transformers     []transforms.Transformer
	meta             *reader.Meta
	rs               *RunnerStatus
	lastRs           *RunnerStatus
	rsMutex          *sync.RWMutex
	lastSend         time.Time
	stopped          int32
	exitChan         chan struct{}
	exitSuccessChan  chan struct{}
	senders          []sender.Sender
	Ticker           *time.Ticker
	BatchCount       int
	MaxBatchInterval int
	Cycle            int
	PluginConfig     string
	logPath          string
}

func NewPluginRunner(rc RunnerConfig, sr *sender.SenderRegistry) (runner *PluginRunner, err error) {
	if plugin.Conf.Enabled == false {
		return nil, fmt.Errorf("Runner " + rc.RunnerName + " add failed, err is plugin runner is not allowed")
	}
	//meta
	cf := conf.MapConf{
		GlobalKeyName:  rc.RunnerName,
		KeyRunnerName:  rc.RunnerName,
		reader.KeyMode: reader.ModeMetrics,
	}
	if rc.ExtraInfo {
		cf[ExtraInfo] = Bool2String(rc.ExtraInfo)
	}
	meta, err := reader.NewMetaWithConf(cf)
	if err != nil {
		return nil, fmt.Errorf("Runner "+rc.RunnerName+" add failed, err is %v", err)
	}

	//plugin
	plugin.Lock.RLock()
	p := plugin.Plugins[rc.PluginConfig.PluginType]
	if p == nil {
		return nil, fmt.Errorf("no such type of %v plugin", rc.PluginConfig.PluginType)
	}
	if rc.CollectInterval <= 0 {
		rc.CollectInterval = p.DefaultCycle
	}
	ticker := time.NewTicker(time.Duration(rc.CollectInterval) * time.Second)
	if rc.MaxBatchLen <= 0 {
		rc.MaxBatchLen = DefaultBatchCount
	}
	if rc.MaxBatchInterval <= 0 {
		rc.MaxBatchInterval = defaultSendIntervalSeconds
	}
	/*	if rc.MaxBatchLen > rc.MaxBatchInterval/rc.CollectInterval {
		rc .MaxBatchLen = rc.MaxBatchInterval/rc.CollectInterval
	}*/
	confBytes, err := jsoniter.MarshalIndent(rc.PluginConfig.Config, "", "    ")
	if err != nil {
		return nil, fmt.Errorf("plugin config %v marshal failed, err is %v", rc.PluginConfig.Config, err)
	}
	pluginConfigDir := filepath.Join(p.Path, p.ConfDir)
	plugin.Lock.RUnlock()
	pluginConfigFile := rc.RunnerName + ".conf"
	if _, err := os.Stat(pluginConfigDir); err != nil {
		if os.IsNotExist(err) {
			if err = os.Mkdir(pluginConfigDir, 0755); err != nil && !os.IsExist(err) {
				return nil, fmt.Errorf("plugin config dir not exists and make dir failed, err is %v", err)
			}
		}
	}
	pluginConfig := filepath.Join(pluginConfigDir, pluginConfigFile)
	if err := ioutil.WriteFile(pluginConfig, confBytes, 0644); err != nil {
		return nil, err
	}
	//transformer
	transformers := createTransformers(rc)
	//sender
	for i := range rc.SenderConfig {
		rc.SenderConfig[i][KeyRunnerName] = rc.RunnerName
	}
	senders := make([]sender.Sender, 0)
	for _, c := range rc.SenderConfig {
		s, err := sr.NewSender(c, meta.FtSaveLogPath())
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
	}

	runner = &PluginRunner{
		RunnerName:      rc.RunnerName,
		exitChan:        make(chan struct{}),
		exitSuccessChan: make(chan struct{}),
		lastSend:        time.Now(), // 上一次发送时间
		meta:            meta,
		rs: &RunnerStatus{
			ReaderStats:   StatsInfo{},
			SenderStats:   make(map[string]StatsInfo),
			lastState:     time.Now(),
			Name:          rc.RunnerName,
			RunningStatus: RunnerRunning,
		},
		lastRs: &RunnerStatus{
			ReaderStats:   StatsInfo{},
			SenderStats:   make(map[string]StatsInfo),
			lastState:     time.Now(),
			Name:          rc.RunnerName,
			RunningStatus: RunnerRunning,
		},
		rsMutex:          new(sync.RWMutex),
		Ticker:           ticker,
		Cycle:            rc.CollectInterval,
		PluginType:       rc.PluginConfig.PluginType,
		BatchCount:       rc.MaxBatchLen,
		MaxBatchInterval: rc.MaxBatchInterval,
		PluginConfig:     pluginConfig,
		logPath:          rc.PluginConfig.LogPath,
		transformers:     transformers,
		senders:          senders,
	}
	runner.StatusRestore()

	return
}

func (pr *PluginRunner) Name() string {
	return pr.RunnerName
}

func (pr *PluginRunner) Run() {
	pr.lastSend = time.Now()
	defer close(pr.exitSuccessChan)
	datas := make([]Data, 0)
	for {
		select {
		case <-pr.exitChan:
			if len(datas) > 0 {
				pr.batchProcess(datas)
			}
			pr.exitSuccessChan <- struct{}{}
			return
		case <-pr.Ticker.C:
			plugin.Lock.RLock()
			p := plugin.Plugins[pr.PluginType]
			if p == nil {
				log.Errorf("plugin %v running err, no such type plugin", pr.PluginType)
				continue
			}
			resDatas, err := plugin.PluginRun(p, pr.PluginConfig, pr.logPath, pr.Cycle)
			plugin.Lock.RUnlock()
			if err != nil {
				log.Error(err)
				continue
			}
			datas = append(datas, resDatas...)
			pr.rs.ReadDataCount += int64(len(resDatas))
			if len(datas) >= pr.BatchCount || time.Now().Sub(pr.lastSend).Seconds() >= float64(pr.MaxBatchInterval) {
				pr.batchProcess(datas)
				datas = make([]Data, 0)
			}
		}
	}
}

//批处理
func (pr *PluginRunner) batchProcess(datas []Data) {
	var err error
	for i := range pr.transformers {
		if pr.transformers[i].Stage() == transforms.StageAfterParser {
			datas, err = pr.transformers[i].Transform(datas)
			if err != nil {
				log.Error(err)
			}
		}
	}
	for _, s := range pr.senders {
		if !pr.trySend(s, datas, 3) {
			log.Errorf("failed to send metricData: << %v >>", datas)
		}
	}
	pr.lastSend = time.Now()
}

func (pr *PluginRunner) trySend(s sender.Sender, datas []Data, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	if _, ok := pr.rs.SenderStats[s.Name()]; !ok {
		pr.rs.SenderStats[s.Name()] = StatsInfo{}
	}
	pr.rsMutex.RLock()
	info := pr.rs.SenderStats[s.Name()]
	pr.rsMutex.RUnlock()
	cnt := 1
	for {
		// 至少尝试一次。如果任务已经停止，那么只尝试一次
		if cnt > 1 && atomic.LoadInt32(&pr.stopped) > 0 {
			return false
		}
		err := s.Send(datas)
		if se, ok := err.(*StatsError); ok {
			err = se.ErrorDetail
			if se.Ft {
				pr.rs.Lag.Ftlags = se.FtQueueLag
			} else {
				if cnt > 1 {
					info.Errors -= se.Success
				} else {
					info.Errors += se.Errors
				}
				info.Success += se.Success
			}
		} else if err != nil {
			if cnt <= 1 {
				info.Errors += int64(len(datas))
			}
		} else {
			info.Success += int64(len(datas))
		}
		if err != nil {
			log.Error(err)
			time.Sleep(time.Second)
			if times <= 0 || cnt < times {
				cnt++
				continue
			}
			log.Errorf("retry send %v times, but still error %v, discard datas %v ... total %v lines", cnt, err, datas[0], len(datas))
		}
		break
	}
	pr.rsMutex.Lock()
	pr.rs.SenderStats[s.Name()] = info
	pr.rsMutex.Unlock()
	return true
}

func (pr *PluginRunner) Stop() {
	defer close(pr.exitChan)
	atomic.AddInt32(&pr.stopped, 1)
	pr.exitChan <- struct{}{}
	log.Warnf("wait for PluginRunner " + pr.Name() + " stopped")
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-pr.exitSuccessChan:
		log.Warnf("PluginRunner " + pr.Name() + " has been stopped ")
	case <-timer.C:
		log.Warnf("PluginRunner " + pr.Name() + " exited timeout ")
	}
	for _, s := range pr.senders {
		err := s.Close()
		if err != nil {
			log.Errorf("cannot close sender name: %s, err: %v", s.Name(), err)
		} else {
			log.Warnf("sender %v of PluginRunner %v closed", s.Name(), pr.Name())
		}
	}
	//停止runner,删除配置文件
	os.Remove(pr.PluginConfig)
}

func (pr *PluginRunner) Reset() error {
	var errMsg string
	err := pr.meta.Reset()
	if err != nil {
		errMsg += err.Error() + "\n"
	}
	for _, sd := range pr.senders {
		ssd, ok := sd.(Resetable)
		if ok {
			if nerr := ssd.Reset(); nerr != nil {
				errMsg += err.Error() + "\n"
			}
		}
	}
	if errMsg != "" {
		err = errors.New(errMsg)
	}
	return err
}

func (_ *PluginRunner) Cleaner() CleanInfo {
	return CleanInfo{
		enable: false,
	}
}

func (pr *PluginRunner) getStatusFrequently(now time.Time) (bool, float64) {
	pr.rsMutex.RLock()
	defer pr.rsMutex.RUnlock()
	elaspedTime := now.Sub(pr.rs.lastState).Seconds()
	if elaspedTime <= 3 {
		return true, elaspedTime
	}
	return false, elaspedTime
}

func (pr *PluginRunner) Status() RunnerStatus {
	var isFre bool
	var elaspedtime float64
	now := time.Now()
	if isFre, elaspedtime = pr.getStatusFrequently(now); isFre {
		return *pr.lastRs
	}
	pr.rsMutex.Lock()
	defer pr.rsMutex.Unlock()
	pr.rs.Elaspedtime += elaspedtime
	pr.rs.lastState = now
	//durationTime := float64(pr.Cycle)
	pr.rs.ReadSpeed = float64(pr.rs.ReadDataCount-pr.lastRs.ReadDataCount) / elaspedtime
	pr.rs.ReadSpeedTrend = getTrend(pr.lastRs.ReadSpeed, pr.rs.ReadSpeed)

	for i := range pr.senders {
		sts, ok := pr.senders[i].(sender.StatsSender)
		if ok {
			pr.rs.SenderStats[pr.senders[i].Name()] = sts.Stats()
		}
	}

	for k, v := range pr.rs.SenderStats {
		if lv, ok := pr.lastRs.SenderStats[k]; ok {
			v.Speed, v.Trend = calcSpeedTrend(lv, v, elaspedtime)
		} else {
			v.Speed, v.Trend = calcSpeedTrend(StatsInfo{}, v, elaspedtime)
		}
		pr.rs.SenderStats[k] = v
	}
	pr.rs.RunningStatus = RunnerRunning
	*pr.lastRs = pr.rs.Clone()
	return *pr.lastRs
}

func (pr *PluginRunner) StatusRestore() {
	rStat, err := pr.meta.ReadStatistic()

	if err != nil {
		log.Warnf("runner %v, restore status failed", pr.RunnerName)
		return
	}
	pr.rs.ReadDataCount = rStat.ReaderCnt
	pr.rs.ParserStats.Success = rStat.ParserCnt[0]
	pr.rs.ParserStats.Errors = rStat.ParserCnt[1]
	for _, s := range pr.senders {
		name := s.Name()
		info, exist := rStat.SenderCnt[name]
		if !exist {
			continue
		}
		sStatus, ok := s.(sender.StatsSender)
		if ok {
			sStatus.Restore(&StatsInfo{
				Success: info[0],
				Errors:  info[1],
			})
		}
		status, ext := pr.rs.SenderStats[name]
		if !ext {
			status = StatsInfo{}
		}
		status.Success = info[0]
		status.Errors = info[1]
		pr.rs.SenderStats[name] = status
	}
	*pr.lastRs = pr.rs.Clone()
	log.Infof("runner %v restore status %v", pr.RunnerName, rStat)
}

func (pr *PluginRunner) StatusBackup() {
	status := pr.Status()
	bStart := &reader.Statistic{
		ReaderCnt: status.ReadDataCount,
		ParserCnt: [2]int64{
			status.ParserStats.Success,
			status.ParserStats.Errors,
		},
		SenderCnt: map[string][2]int64{},
	}
	for _, s := range pr.senders {
		name := s.Name()
		sStatus, ok := s.(sender.StatsSender)
		if ok {
			status.SenderStats[name] = sStatus.Stats()
		}
		if sta, exist := status.SenderStats[name]; exist {
			bStart.SenderCnt[name] = [2]int64{
				sta.Success,
				sta.Errors,
			}
		}
	}
	err := pr.meta.WriteStatistic(bStart)
	if err != nil {
		log.Warnf("runner %v, backup status failed", pr.RunnerName)
	} else {
		log.Infof("runner %v, backup status %v", pr.RunnerName, bStart)
	}
}
