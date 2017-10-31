package mgr

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
)

const (
	KeyMetricType = "type"
)

const (
	defaultCollectInterval = "3s"
)

type MetricRunner struct {
	RunnerName string `json:"name"`

	collectors []metric.Collector
	senders    []sender.Sender

	collectInterval time.Duration
	rs              RunnerStatus
	lastRs          RunnerStatus
	rsMutex         *sync.RWMutex
	meta            *reader.Meta
	lastSend        time.Time
	stopped         int32
	exitChan        chan struct{}
}

func NewMetric(tp string) (metric.Collector, error) {
	if c, ok := metric.Collectors[tp]; ok {
		return c(), nil
	}
	return nil, fmt.Errorf("Metric <%v> is not support now", tp)
}

func NewMetricRunner(rc RunnerConfig, sr *sender.SenderRegistry) (runner *MetricRunner, err error) {
	if rc.CollectInterval == "" {
		rc.CollectInterval = defaultCollectInterval
	}
	interval, err := time.ParseDuration(rc.CollectInterval)
	if err != nil {
		return
	}
	meta, err := reader.NewMetaWithConf(conf.MapConf{
		reader.KeyRunnerName: rc.RunnerName,
		reader.KeyMode:       reader.ModeMetrics,
	})
	if err != nil {
		return nil, fmt.Errorf("Runner "+rc.RunnerName+" add failed, err is %v", err)
	}
	collectors := make([]metric.Collector, 0)
	for _, c := range rc.Metric {
		tp, err := c.GetString(KeyMetricType)
		if err != nil {
			return nil, err
		}
		c, err := NewMetric(tp)
		if err != nil {
			log.Errorf("%v ignore it...", err)
			err = nil
			continue
		}
		collectors = append(collectors, c)
	}
	if len(collectors) < 1 {
		err = errors.New("no collectors were added")
		return
	}
	senders := make([]sender.Sender, 0)
	for _, c := range rc.SenderConfig {
		s, err := sr.NewSender(c, meta.FtSaveLogPath())
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
	}
	runner = &MetricRunner{
		RunnerName: rc.RunnerName,
		exitChan:   make(chan struct{}),
		lastSend:   time.Now(), // 上一次发送时间
		meta:       meta,
		rs: RunnerStatus{
			ReaderStats: utils.StatsInfo{},
			SenderStats: make(map[string]utils.StatsInfo),
			lastState:   time.Now(),
			Name:        rc.RunnerName,
		},
		lastRs: RunnerStatus{
			ReaderStats: utils.StatsInfo{},
			SenderStats: make(map[string]utils.StatsInfo),
			lastState:   time.Now(),
			Name:        rc.RunnerName,
		},
		rsMutex:         new(sync.RWMutex),
		collectInterval: interval,
		collectors:      collectors,
		senders:         senders,
	}
	runner.StatusRestore()
	return
}

func (mr *MetricRunner) Name() string {
	return mr.RunnerName
}

func (r *MetricRunner) Run() {
	defer close(r.exitChan)
	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			log.Debugf("runner %v exited from run", r.RunnerName)
			r.exitChan <- struct{}{}
			return
		}
		dataCnt := 0
		datas := make([]sender.Data, 0)
		// collect data
		for _, c := range r.collectors {
			tmpdatas, err := c.Collect()
			if err != nil {
				log.Errorf("collecter <%v> collect data error: %v", c.Name(), err)
				continue
			}
			for i := range tmpdatas {
				newdata := sender.Data{}
				for k, v := range tmpdatas[i] {
					newdata[c.Name()+"_"+k] = v
				}
				if len(newdata) > 0 {
					datas = append(datas, newdata)
				}
			}
			if len(tmpdatas) < 1 {
				log.Debugf("MetricRunner %v collect No data", c.Name())
			}
			dataCnt += len(tmpdatas)
		}
		r.rsMutex.Lock()
		r.rs.ReadDataCount += int64(dataCnt)
		r.rsMutex.Unlock()
		r.lastSend = time.Now()
		if len(datas) <= 0 {
			log.Debug("MetricRunner collect No data")
			continue
		}

		for _, s := range r.senders {
			if !r.trySend(s, datas, 3) {
				log.Errorf("failed to send metricData: << %v >>", datas)
				break
			}
		}
		time.Sleep(r.collectInterval)
	}
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (r *MetricRunner) trySend(s sender.Sender, datas []sender.Data, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	if _, ok := r.rs.SenderStats[s.Name()]; !ok {
		r.rs.SenderStats[s.Name()] = utils.StatsInfo{}
	}
	r.rsMutex.RLock()
	info := r.rs.SenderStats[s.Name()]
	r.rsMutex.RUnlock()
	cnt := 1
	for {
		// 至少尝试一次。如果任务已经停止，那么只尝试一次
		if cnt > 1 && atomic.LoadInt32(&r.stopped) > 0 {
			return false
		}
		err := s.Send(datas)
		if se, ok := err.(*utils.StatsError); ok {
			err = se.ErrorDetail
			if se.Ft {
				info.Errors = se.Errors
				info.Success = se.Success
				r.rs.Lag.Ftlags = se.Ftlag
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
	r.rsMutex.Lock()
	r.rs.SenderStats[s.Name()] = info
	r.rsMutex.Unlock()
	return true
}

func (mr *MetricRunner) Stop() {
	atomic.AddInt32(&mr.stopped, 1)

	log.Warnf("wait for MetricRunner " + mr.Name() + " stopped")
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-mr.exitChan:
		log.Warnf("MetricRunner " + mr.Name() + " has been stopped ")
	case <-timer.C:
		log.Warnf("MetricRunner " + mr.Name() + " exited timeout ")
	}
	for _, s := range mr.senders {
		err := s.Close()
		if err != nil {
			log.Errorf("cannot close sender name: %s, err: %v", s.Name(), err)
		} else {
			log.Warnf("sender %v of MetricRunner %v closed", s.Name(), mr.Name())
		}
	}
}

func (mr *MetricRunner) Reset() error {
	var errMsg string
	err := mr.meta.Reset()
	if err != nil {
		errMsg += err.Error() + "\n"
	}
	for _, sd := range mr.senders {
		ssd, ok := sd.(Resetable)
		if ok {
			if nerr := ssd.Reset(); nerr != nil {
				errMsg += err.Error() + "\n"
			}
		}
	}
	return errors.New(errMsg)
}

func (_ *MetricRunner) Cleaner() CleanInfo {
	return CleanInfo{
		enable: false,
	}
}

func (mr *MetricRunner) Status() RunnerStatus {
	mr.rsMutex.Lock()
	defer mr.rsMutex.Unlock()
	durationTime := float64(mr.collectInterval.Seconds())
	mr.rs.ReadSpeed = float64(mr.rs.ReadDataCount-mr.lastRs.ReadDataCount) / durationTime
	mr.rs.ReadSpeedTrend = getTrend(mr.lastRs.ReadSpeed, mr.rs.ReadSpeed)

	for i := range mr.senders {
		sts, ok := mr.senders[i].(sender.StatsSender)
		if ok {
			mr.rs.SenderStats[mr.senders[i].Name()] = sts.Stats()
		}
	}

	for k, v := range mr.rs.SenderStats {
		if lv, ok := mr.lastRs.SenderStats[k]; ok {
			v.Speed, v.Trend = calcSpeedTrend(lv, v, durationTime)
		} else {
			v.Speed, v.Trend = calcSpeedTrend(utils.StatsInfo{}, v, durationTime)
		}
		mr.rs.SenderStats[k] = v
	}
	copyRunnerStatus(&mr.lastRs, &mr.rs)
	return mr.rs
}

func (mr *MetricRunner) StatusRestore() {
	rStat, err := mr.meta.ReadStatistic()

	if err != nil {
		log.Warnf("runner %v, restore status failed", mr.RunnerName)
		return
	}
	mr.rs.ReadDataCount = rStat.ReaderCnt
	mr.rs.ParserStats.Success = rStat.ParserCnt[0]
	mr.rs.ParserStats.Errors = rStat.ParserCnt[1]
	for _, s := range mr.senders {
		name := s.Name()
		info, exist := rStat.SenderCnt[name]
		if !exist {
			continue
		}
		sStatus, ok := s.(sender.StatsSender)
		if ok {
			sStatus.Restore(&utils.StatsInfo{
				Success: info[0],
				Errors:  info[1],
			})
		}
		status, ext := mr.rs.SenderStats[name]
		if !ext {
			status = utils.StatsInfo{}
		}
		status.Success = info[0]
		status.Errors = info[1]
		mr.rs.SenderStats[name] = status
	}
	copyRunnerStatus(&mr.lastRs, &mr.rs)
	log.Infof("runner %v restore status %v", mr.RunnerName, rStat)
}

func (mr *MetricRunner) StatusBackup() {
	status := mr.Status()
	bStart := &reader.Statistic{
		ReaderCnt: status.ReadDataCount,
		ParserCnt: [2]int64{
			status.ParserStats.Success,
			status.ParserStats.Errors,
		},
		SenderCnt: map[string][2]int64{},
	}
	for _, s := range mr.senders {
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
	err := mr.meta.WriteStatistic(bStart)
	if err != nil {
		log.Warnf("runner %v, backup status failed", mr.RunnerName)
	} else {
		log.Infof("runner %v, backup status %v", mr.RunnerName, bStart)
	}
}
