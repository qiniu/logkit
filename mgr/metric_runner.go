package mgr

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/metric/curl"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/sender"
	senderConf "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	KeyMetricType = "type"
)

const (
	defaultCollectInterval = 30
)

type MetricConfig struct {
	MetricType string                 `json:"type"`
	Attributes map[string]bool        `json:"attributes"`
	Config     map[string]interface{} `json:"config"`
}

var (
	_ Deleteable = &MetricRunner{}
)

type MetricRunner struct {
	RunnerName string `json:"name"`
	envTag     string

	collectors   []metric.Collector
	senders      []sender.Sender
	transformers map[string][]transforms.Transformer
	commonTrans  []transforms.Transformer

	collectInterval time.Duration
	rs              *RunnerStatus
	lastRs          *RunnerStatus
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
	return nil, fmt.Errorf("metric <%v> is not support now", tp)
}

func NewMetricRunner(rc RunnerConfig, sr *sender.Registry) (runner *MetricRunner, err error) {
	if rc.CollectInterval <= 0 {
		rc.CollectInterval = defaultCollectInterval
	}
	if len(rc.MetricConfig) == 0 {
		return nil, errors.New("Runner " + rc.RunnerName + " has no metric, ignore it")
	}
	interval := time.Duration(rc.CollectInterval) * time.Second
	cf := conf.MapConf{
		GlobalKeyName: rc.RunnerName,
		KeyRunnerName: rc.RunnerName,
		KeyMode:       reader.ModeMetrics,
	}
	if rc.ExtraInfo {
		cf[ExtraInfo] = Bool2String(rc.ExtraInfo)
	}
	meta, err := reader.NewMetaWithConf(cf)
	if err != nil {
		return nil, fmt.Errorf("Runner "+rc.RunnerName+" add failed, err is %v", err)
	}
	for i := range rc.SendersConfig {
		rc.SendersConfig[i][KeyRunnerName] = rc.RunnerName
	}
	collectors := make([]metric.Collector, 0)
	transformers := make(map[string][]transforms.Transformer)

	for _, m := range rc.MetricConfig {
		tp := m.MetricType
		c, err := NewMetric(tp)
		if err != nil {
			log.Errorf("%v ignore it...", err)
			continue
		}
		// sync config to ExtCollector
		ec, ok := c.(metric.ExtCollector)
		if ok {
			if err := ec.SyncConfig(m.Config, meta); err != nil {
				return nil, fmt.Errorf("metric %v sync config error %v", tp, err)
			}
		} else {
			// sync config to buildin Collector
			configBytes, err := jsoniter.Marshal(m.Config)
			if err != nil {
				return nil, fmt.Errorf("metric %v marshal config error %v", tp, err)
			}
			err = jsoniter.Unmarshal(configBytes, c)
			if err != nil {
				return nil, fmt.Errorf("metric %v unmarshal config error %v", tp, err)
			}
		}

		collectors = append(collectors, c)

		// 配置文件中明确标明 false 的 attr 加入 discard transformer
		config := c.Config()
		metricName := c.Name()
		trans := make([]transforms.Transformer, 0)
		if attributes, ex := config[metric.AttributesString]; ex {
			if attrs, ok := attributes.(KeyValueSlice); ok {
				for _, attr := range attrs {
					val, exist := m.Attributes[attr.Key]
					if exist && !val {
						if m.MetricType == curl.TypeMetricHttp {
							var httpDataArr []curl.HttpDataReq
							if _, ok := m.Config["http_datas"]; !ok {
								return nil, fmt.Errorf("metric %v http_datas can't be empty", curl.TypeMetricHttp)
							}
							httpData, ok := m.Config["http_datas"].(string)
							if ok {
								err = jsoniter.Unmarshal([]byte(httpData), &httpDataArr)
								if err != nil {
									return nil, fmt.Errorf("metric %v unmarshal config error %v", curl.TypeMetricHttp, err)
								}
								length := len(httpDataArr)
								for i := 0; i < length; i++ {
									key := attr.Key + "_" + strconv.Itoa(i+1)
									DisTrans, err := createDiscardTransformer(key)
									if err != nil {
										return nil, fmt.Errorf("metric %v key %v, transform add failed, %v", tp, attr.Key, err)
									}
									trans = append(trans, DisTrans)
								}
							} else {
								return nil, errors.New("http_datas need to be string")
							}
						} else {
							key := attr.Key
							if !strings.HasPrefix(attr.Key, metricName+"_") {
								key = metricName + "_" + attr.Key
							}
							DisTrans, err := createDiscardTransformer(key)
							if err != nil {
								return nil, fmt.Errorf("metric %v key %v, transform add failed, %v", tp, attr.Key, err)
							}
							trans = append(trans, DisTrans)
						}
					}
				}
			}
		}
		transformers[metricName] = trans
	}
	if len(collectors) < 1 {
		return nil, errors.New("no collectors were added")
	}

	commonTransformers, err := createTransformers(rc)
	if err != nil {
		return nil, err
	}

	senders := make([]sender.Sender, 0)
	for _, senderConfig := range rc.SendersConfig {
		senderConfig[senderConf.KeyIsMetrics] = "true"
		senderConfig[senderConf.KeyPandoraTSDBTimeStamp] = metric.Timestamp
		if senderConfig[senderConf.KeySenderType] == senderConf.TypePandora {
			if rc.ExtraInfo {
				//如果已经开启了，不要重复加
				senderConfig[senderConf.KeyPandoraExtraInfo] = "false"
			}
			if senderConfig[senderConf.KeyPandoraDescription] == "" {
				senderConfig[senderConf.KeyPandoraDescription] = MetricAutoCreateDescription
			}
		}
		if senderType, ok := senderConfig[senderConf.KeySenderType]; ok && senderType == senderConf.TypeOpenFalconTransfer {
			senderConfig[senderConf.KeyCollectInterval] = fmt.Sprintf("%d", rc.CollectInterval)
			senderConfig[senderConf.KeyName] = rc.RunnerName
		}
		s, err := sr.NewSender(senderConfig, meta.FtSaveLogPath())
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
		rsMutex:         new(sync.RWMutex),
		collectInterval: interval,
		collectors:      collectors,
		transformers:    transformers,
		commonTrans:     commonTransformers,
		senders:         senders,
		envTag:          rc.EnvTag,
	}
	runner.StatusRestore()
	return
}

func (mr *MetricRunner) Name() string {
	return mr.RunnerName
}

func (r *MetricRunner) Run() {
	defer close(r.exitChan)
	defer func() {
		// recover when runner is stopped
		if atomic.LoadInt32(&r.stopped) <= 0 {
			return
		}
		if r := recover(); r != nil {
			log.Errorf("recover when runner is stopped\npanic: %v\nstack: %s", r, debug.Stack())
		}
	}()

	tags := r.meta.GetTags()
	tags = MergeEnvTags(r.envTag, tags)
	tags = MergeExtraInfoTags(r.meta, tags)

	for _, c := range r.collectors {
		collectorService, ok := c.(metric.CollectorService)
		if !ok {
			continue
		}
		log.Warnf("MetricRunner %v has %v collect", r.Name(), c.Name())
		if err := collectorService.Start(); err != nil {
			log.Errorf("collecter <%v> start failed: %v", c.Name(), err)
		} else {
			log.Infof("collecter <%v> start success", c.Name())
		}
	}

	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			log.Debugf("runner %v exited from run", r.RunnerName)
			r.exitChan <- struct{}{}
			return
		}
		// collect data
		dataCnt := 0
		datas := make([]Data, 0)
		tags[metric.Timestamp] = time.Now().Format(time.RFC3339Nano)
		for _, c := range r.collectors {
			metricName := c.Name()
			tmpdatas, err := c.Collect()
			if err != nil {
				log.Warnf("collecter <%v> collect data error: %v", c.Name(), err)
				if len(tmpdatas) == 0 {
					continue
				}
			}
			dataLen := len(tmpdatas)
			nameLen := len(metricName)
			if dataLen == 0 {
				log.Debugf("MetricRunner %v collect No data", c.Name())
				continue
			}
			tmpDatas := make([]Data, dataLen)
			for i, d := range tmpdatas {
				tmpDatas[i] = d
			}
			if trans, ok := r.transformers[metricName]; ok {
				for _, t := range trans {
					tmpDatas, err = t.Transform(tmpDatas)
					if err != nil {
						log.Errorf("runner[%v]: error %v", r.RunnerName, err)
					}
				}
			}
			for _, t := range r.commonTrans {
				tmpDatas, err = t.Transform(tmpDatas)
				if err != nil {
					log.Errorf("runner[%v]: error %v", r.RunnerName, err)
				}
			}
			for _, metricData := range tmpDatas {
				if len(metricData) == 0 {
					continue
				}
				data := Data{}
				// 重命名
				// cpu_time_user --> cpu__time_user
				for m, d := range metricData {
					newName := m
					if strings.HasPrefix(m, metricName) {
						newName = metricName + "_" + m[nameLen:]
					}
					data[newName] = d
				}
				datas = append(datas, data)
				dataCnt++
			}
		}
		if len(datas) == 0 {
			log.Warnf("metrics collect no data")
			time.Sleep(r.collectInterval)
			continue
		}
		if len(tags) > 0 {
			datas = AddTagsToData(tags, datas, r.Name())
		}
		r.rsMutex.Lock()
		r.rs.ReadDataCount += int64(dataCnt)
		r.rsMutex.Unlock()
		r.lastSend = time.Now()
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
func (r *MetricRunner) trySend(s sender.Sender, datas []Data, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	if _, ok := r.rs.SenderStats[s.Name()]; !ok {
		r.rs.SenderStats[s.Name()] = StatsInfo{}
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
		if se, ok := err.(*StatsError); ok {
			err = se.SendError
			if se.Ft {
				r.rs.Lag.Ftlags = se.FtQueueLag
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
			log.Errorf("runner[%v]: error %v", r.RunnerName, err)
			time.Sleep(time.Second)
			if times <= 0 || cnt < times {
				cnt++
				continue
			}
			log.Errorf("retry send %v times, but still error %v, discard datas... total %v lines", cnt, err, len(datas))
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
	defer timer.Stop()
	select {
	case <-mr.exitChan:
		log.Warnf("MetricRunner " + mr.Name() + " has been stopped ")
	case <-timer.C:
		log.Warnf("MetricRunner " + mr.Name() + " exited timeout ")
	}
	for _, collector := range mr.collectors {
		collectorClose, ok := collector.(metric.CollectorService)
		if !ok {
			continue
		}
		if err := collectorClose.Close(); err != nil {
			log.Errorf("cannot close collect name: %s, err: %v", collector.Name(), err)
		}
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

func (mr *MetricRunner) Reset() (err error) {
	var errMsg string
	if err = mr.meta.Reset(); err != nil {
		errMsg += err.Error() + "\n"
	}
	for _, sd := range mr.senders {
		ssd, ok := sd.(Resetable)
		if ok {
			if nerr := ssd.Reset(); nerr != nil {
				errMsg += nerr.Error() + "\n"
			}
		}
	}
	if errMsg != "" {
		err = errors.New(errMsg)
	}
	return err
}

func (mr *MetricRunner) Delete() error {
	var errs []string
	for _, collector := range mr.collectors {
		collectorClose, ok := collector.(metric.CollectorService)
		if !ok {
			continue
		}
		if err := collectorClose.Close(); err != nil {
			errs = append(errs, err.Error())
			log.Errorf("cannot close collect name: %s, err: %v", collector.Name(), err)
		}
	}
	err := mr.meta.Delete()
	if err != nil {
		errs = append(errs, err.Error())
	}
	return errors.New(strings.Join(errs, ","))
}

func (*MetricRunner) Cleaner() CleanInfo {
	return CleanInfo{
		enable: false,
	}
}

func (mr *MetricRunner) getStatusFrequently(now time.Time) (bool, float64, RunnerStatus) {
	mr.rsMutex.RLock()
	defer mr.rsMutex.RUnlock()
	elaspedTime := now.Sub(mr.rs.lastState).Seconds()
	if elaspedTime <= 3 {
		return true, elaspedTime, mr.lastRs.Clone()
	}
	return false, elaspedTime, RunnerStatus{}
}

func (mr *MetricRunner) Status() (rs RunnerStatus) {
	var isFre bool
	var elaspedtime float64
	now := time.Now()
	if isFre, elaspedtime, rs = mr.getStatusFrequently(now); isFre {
		return rs
	}
	mr.rsMutex.Lock()
	defer mr.rsMutex.Unlock()
	mr.rs.Elaspedtime += elaspedtime
	mr.rs.lastState = now
	durationTime := float64(mr.collectInterval.Seconds())
	mr.rs.ReadSpeed = int64(float64(mr.rs.ReadDataCount-mr.lastRs.ReadDataCount) / durationTime)
	mr.rs.ReadSpeedTrend = getTrend(mr.lastRs.ReadSpeed, mr.rs.ReadSpeed)

	for i := range mr.senders {
		sts, ok := mr.senders[i].(sender.StatsSender)
		if ok {
			senderStats := sts.Stats()
			senderStats.LastError = TruncateStrSize(senderStats.LastError, DefaultTruncateMaxSize)
			mr.rs.SenderStats[mr.senders[i].Name()] = senderStats
		}
	}

	for k, v := range mr.rs.SenderStats {
		if lv, ok := mr.lastRs.SenderStats[k]; ok {
			v.Speed, v.Trend = calcSpeedTrend(lv, v, durationTime)
		} else {
			v.Speed, v.Trend = calcSpeedTrend(StatsInfo{}, v, durationTime)
		}
		mr.rs.SenderStats[k] = v
	}
	mr.rs.RunningStatus = RunnerRunning
	*mr.lastRs = mr.rs.Clone()
	return *mr.lastRs
}

func (mr *MetricRunner) TokenRefresh(tokens AuthTokens) error {
	if mr.RunnerName != tokens.RunnerName {
		return fmt.Errorf("tokens.RunnerName[%v] is not match %v", tokens.RunnerName, mr.RunnerName)
	}
	if len(mr.senders) > tokens.SenderIndex {
		if tokenSender, ok := mr.senders[tokens.SenderIndex].(sender.TokenRefreshable); ok {
			return tokenSender.TokenRefresh(tokens.SenderTokens)
		}
	}
	return nil
}

func (mr *MetricRunner) StatusRestore() {
	rStat, err := mr.meta.ReadStatistic()

	if err != nil {
		log.Warnf("Runner[%v] restore status failed: %v", mr.RunnerName, err)
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
			sStatus.Restore(&StatsInfo{
				Success: info[0],
				Errors:  info[1],
			})
		}
		status, ext := mr.rs.SenderStats[name]
		if !ext {
			status = StatsInfo{}
		}
		status.Success = info[0]
		status.Errors = info[1]
		mr.rs.SenderStats[name] = status
	}
	*mr.lastRs = mr.rs.Clone()
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
			senderStats := sStatus.Stats()
			senderStats.LastError = TruncateStrSize(senderStats.LastError, DefaultTruncateMaxSize)
			status.SenderStats[name] = senderStats
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

func createDiscardTransformer(key string) (transforms.Transformer, error) {
	strTP := "discard"
	creator, ok := transforms.Transformers[strTP]
	if !ok {
		return nil, fmt.Errorf("type %v of transformer not exist", strTP)
	}
	tConf := map[string]string{
		"key":   key,
		"type":  strTP,
		"stage": "after_parser",
	}
	trans := creator()
	bts, err := jsoniter.Marshal(tConf)
	if err != nil {
		return nil, fmt.Errorf("type %v of transformer marshal config error %v", strTP, err)
	}
	err = jsoniter.Unmarshal(bts, trans)
	if err != nil {
		return nil, fmt.Errorf("type %v of transformer unmarshal config error %v", strTP, err)
	}
	return trans, nil
}
