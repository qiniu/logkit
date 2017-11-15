package mgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/cleaner"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

type CleanInfo struct {
	enable bool
	logdir string
}

const (
	SpeedUp     = "up"
	SpeedDown   = "down"
	SpeedStable = "stable"
)

type Runner interface {
	Name() string
	Run()
	Stop()
	Cleaner() CleanInfo
	Status() RunnerStatus
}

type Resetable interface {
	Reset() error
}

type StatusPersistable interface {
	StatusBackup()
	StatusRestore()
}

type RunnerStatus struct {
	Name             string                     `json:"name"`
	Logpath          string                     `json:"logpath"`
	ReadDataSize     int64                      `json:"readDataSize"`
	ReadDataCount    int64                      `json:"readDataCount"`
	Elaspedtime      float64                    `json:"elaspedtime"`
	Lag              RunnerLag                  `json:"lag"`
	ReaderStats      utils.StatsInfo            `json:"readerStats"`
	ParserStats      utils.StatsInfo            `json:"parserStats"`
	SenderStats      map[string]utils.StatsInfo `json:"senderStats"`
	TransformStats   map[string]utils.StatsInfo `json:"transformStats"`
	Error            string                     `json:"error,omitempty"`
	lastState        time.Time
	ReadSpeedKB      float64 `json:"readspeed_kb"`
	ReadSpeed        float64 `json:"readspeed"`
	ReadSpeedTrendKb string  `json:"readspeedtrend_kb"`
	ReadSpeedTrend   string  `json:"readspeedtrend"`
	Tag              string  `json:"tag,omitempty"`
	Url              string  `json:"url,omitempty"`
}

type RunnerLag struct {
	Size   int64 `json:"size"`
	Files  int64 `json:"files"`
	Ftlags int64 `json:"ftlags"`
}

// RunnerConfig 从多数据源读取，经过解析后，发往多个数据目的地
type RunnerConfig struct {
	RunnerInfo
	MetricConfig  []MetricConfig           `json:"metric,omitempty"`
	ReaderConfig  conf.MapConf             `json:"reader"`
	CleanerConfig conf.MapConf             `json:"cleaner,omitempty"`
	ParserConf    conf.MapConf             `json:"parser"`
	Transforms    []map[string]interface{} `json:"transforms,omitempty"`
	SenderConfig  []conf.MapConf           `json:"senders"`
	IsInWebFolder bool                     `json:"web_folder,omitempty"`
	IsStopped     bool                     `json:"is_stopped,omitempty"`
}

type RunnerInfo struct {
	RunnerName       string `json:"name"`
	CollectInterval  int    `json:"collect_interval,omitempty"` // metric runner收集的频率
	MaxBatchLen      int    `json:"batch_len,omitempty"`        // 每个read batch的行数
	MaxBatchSize     int    `json:"batch_size,omitempty"`       // 每个read batch的字节数
	MaxBatchInteval  int    `json:"batch_interval,omitempty"`   // 最大发送时间间隔
	MaxBatchTryTimes int    `json:"batch_try_times,omitempty"`  // 最大发送次数，小于等于0代表无限重试
	CreateTime       string `json:"createtime"`
}

type LogExportRunner struct {
	RunnerInfo

	stopped      int32
	exitChan     chan struct{}
	reader       reader.Reader
	cleaner      *cleaner.Cleaner
	parser       parser.LogParser
	senders      []sender.Sender
	transformers []transforms.Transformer

	rs      RunnerStatus
	lastRs  RunnerStatus
	rsMutex *sync.RWMutex

	meta *reader.Meta

	batchLen  int
	batchSize int
	lastSend  time.Time
}

const defaultSendIntervalSeconds = 60
const defaultMaxBatchSize = 2 * 1024 * 1024
const qiniulogHeadPatthern = "[1-9]\\d{3}/[0-1]\\d/[0-3]\\d [0-2]\\d:[0-6]\\d:[0-6]\\d(\\.\\d{6})?"

// NewRunner 创建Runner
func NewRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal) (runner Runner, err error) {
	return NewLogExportRunner(rc, cleanChan, parser.NewParserRegistry(), sender.NewSenderRegistry())
}

func NewCustomRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal, ps *parser.ParserRegistry, sr *sender.SenderRegistry) (runner Runner, err error) {
	if ps == nil {
		ps = parser.NewParserRegistry()
	}
	if sr == nil {
		sr = sender.NewSenderRegistry()
	}
	if rc.MetricConfig != nil {
		return NewMetricRunner(rc, sender.NewSenderRegistry())
	}
	return NewLogExportRunner(rc, cleanChan, ps, sr)
}

func NewRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.LogParser, transformers []transforms.Transformer, senders []sender.Sender, meta *reader.Meta) (runner Runner, err error) {
	return NewLogExportRunnerWithService(info, reader, cleaner, parser, transformers, senders, meta)
}

func NewLogExportRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.LogParser, transformers []transforms.Transformer, senders []sender.Sender, meta *reader.Meta) (runner *LogExportRunner, err error) {
	if info.MaxBatchSize <= 0 {
		info.MaxBatchSize = defaultMaxBatchSize
	}
	if info.MaxBatchInteval <= 0 {
		info.MaxBatchInteval = defaultSendIntervalSeconds
	}
	runner = &LogExportRunner{
		RunnerInfo: info,
		exitChan:   make(chan struct{}),
		lastSend:   time.Now(), // 上一次发送时间
		rs: RunnerStatus{
			SenderStats:    make(map[string]utils.StatsInfo),
			TransformStats: make(map[string]utils.StatsInfo),
			lastState:      time.Now(),
			Name:           info.RunnerName,
		},
		lastRs: RunnerStatus{
			SenderStats:    make(map[string]utils.StatsInfo),
			TransformStats: make(map[string]utils.StatsInfo),
			lastState:      time.Now(),
			Name:           info.RunnerName,
		},
		rsMutex: new(sync.RWMutex),
	}
	if reader == nil {
		err = errors.New("reader can not be nil")
		return
	}
	runner.reader = reader
	if meta == nil {
		err = errors.New("meta can not be nil")
		return
	}
	runner.meta = meta
	if cleaner == nil {
		log.Warnf("%v's cleaner was disabled", info.RunnerName)
	}
	runner.cleaner = cleaner
	if parser == nil {
		err = errors.New("parser can not be nil")
		return
	}
	runner.parser = parser

	runner.transformers = transformers

	if len(senders) < 1 {
		err = errors.New("senders can not be nil")
		return
	}
	runner.senders = senders
	runner.StatusRestore()
	return runner, nil
}

func NewLogExportRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal, ps *parser.ParserRegistry, sr *sender.SenderRegistry) (runner *LogExportRunner, err error) {
	runnerInfo := RunnerInfo{
		RunnerName:       rc.RunnerName,
		MaxBatchSize:     rc.MaxBatchSize,
		MaxBatchLen:      rc.MaxBatchLen,
		MaxBatchInteval:  rc.MaxBatchInteval,
		MaxBatchTryTimes: rc.MaxBatchTryTimes,
	}
	if rc.ReaderConfig == nil {
		return nil, errors.New(rc.RunnerName + " readerConfig is nil")
	}
	if rc.SenderConfig == nil {
		return nil, errors.New(rc.RunnerName + " SenderConfig is nil")
	}
	if rc.ParserConf == nil {
		return nil, errors.New(rc.RunnerName + " ParserConf is nil")
	}
	rc.ReaderConfig[utils.GlobalKeyName] = rc.RunnerName
	rc.ReaderConfig[reader.KeyRunnerName] = rc.RunnerName
	for i := range rc.SenderConfig {
		rc.SenderConfig[i][sender.KeyRunnerName] = rc.RunnerName
	}
	rc.ParserConf[parser.KeyRunnerName] = rc.RunnerName
	//配置文件适配
	rc = Compatible(rc)
	var (
		rd reader.Reader
		cl *cleaner.Cleaner
	)
	meta, err := reader.NewMetaWithConf(rc.ReaderConfig)
	if err != nil {
		return nil, err
	}
	if len(rc.CleanerConfig) > 0 {
		rd, err = reader.NewFileBufReaderWithMeta(rc.ReaderConfig, meta, rc.IsInWebFolder)
		if err != nil {
			return nil, err
		}
		cl, err = cleaner.NewCleaner(rc.CleanerConfig, meta, cleanChan, meta.LogPath())
		if err != nil {
			return nil, err
		}
	} else {
		rd, err = reader.NewFileBufReaderWithMeta(rc.ReaderConfig, meta, rc.IsInWebFolder)
		if err != nil {
			return nil, err
		}
	}
	parser, err := ps.NewLogParser(rc.ParserConf)
	if err != nil {
		return nil, err
	}
	transformers := createTransformers(rc)
	senders := make([]sender.Sender, 0)
	for i, c := range rc.SenderConfig {
		s, err := sr.NewSender(c, meta.FtSaveLogPath())
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
		delete(rc.SenderConfig[i], sender.InnerUserAgent)
	}
	return NewLogExportRunnerWithService(runnerInfo, rd, cl, parser, transformers, senders, meta)
}

func createTransformers(rc RunnerConfig) []transforms.Transformer {
	transformers := make([]transforms.Transformer, 0)
	for idx := range rc.Transforms {
		tConf := rc.Transforms[idx]
		tp := tConf[transforms.KeyType]
		if tp == nil {
			log.Error("field type is empty")
			continue
		}
		strTP, ok := tp.(string)
		if !ok {
			log.Error("field type is not string")
			continue
		}
		creater, ok := transforms.Transformers[strTP]
		if !ok {
			log.Errorf("type %v of transformer not exist", strTP)
			continue
		}
		trans := creater()
		bts, err := json.Marshal(tConf)
		if err != nil {
			log.Errorf("type %v of transformer marshal config error %v", strTP, err)
			continue
		}
		err = json.Unmarshal(bts, trans)
		if err != nil {
			log.Errorf("type %v of transformer unmarshal config error %v", strTP, err)
			continue
		}
		transformers = append(transformers, trans)
	}
	return transformers
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (r *LogExportRunner) trySend(s sender.Sender, datas []sender.Data, times int) bool {
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
			time.Sleep(time.Second)
			se, succ := err.(*reqerr.SendError)
			if succ {
				datas = sender.ConvertDatas(se.GetFailDatas())
				//无限重试的，除非遇到关闭
				if atomic.LoadInt32(&r.stopped) > 0 {
					return false
				}
				log.Errorf("Runner[%v] send error %v for %v times, failed datas %v will retry send it", r.RunnerName, se.Error(), cnt, len(datas))
				cnt++
				continue
			}
			if times <= 0 || cnt < times {
				cnt++
				continue
			}
			log.Errorf("Runner[%v] retry send %v times, but still error %v, discard datas %v ... total %v lines", r.RunnerName, cnt, err, datas, len(datas))
		}
		break
	}
	r.rsMutex.Lock()
	r.rs.SenderStats[s.Name()] = info
	r.rsMutex.Unlock()
	return true
}

func (r *LogExportRunner) Run() {
	if r.cleaner != nil {
		go r.cleaner.Run()
	}
	defer close(r.exitChan)
	datasourceTag := r.meta.GetDataSourceTag()
	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			log.Debugf("Runner[%v] exited from run", r.Name())
			if atomic.LoadInt32(&r.stopped) < 2 {
				r.exitChan <- struct{}{}
			}
			return
		}
		// read data
		var lines, froms []string
		for !r.batchFullOrTimeout() {
			line, err := r.reader.ReadLine()
			if err != nil && err != io.EOF {
				log.Errorf("Runner[%v] reader %s - error: %v, sleep 1 second...", r.Name(), r.reader.Name(), err)
				time.Sleep(time.Second)
				break
			}
			if len(line) <= 0 {
				log.Debugf("Runner[%v] reader %s no more content fetched sleep 1 second...", r.Name(), r.reader.Name())
				time.Sleep(1 * time.Second)
				continue
			}
			if len(line) >= r.MaxBatchSize {
				log.Errorf("Runner[%v] reader %s read lines larger than MaxBatchSize %v, content is %s", r.Name(), r.reader.Name(), r.MaxBatchSize, line)
				continue
			}
			r.rs.ReadDataSize += int64(len(line))
			r.rs.ReadDataCount++
			lines = append(lines, line)
			if datasourceTag != "" {
				froms = append(froms, r.reader.Source())
			}
			r.batchLen++
			r.batchSize += len(line)
		}
		r.batchLen = 0
		r.batchSize = 0
		r.lastSend = time.Now()

		for i := range r.transformers {
			var err error
			if r.transformers[i].Stage() == transforms.StageBeforeParser {
				lines, err = r.transformers[i].RawTransform(lines)
				if err != nil {
					log.Error(err)
				}
			}
		}

		if len(lines) <= 0 {
			log.Debugf("Runner[%v] fetched 0 lines", r.Name())
			pt, ok := r.parser.(parser.ParserType)
			if ok && pt.Type() == parser.TypeSyslog {
				lines = []string{parser.SyslogEofLine}
			} else {
				continue
			}
		}

		// parse data
		datas, err := r.parser.Parse(lines)
		se, ok := err.(*utils.StatsError)
		if ok {
			err = se.ErrorDetail
			r.rs.ParserStats.Errors += se.Errors
			r.rs.ParserStats.Success += se.Success
		} else if err != nil {
			r.rs.ParserStats.Errors++
		} else {
			r.rs.ParserStats.Success++
		}
		if err != nil {
			log.Errorf("Runner[%v] parser %s error : %v ", r.Name(), r.parser.Name(), err.Error())
		}
		// send data
		if len(datas) <= 0 {
			log.Debugf("Runner[%v] received parsed data length = 0", r.Name())
			continue
		}
		//把datasourcetag加到data里，前提是认为[]line变成[]data以后是一一对应的，一旦错位就不加

		if datasourceTag != "" {
			if len(datas)+len(se.ErrorIndex) == len(froms) {
				var j int = 0
				for i, v := range froms {
					if se.ErrorIndexIn(i) {
						continue
					}
					if j >= len(datas) {
						continue
					}
					if dt, ok := datas[j][datasourceTag]; ok {
						log.Debugf("Runner[%v] datasource tag already has data %v, ignore %v", r.Name(), dt, v)
					} else {
						datas[j][datasourceTag] = v
					}
				}
			} else {
				log.Errorf("Runner[%v] datasourcetag add error, datas %v not match with froms %v", r.Name(), datas, froms)
			}
		}
		for i := range r.transformers {
			if r.transformers[i].Stage() == transforms.StageAfterParser {
				datas, err = r.transformers[i].Transform(datas)
				if err != nil {
					log.Error(err)
				}
			}
		}
		success := true
		log.Debugf("Runner[%v] reader %s start to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
		for _, s := range r.senders {
			if !r.trySend(s, datas, r.MaxBatchTryTimes) {
				success = false
				log.Errorf("Runner[%v] failed to send data finally", r.Name())
				break
			}
		}
		if success {
			r.reader.SyncMeta()
		}
		log.Debugf("Runner[%v] send %s finish to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
	}
}

func (r *LogExportRunner) Stop() {
	atomic.AddInt32(&r.stopped, 1)

	log.Warnf("Runner[%v] waiting for stopped signal", r.Name())
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-r.exitChan:
		log.Warnf("runner " + r.Name() + " has been stopped ")
	case <-timer.C:
		log.Warnf("runner " + r.Name() + " exited timeout ")
		atomic.AddInt32(&r.stopped, 1)
	}
	log.Warnf("Runner[%v] wait for reader %v stopped", r.Name(), r.reader.Name())
	// 清理所有使用到的资源
	err := r.reader.Close()
	if err != nil {
		log.Errorf("Runner[%v] cannot close reader name: %s, err: %v", r.Name(), r.reader.Name(), err)
	} else {
		log.Warnf("Runner[%v] reader %v of runner %v closed", r.Name(), r.reader.Name(), r.Name())
	}
	for _, s := range r.senders {
		err := s.Close()
		if err != nil {
			log.Errorf("Runner[%v] cannot close sender name: %s, err: %v", r.Name(), s.Name(), err)
		} else {
			log.Warnf("Runner[%v] sender %v closed", r.Name(), s.Name())
		}
	}
	if r.cleaner != nil {
		r.cleaner.Close()
	}
}

func (r *LogExportRunner) Name() string {
	return r.RunnerName
}

func (r *LogExportRunner) Reset() error {
	var errmsg string
	err := r.meta.Reset()
	if err != nil {
		errmsg += err.Error() + "\n"
	}
	for _, sd := range r.senders {
		ssd, ok := sd.(Resetable)
		if ok {
			if nerr := ssd.Reset(); nerr != nil {
				errmsg += err.Error() + "\n"
			}
		}
	}
	return errors.New(errmsg)
}

func (r *LogExportRunner) Cleaner() CleanInfo {
	ci := CleanInfo{
		enable: r.cleaner != nil,
		logdir: r.reader.Source(),
	}
	return ci
}

func (r *LogExportRunner) batchFullOrTimeout() bool {
	// 达到最大行数
	if r.MaxBatchLen > 0 && r.batchLen >= r.MaxBatchLen {
		log.Debugf("Runner[%v] meet the max batch length %v", r.RunnerName, r.MaxBatchLen)
		return true
	}
	// 达到最大字节数
	if r.MaxBatchSize > 0 && r.batchSize >= r.MaxBatchSize {
		log.Debugf("Runner[%v] meet the max batch size %v", r.RunnerName, r.MaxBatchSize)
		return true
	}
	// 超过最长的发送间隔
	if time.Now().Sub(r.lastSend).Seconds() >= float64(r.MaxBatchInteval) {
		log.Debugf("Runner[%v] meet the max batch send interval %v", r.RunnerName, r.MaxBatchInteval)
		return true
	}
	// 如果任务已经停止
	if atomic.LoadInt32(&r.stopped) > 0 {
		log.Warnf("Runner[%v] meet the stopped signal", r.RunnerName)
		return true
	}
	return false
}

func (r *LogExportRunner) getReadDoneSize() (size int64, logreading string, err error) {
	mf := r.meta.MetaFile()
	bd, err := ioutil.ReadFile(mf)
	if err != nil {
		log.Warnf("Runner[%v] Read meta File err %v, can't get stats", r.Name(), err)
		return 0, "", nil
	}
	ss := strings.Split(strings.TrimSpace(string(bd)), "\t")
	if len(ss) != 2 {
		err = fmt.Errorf("Runner[%v] metafile format err %v, can't get stats", r.Name(), ss)
		log.Warn(err)
		return
	}
	logreading, logsize := ss[0], ss[1]
	size, err = strconv.ParseInt(logsize, 10, 64)
	if err != nil {
		log.Errorf("Runner[%v] parse log meta error %v, can't get stats", r.Name(), err)
		return
	}
	return
}

func (r *LogExportRunner) LagStats() (rl RunnerLag, err error) {
	size, logreading, err := r.getReadDoneSize()
	if err != nil {
		return
	}
	rl = RunnerLag{Files: 0, Size: -size}
	logpath := r.meta.LogPath()
	switch r.meta.GetMode() {
	case reader.DirMode:
		logs, serr := utils.ReadDirByTime(logpath)
		if serr != nil {
			log.Warnf("Runner[%v] ReadDirByTime err %v, can't get stats", r.Name(), serr)
			err = serr
			return
		}
		logreading = filepath.Base(logreading)
		for _, l := range logs {
			if l.IsDir() {
				continue
			}
			rl.Size += l.Size()
			if l.Name() == logreading {
				break
			}
			rl.Files++
		}
	case reader.FileMode:
		fi, serr := os.Stat(logpath)
		if serr != nil {
			err = serr
			return
		}
		rl.Size += fi.Size()
	default:
		err = fmt.Errorf("Runner[%v] readmode %v not support LagStats, can't get stats", r.Name(), r.meta.GetMode())
	}
	return
}

func getTrend(old, new float64) string {
	if old < new-0.1 {
		return SpeedUp
	}
	if old > new+0.1 {
		return SpeedDown
	}
	return SpeedStable
}

func (r *LogExportRunner) Status() RunnerStatus {
	now := time.Now()
	elaspedtime := now.Sub(r.rs.lastState).Seconds()
	if elaspedtime <= 3 {
		return r.rs
	}
	r.rsMutex.Lock()
	defer r.rsMutex.Unlock()
	r.rs.Error = ""
	if r.meta.IsFileMode() {
		r.rs.Logpath = r.meta.LogPath()
		rl, err := r.LagStats()
		if err != nil {
			r.rs.Error = fmt.Sprintf("get lag error %v", err)
		}
		r.rs.Lag = rl
	}

	r.rs.Elaspedtime += elaspedtime
	r.rs.lastState = now
	for i := range r.transformers {
		newtsts := r.transformers[i].Stats()
		ttp := r.transformers[i].Type()
		if oldtsts, ok := r.lastRs.TransformStats[ttp]; ok {
			newtsts.Speed, newtsts.Trend = calcSpeedTrend(oldtsts, newtsts, elaspedtime)
		} else {
			newtsts.Speed, newtsts.Trend = calcSpeedTrend(utils.StatsInfo{}, newtsts, elaspedtime)
		}
		r.rs.TransformStats[ttp] = newtsts
	}

	if str, ok := r.reader.(reader.StatsReader); ok {
		r.rs.ReaderStats = str.Status()
	}

	r.rs.ReadSpeedKB = float64(r.rs.ReadDataSize-r.lastRs.ReadDataSize) / elaspedtime
	r.rs.ReadSpeedTrendKb = getTrend(r.lastRs.ReadSpeedKB, r.rs.ReadSpeedKB)
	r.rs.ReadSpeed = float64(r.rs.ReadDataCount-r.lastRs.ReadDataCount) / elaspedtime
	r.rs.ReadSpeedTrend = getTrend(r.lastRs.ReadSpeed, r.rs.ReadSpeed)

	r.rs.ParserStats.Speed, r.rs.ParserStats.Trend = calcSpeedTrend(r.lastRs.ParserStats, r.rs.ParserStats, elaspedtime)

	for i := range r.senders {
		sts, ok := r.senders[i].(sender.StatsSender)
		if ok {
			r.rs.SenderStats[r.senders[i].Name()] = sts.Stats()
		}
	}

	for k, v := range r.rs.SenderStats {
		if lv, ok := r.lastRs.SenderStats[k]; ok {
			v.Speed, v.Trend = calcSpeedTrend(lv, v, elaspedtime)
		} else {
			v.Speed, v.Trend = calcSpeedTrend(utils.StatsInfo{}, v, elaspedtime)
		}
		r.rs.SenderStats[k] = v
	}
	copyRunnerStatus(&r.lastRs, &r.rs)
	return r.rs
}

func calcSpeedTrend(old, new utils.StatsInfo, elaspedtime float64) (speed float64, trend string) {
	if elaspedtime < 0.001 {
		speed = old.Speed
	} else {
		speed = float64(new.Success-old.Success) / elaspedtime
	}
	trend = getTrend(old.Speed, speed)
	return
}

func copyRunnerStatus(dst, src *RunnerStatus) {
	dst.TransformStats = make(map[string]utils.StatsInfo, len(src.TransformStats))
	dst.SenderStats = make(map[string]utils.StatsInfo, len(src.SenderStats))
	dst.ReadDataSize = src.ReadDataSize
	dst.ReadDataCount = src.ReadDataCount

	dst.ParserStats = src.ParserStats
	for k, v := range src.SenderStats {
		dst.SenderStats[k] = v
	}
	for k, v := range src.TransformStats {
		dst.TransformStats[k] = v
	}
	dst.ReadSpeedKB = src.ReadSpeedKB
	dst.ReadSpeed = src.ReadSpeed
}

//Compatible 用于新老配置的兼容
func Compatible(rc RunnerConfig) RunnerConfig {
	//兼容qiniulog与reader多行的配置
	if rc.ParserConf == nil {
		return rc
	}
	if rc.ReaderConfig == nil {
		return rc
	}
	parserType, err := rc.ParserConf.GetString(parser.KeyParserType)
	if err != nil {
		return rc
	}
	pattern, _ := rc.ReaderConfig.GetStringOr(reader.KeyHeadPattern, "")
	if parserType == parser.TypeLogv1 && pattern == "" {
		prefix, _ := rc.ParserConf.GetStringOr(parser.KeyQiniulogPrefix, "")
		prefix = strings.TrimSpace(prefix)
		var readpattern string
		if len(prefix) > 0 {
			readpattern = "^" + prefix + " " + qiniulogHeadPatthern
		} else {
			readpattern = "^" + qiniulogHeadPatthern
		}
		rc.ReaderConfig[reader.KeyHeadPattern] = readpattern
	}
	return rc
}

func (r *LogExportRunner) StatusRestore() {
	rStat, err := r.meta.ReadStatistic()

	if err != nil {
		log.Warnf("runner %v, restore status failed", r.RunnerName)
		return
	}
	r.rs.ReadDataCount = rStat.ReaderCnt
	r.rs.ParserStats.Success = rStat.ParserCnt[0]
	r.rs.ParserStats.Errors = rStat.ParserCnt[1]
	for _, s := range r.senders {
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
		status, ext := r.rs.SenderStats[name]
		if !ext {
			status = utils.StatsInfo{}
		}
		status.Success = info[0]
		status.Errors = info[1]
		r.rs.SenderStats[name] = status
	}
	copyRunnerStatus(&r.lastRs, &r.rs)
	log.Infof("runner %v restore status %v", r.RunnerName, rStat)
}

func (r *LogExportRunner) StatusBackup() {
	status := r.Status()
	bStart := &reader.Statistic{
		ReaderCnt: status.ReadDataCount,
		ParserCnt: [2]int64{
			status.ParserStats.Success,
			status.ParserStats.Errors,
		},
		SenderCnt: map[string][2]int64{},
	}
	for _, s := range r.senders {
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
	err := r.meta.WriteStatistic(bStart)
	if err != nil {
		log.Warnf("runner %v, backup status failed", r.RunnerName)
	} else {
		log.Infof("runner %v, backup status %v", r.RunnerName, bStart)
	}
}
