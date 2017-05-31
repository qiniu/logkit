package mgr

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/cleaner"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
)

type CleanInfo struct {
	enable bool
	logdir string
}

type Runner interface {
	Name() string
	Run()
	Stop()
	Cleaner() CleanInfo
	Status() RunnerStatus
}

type RunnerStatus struct {
	Name        string                     `json:"name"`
	Logpath     string                     `json:"logpath"`
	Lag         RunnerLag                  `json:"lag,omitempty"`
	ParserStats utils.StatsInfo            `json:"parserStats,omitempty"`
	SenderStats map[string]utils.StatsInfo `json:"senderStats,omitempty"`
	Error       error                      `json:"error,omitempty"`
}

type RunnerLag struct {
	Size   int64 `json:"size"`
	Files  int64 `json:"files"`
	Ftlags int64 `json:"ftlags"`
}

// RunnerConfig 从多数据源读取，经过解析后，发往多个数据目的地
type RunnerConfig struct {
	RunnerInfo
	ReaderConfig  conf.MapConf   `json:"reader"`
	CleanerConfig conf.MapConf   `json:"cleaner"`
	ParserConf    conf.MapConf   `json:"parser"`
	SenderConfig  []conf.MapConf `json:"senders"`
}

type RunnerInfo struct {
	RunnerName       string `json:"name"`
	MaxBatchLen      int    `json:"batch_len"`       // 每个read batch的行数
	MaxBatchSize     int    `json:"batch_size"`      // 每个read batch的字节数
	MaxBatchInteval  int    `json:"batch_interval"`  // 最大发送时间间隔
	MaxBatchTryTimes int    `json:"batch_try_times"` // 最大发送次数，小于等于0代表无限重试
}

type LogExportRunner struct {
	RunnerInfo

	stopped  int32
	exitChan chan struct{}
	reader   reader.Reader
	cleaner  *cleaner.Cleaner
	parser   parser.LogParser
	senders  []sender.Sender
	rs       RunnerStatus

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
	return NewLogExportRunner(rc, cleanChan, ps, sr)
}

func NewRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.LogParser, senders []sender.Sender, meta *reader.Meta) (runner Runner, err error) {
	return NewLogExportRunnerWithService(info, reader, cleaner, parser, senders, meta)
}

func NewLogExportRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.LogParser, senders []sender.Sender, meta *reader.Meta) (runner *LogExportRunner, err error) {
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
		rs:         RunnerStatus{SenderStats: make(map[string]utils.StatsInfo)},
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
	if len(senders) < 1 {
		err = errors.New("senders can not be nil")
		return
	}
	runner.senders = senders
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

	rc.ReaderConfig[utils.GlobalKeyName] = rc.RunnerName
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
		rd, err = reader.NewFileBufReaderWithMeta(rc.ReaderConfig, meta)
		if err != nil {
			return nil, err
		}
		cl, err = cleaner.NewCleaner(rc.CleanerConfig, meta, cleanChan, meta.LogPath())
		if err != nil {
			return nil, err
		}
	} else {
		rd, err = reader.NewFileBufReaderWithMeta(rc.ReaderConfig, meta)
		if err != nil {
			return nil, err
		}
	}
	parser, err := ps.NewLogParser(rc.ParserConf)
	if err != nil {
		return nil, err
	}
	senders := make([]sender.Sender, 0)
	for _, c := range rc.SenderConfig {
		s, err := sr.NewSender(c)
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
	}
	return NewLogExportRunnerWithService(runnerInfo, rd, cl, parser, senders, meta)
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (r *LogExportRunner) trySend(s sender.Sender, datas []sender.Data, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	if _, ok := r.rs.SenderStats[s.Name()]; !ok {
		r.rs.SenderStats[s.Name()] = utils.StatsInfo{}
	}
	info := r.rs.SenderStats[s.Name()]
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
				info.Errors += se.Errors
				info.Success += se.Success
			}
		} else if err != nil {
			info.Errors++
		} else {
			info.Success++
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
	r.rs.SenderStats[s.Name()] = info
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
			log.Debugf("runner %v exited from run", r.RunnerName)
			r.exitChan <- struct{}{}
			return
		}
		// read data
		var lines, froms []string
		for !r.batchFullOrTimeout() {
			line, err := r.reader.ReadLine()
			if err != nil && err != io.EOF {
				log.Warnf("runner %s, reader %s - error: %v", r.Name(), r.reader.Name(), err)
				break
			}
			if len(line) <= 0 {
				log.Debugf("runner %s, reader %s cannot get any content", r.Name(), r.reader.Name())
				time.Sleep(2 * time.Second)
				continue
			}
			if len(line) >= r.MaxBatchSize {
				log.Errorf("runner %s, reader %s read lines larger than MaxBatchSize %v, content is %s", r.Name(), r.reader.Name(), r.MaxBatchSize, line)
				continue
			}
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

		if len(lines) <= 0 {
			log.Debug("runner fetched 0 lines")
			continue
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
			log.Errorf("runner %s, parser %s error : %v ", r.Name(), r.parser.Name(), err.Error())
		}
		// send data
		if len(datas) <= 0 {
			log.Debug("runner received parsed data length = 0")
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
						log.Debugf("%v datasource tag already has data %v, ignore %v", r.Name(), dt, v)
					} else {
						datas[j][datasourceTag] = v
					}
				}
			} else {
				log.Errorf("%v datasourcetag add error, datas %v not match with froms %v", r.Name(), datas, froms)
			}
		}
		success := true
		for _, s := range r.senders {
			if !r.trySend(s, datas, r.MaxBatchTryTimes) {
				success = false
				log.Println(datas)
				break
			}
		}
		if success {
			r.reader.SyncMeta()
		}
	}
}

func (r *LogExportRunner) Stop() {
	atomic.AddInt32(&r.stopped, 1)

	log.Warnf("wait for runner " + r.Name() + " stopped")
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-r.exitChan:
		log.Warnf("runner " + r.Name() + " has been stopped ")
	case <-timer.C:
		log.Warnf("runner " + r.Name() + " exited timeout ")
	}
	log.Warnf("wait for reader " + r.reader.Name() + " stopped")
	// 清理所有使用到的资源
	err := r.reader.Close()
	if err != nil {
		log.Errorf("cannot close reader name: %s, err: %v", r.reader.Name(), err)
	} else {
		log.Warnf("reader %v of runner %v closed", r.reader.Name(), r.Name())
	}
	for _, s := range r.senders {
		err := s.Close()
		if err != nil {
			log.Errorf("cannot close sender name: %s, err: %v", s.Name(), err)
		} else {
			log.Warnf("sender %v of runner %v closed", s.Name(), r.Name())
		}
	}
	if r.cleaner != nil {
		r.cleaner.Close()
	}
}

func (r *LogExportRunner) Name() string {
	return r.RunnerName
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
		log.Debugf("runner %v meet the max batch length %v", r.RunnerName, r.MaxBatchLen)
		return true
	}
	// 达到最大字节数
	if r.MaxBatchSize > 0 && r.batchSize >= r.MaxBatchSize {
		log.Debugf("runner %v meet the max batch size %v", r.RunnerName, r.MaxBatchSize)
		return true
	}
	// 超过最长的发送间隔
	if time.Now().Sub(r.lastSend).Seconds() >= float64(r.MaxBatchInteval) {
		log.Debugf("runner %v meet the max batch send interval %v", r.RunnerName, r.MaxBatchInteval)
		return true
	}
	// 如果任务已经停止
	if atomic.LoadInt32(&r.stopped) > 0 {
		log.Warnf("runner %v meet the stopped signal", r.RunnerName)
		return true
	}

	return false
}

func (r *LogExportRunner) LagStats() (rl RunnerLag, err error) {
	mf := r.meta.MetaFile()

	bd, err := ioutil.ReadFile(mf)
	if err != nil {
		log.Warnf("Read meta File err %v, can't get stats", err)
		return
	}
	ss := strings.Split(strings.TrimSpace(string(bd)), "\t")
	if len(ss) != 2 {
		err = fmt.Errorf("metafile format err %v, can't get stats", ss)
		log.Warn(err)
		return
	}
	logreading, logsize := ss[0], ss[1]
	size, err := strconv.ParseInt(logsize, 10, 64)
	if err != nil {
		log.Errorf("parse log meta error %v, can't get stats", err)
		return
	}

	logs, err := utils.ReadDirByTime(r.meta.LogPath())
	if err != nil {
		log.Warn("ReadDirByTime err %v, can't get stats", err)
		return
	}
	logreading = filepath.Base(logreading)
	rl = RunnerLag{Files: 0, Size: -size}
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
	return
}

func (r *LogExportRunner) Status() RunnerStatus {
	r.rs.Name = r.RunnerName
	r.rs.Logpath = r.meta.LogPath()
	rl, err := r.LagStats()
	if err != nil {
		r.rs.Error = err
		return r.rs
	}
	r.rs.Lag = rl
	//self assign
	//r.rs.ParserStats = r.rs.ParserStats
	//r.rs.SenderStats = r.rs.SenderStats
	return r.rs
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
