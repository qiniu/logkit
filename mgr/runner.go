package mgr

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
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
	. "github.com/qiniu/logkit/utils/models"

	jsoniter "github.com/json-iterator/go"
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

	RunnerRunning = "running"
	RunnerStopped = "stopped"
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

type TokenRefreshable interface {
	TokenRefresh(AuthTokens) error
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
	Lag              LagInfo                    `json:"lag"`
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
	RunningStatus    string  `json:"runningStatus"`
	Tag              string  `json:"tag,omitempty"`
	Url              string  `json:"url,omitempty"`
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
	Router        sender.RouterConfig      `json:"router,omitempty"`
	IsInWebFolder bool                     `json:"web_folder,omitempty"`
	IsStopped     bool                     `json:"is_stopped,omitempty"`
}

type RunnerInfo struct {
	RunnerName       string `json:"name"`
	CollectInterval  int    `json:"collect_interval,omitempty"` // metric runner收集的频率
	MaxBatchLen      int    `json:"batch_len,omitempty"`        // 每个read batch的行数
	MaxBatchSize     int    `json:"batch_size,omitempty"`       // 每个read batch的字节数
	MaxBatchInterval int    `json:"batch_interval,omitempty"`   // 最大发送时间间隔
	MaxBatchTryTimes int    `json:"batch_try_times,omitempty"`  // 最大发送次数，小于等于0代表无限重试
	CreateTime       string `json:"createtime"`
	EnvTag           string `json:"env_tag,omitempty"`
	// 用这个字段的值来获取环境变量, 作为 tag 添加到数据中
}

type LogExportRunner struct {
	RunnerInfo

	stopped      int32
	exitChan     chan struct{}
	reader       reader.Reader
	cleaner      *cleaner.Cleaner
	parser       parser.LogParser
	senders      []sender.Sender
	router       *sender.Router
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
		return NewMetricRunner(rc, sr)
	}
	return NewLogExportRunner(rc, cleanChan, ps, sr)
}

func NewRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.LogParser, transformers []transforms.Transformer,
	senders []sender.Sender, router *sender.Router, meta *reader.Meta) (runner Runner, err error) {
	return NewLogExportRunnerWithService(info, reader, cleaner, parser, transformers, senders, router, meta)
}

func NewLogExportRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.LogParser,
	transformers []transforms.Transformer, senders []sender.Sender, router *sender.Router, meta *reader.Meta) (runner *LogExportRunner, err error) {
	if info.MaxBatchSize <= 0 {
		info.MaxBatchSize = defaultMaxBatchSize
	}
	if info.MaxBatchInterval <= 0 {
		info.MaxBatchInterval = defaultSendIntervalSeconds
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
			RunningStatus:  RunnerRunning,
		},
		lastRs: RunnerStatus{
			SenderStats:    make(map[string]utils.StatsInfo),
			TransformStats: make(map[string]utils.StatsInfo),
			lastState:      time.Now(),
			Name:           info.RunnerName,
			RunningStatus:  RunnerRunning,
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
	runner.router = router
	runner.StatusRestore()
	return runner, nil
}

func NewLogExportRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal, ps *parser.ParserRegistry, sr *sender.SenderRegistry) (runner *LogExportRunner, err error) {
	runnerInfo := RunnerInfo{
		EnvTag:           rc.EnvTag,
		RunnerName:       rc.RunnerName,
		MaxBatchSize:     rc.MaxBatchSize,
		MaxBatchLen:      rc.MaxBatchLen,
		MaxBatchInterval: rc.MaxBatchInterval,
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
	rc.ReaderConfig[GlobalKeyName] = rc.RunnerName
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
	senderCnt := len(senders)
	router, err := sender.NewSenderRouter(rc.Router, senderCnt)
	if err != nil {
		return nil, fmt.Errorf("runner %v add sender router error, %v", rc.RunnerName, err)
	}
	return NewLogExportRunnerWithService(runnerInfo, rd, cl, parser, transformers, senders, router, meta)
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
		bts, err := jsoniter.Marshal(tConf)
		if err != nil {
			log.Errorf("type %v of transformer marshal config error %v", strTP, err)
			continue
		}
		err = jsoniter.Unmarshal(bts, trans)
		if err != nil {
			log.Errorf("type %v of transformer unmarshal config error %v", strTP, err)
			continue
		}
		//transformer初始化
		if trans, ok := trans.(transforms.Initialize); ok {
			err = trans.Init()
			if err != nil {
				log.Errorf("type %v of transformer init error %v", strTP, err)
				continue
			}
		}
		transformers = append(transformers, trans)
	}
	return transformers
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (r *LogExportRunner) trySend(s sender.Sender, datas []Data, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	r.rsMutex.Lock()
	if _, ok := r.rs.SenderStats[s.Name()]; !ok {
		r.rs.SenderStats[s.Name()] = utils.StatsInfo{}
	}
	info := r.rs.SenderStats[s.Name()]
	r.rsMutex.Unlock()
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
				r.rsMutex.Lock()
				r.rs.Lag.Ftlags = se.Ftlag
				r.rsMutex.Unlock()
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
	datasourceTag := r.meta.GetDataSourceTag()
	schemaErr := utils.SchemaErr{Number: 0, Last: time.Unix(0, 0)}
	tags = GetEnvTag(r.EnvTag, tags)
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
			r.rsMutex.Lock()
			r.rs.ReadDataSize += int64(len(line))
			r.rs.ReadDataCount++
			r.rsMutex.Unlock()
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
		errorCnt := int64(0)
		datas, err := r.parser.Parse(lines)
		se, ok := err.(*utils.StatsError)
		r.rsMutex.Lock()
		if ok {
			errorCnt = se.Errors
			err = se.ErrorDetail
			r.rs.ParserStats.Errors += se.Errors
			r.rs.ParserStats.Success += se.Success
			if err != nil {
				r.rs.ParserStats.LastError = err.Error()
			}
		} else if err != nil {
			errorCnt = 1
			r.rs.ParserStats.Errors++
			r.rs.ParserStats.LastError = err.Error()
		} else {
			r.rs.ParserStats.Success++
		}
		if err != nil {
			r.rs.ParserStats.LastError = err.Error()
		}
		r.rsMutex.Unlock()
		if err != nil {
			errMsg := fmt.Sprintf("Runner[%v] parser %s error : %v ", r.Name(), r.parser.Name(), err.Error())
			log.Debugf(errMsg)
			schemaErr.Output(errorCnt, errors.New(errMsg))
		}
		// send data
		if len(datas) <= 0 {
			log.Debugf("Runner[%v] received parsed data length = 0", r.Name())
			continue
		}

		//把datasourcetag加到data里，前提是认为[]line变成[]data以后是一一对应的，一旦错位就不加
		if datasourceTag != "" {
			if len(datas) == len(froms) {
				datas = addSourceToData(froms, se, datas, datasourceTag, r.Name(), true)
			} else if len(datas)+len(se.ErrorIndex) == len(froms) {
				datas = addSourceToData(froms, se, datas, datasourceTag, r.Name(), false)
			} else {
				log.Errorf("Runner[%v] datasourcetag add error, datas %v not match with froms %v", r.Name(), datas, froms)
			}
		}
		//把tagfile加到data里，前提是认为[]line变成[]data以后是一一对应的，一旦错位就不加
		if tags != nil {
			datas = addTagsToData(tags, datas, r.Name())
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
		senderCnt := len(r.senders)
		log.Debugf("Runner[%v] reader %s start to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
		senderDataList := classifySenderData(datas, r.router, senderCnt)
		for index, s := range r.senders {
			if !r.trySend(s, senderDataList[index], r.MaxBatchTryTimes) {
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

func classifySenderData(datas []Data, router *sender.Router, senderCnt int) [][]Data {
	senderDataList := make([][]Data, senderCnt)
	for i := 0; i < senderCnt; i++ {
		if router == nil {
			senderDataList[i] = datas
		} else {
			senderDataList[i] = make([]Data, 0)
		}
	}
	if router == nil {
		return senderDataList
	}
	for _, d := range datas {
		senderIndex := router.GetSenderIndex(d)
		senderData := senderDataList[senderIndex]
		senderData = append(senderData, d)
		senderDataList[senderIndex] = senderData
	}
	return senderDataList
}

func addSourceToData(sourceFroms []string, se *utils.StatsError, datas []Data, datasourceTagName, runnername string, recordErrData bool) []Data {
	j := 0
	for i, v := range sourceFroms {
		if recordErrData {
			j = i
		} else {
			if se.ErrorIndexIn(i) {
				continue
			}
		}
		if j >= len(datas) {
			continue
		}

		if dt, ok := datas[j][datasourceTagName]; ok {
			log.Debugf("Runner[%v] datasource tag already has data %v, ignore %v", runnername, dt, v)
		} else {
			datas[j][datasourceTagName] = v
		}
		j++
	}
	return datas
}

func addTagsToData(tags map[string]interface{}, datas []Data, runnername string) []Data {
	for j, data := range datas {
		for k, v := range tags {
			if dt, ok := data[k]; ok {
				log.Debugf("Runner[%v] datasource tag already has data %v, ignore %v", runnername, dt, v)
			} else {
				data[k] = v
			}
		}
		datas[j] = data
	}
	return datas
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

func (r *LogExportRunner) Reset() (err error) {
	var errMsg string
	if read, ok := r.reader.(Resetable); ok {
		if subErr := read.Reset(); subErr != nil {
			errMsg += subErr.Error() + "\n"
		}
	}
	if err = r.meta.Reset(); err != nil {
		errMsg += err.Error() + "\n"
	}
	for _, sd := range r.senders {
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

func (r *LogExportRunner) Cleaner() CleanInfo {
	if r.cleaner == nil {
		return CleanInfo{enable: false}
	}
	return CleanInfo{
		enable: true,
		logdir: r.cleaner.LogDir(),
	}
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
	if time.Now().Sub(r.lastSend).Seconds() >= float64(r.MaxBatchInterval) {
		log.Debugf("Runner[%v] meet the max batch send interval %v", r.RunnerName, r.MaxBatchInterval)
		return true
	}
	// 如果任务已经停止
	if atomic.LoadInt32(&r.stopped) > 0 {
		log.Warnf("Runner[%v] meet the stopped signal", r.RunnerName)
		return true
	}
	return false
}

func (r *LogExportRunner) LagStats() (rl *LagInfo, err error) {
	lr, ok := r.reader.(reader.LagReader)
	if ok {
		return lr.Lag()
	}
	err = fmt.Errorf("readmode %v not support LagStats, can't get stats", r.meta.GetMode())
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

func (r *LogExportRunner) getStatusFrequently(rss *RunnerStatus, now time.Time) (bool, float64) {
	r.rsMutex.RLock()
	defer r.rsMutex.RUnlock()
	elaspedTime := now.Sub(r.rs.lastState).Seconds()
	if elaspedTime <= 3 {
		deepCopy(rss, &r.rs)
		return true, elaspedTime
	}
	return false, elaspedTime
}

func (r *LogExportRunner) Status() RunnerStatus {
	var isFre bool
	var elaspedtime float64
	rss := RunnerStatus{}
	now := time.Now()
	if isFre, elaspedtime = r.getStatusFrequently(&rss, now); isFre {
		return rss
	}
	r.rsMutex.Lock()
	defer r.rsMutex.Unlock()
	r.rs.Error = ""
	r.rs.Logpath = r.meta.LogPath()
	rl, err := r.LagStats()
	if err != nil {
		r.rs.Error = fmt.Sprintf("get lag error: %v", err)
		log.Warn(r.rs.Error)
	}
	r.rs.Lag = *rl

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
		r.rs.ReaderStats.Success = r.rs.ReadDataCount
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
	r.rs.RunningStatus = RunnerRunning
	copyRunnerStatus(&r.lastRs, &r.rs)
	deepCopy(&rss, &r.rs)
	return rss
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

func deepCopy(dst, src interface{}) {
	var err error
	var confByte []byte
	if confByte, err = jsoniter.Marshal(src); err != nil {
		log.Debugf("runner config marshal error %v", err)
		dst = src
	}
	if err = jsoniter.Unmarshal(confByte, dst); err != nil {
		log.Debugf("runner config unmarshal error %v", err)
		dst = src
	}
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

func (r *LogExportRunner) TokenRefresh(tokens AuthTokens) error {
	if r.RunnerName != tokens.RunnerName {
		return fmt.Errorf("tokens.RunnerName[%v] is not match %v", tokens.RunnerName, r.RunnerName)
	}
	if len(r.senders) > tokens.SenderIndex {
		if tokenSender, ok := r.senders[tokens.SenderIndex].(sender.TokenRefreshable); ok {
			return tokenSender.TokenRefresh(tokens.SenderTokens)
		}
	}
	return nil
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

func GetEnvTag(name string, tags map[string]interface{}) map[string]interface{} {
	if name == "" {
		return tags
	}
	if value := os.Getenv(name); value != "" {
		if tags == nil {
			tags = make(map[string]interface{})
		}
		tags[name] = value
	} else {
	}
	return tags
}
