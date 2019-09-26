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

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/audit"
	"github.com/qiniu/logkit/cleaner"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	_ "github.com/qiniu/logkit/parser/builtin"
	"github.com/qiniu/logkit/parser/config"
	parserconfig "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/qiniu"
	"github.com/qiniu/logkit/reader"
	_ "github.com/qiniu/logkit/reader/builtin"
	"github.com/qiniu/logkit/reader/cloudtrail"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/router"
	"github.com/qiniu/logkit/sender"
	_ "github.com/qiniu/logkit/sender/builtin"
	senderConf "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/transforms/ip"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/utils/equeue"
	. "github.com/qiniu/logkit/utils/models"
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

type RunnerErrors interface {
	GetErrors() ErrorsResult
}

type TokenRefreshable interface {
	TokenRefresh(AuthTokens) error
}

type StatusPersistable interface {
	StatusBackup()
	StatusRestore()
}

var (
	_ Resetable  = &LogExportRunner{}
	_ Deleteable = &LogExportRunner{}
)

type LogExportRunner struct {
	RunnerInfo

	stopped      int32
	exitChan     chan struct{}
	reader       reader.Reader
	cleaner      *cleaner.Cleaner
	parser       parser.Parser
	senders      []sender.Sender
	router       *router.Router
	transformers []transforms.Transformer
	historyError *ErrorsList

	rs           *RunnerStatus
	lastRs       *RunnerStatus
	rsMutex      *sync.RWMutex
	historyMutex *sync.RWMutex

	meta *reader.Meta

	batchLen  int64
	batchSize int64
	lastSend  time.Time
	syncInc   int
	tracker   *utils.Tracker
	auditChan chan<- audit.Message
}

// NewRunner 创建Runner
func NewRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal) (runner Runner, err error) {
	return NewLogExportRunner(rc, cleanChan, reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
}

func NewCustomRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal, rr *reader.Registry, pr *parser.Registry, sr *sender.Registry) (runner Runner, err error) {
	if rr == nil {
		rr = reader.NewRegistry()
	}
	if pr == nil {
		pr = parser.NewRegistry()
	}
	if sr == nil {
		sr = sender.NewRegistry()
	}

	if rc.MetricConfig != nil {
		return NewMetricRunner(rc, sr)
	}
	return NewLogExportRunner(rc, cleanChan, rr, pr, sr)
}

func NewRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.Parser, transformers []transforms.Transformer,
	senders []sender.Sender, router *router.Router, meta *reader.Meta) (runner Runner, err error) {
	return NewLogExportRunnerWithService(info, reader, cleaner, parser, transformers, senders, router, meta)
}

func NewLogExportRunnerWithService(info RunnerInfo, reader reader.Reader, cleaner *cleaner.Cleaner, parser parser.Parser,
	transformers []transforms.Transformer, senders []sender.Sender, router *router.Router, meta *reader.Meta) (runner *LogExportRunner, err error) {
	if info.MaxBatchSize <= 0 {
		info.MaxBatchSize = DefaultMaxBatchSize
	}
	if info.MaxBatchInterval <= 0 {
		info.MaxBatchInterval = DefaultSendIntervalSeconds
	}
	if info.ErrorsListCap <= 0 {
		info.ErrorsListCap = DefaultErrorsListCap
	}
	runner = &LogExportRunner{
		RunnerInfo: info,
		exitChan:   make(chan struct{}),
		lastSend:   time.Now(), // 上一次发送时间
		rs: &RunnerStatus{
			SenderStats:    make(map[string]StatsInfo),
			TransformStats: make(map[string]StatsInfo),
			lastState:      time.Now(),
			Name:           info.RunnerName,
			RunningStatus:  RunnerRunning,
		},
		lastRs: &RunnerStatus{
			SenderStats:    make(map[string]StatsInfo),
			TransformStats: make(map[string]StatsInfo),
			lastState:      time.Now(),
			Name:           info.RunnerName,
			RunningStatus:  RunnerRunning,
		},
		historyError: NewErrorsList(),
		rsMutex:      new(sync.RWMutex),
		tracker:      utils.NewTracker(),
		historyMutex: new(sync.RWMutex),
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
		log.Debugf("%v's cleaner was disabled", info.RunnerName)
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

func NewLogExportRunner(rc RunnerConfig, cleanChan chan<- cleaner.CleanSignal, rr *reader.Registry, pr *parser.Registry, sr *sender.Registry) (runner *LogExportRunner, err error) {
	runnerInfo := rc.RunnerInfo
	if rc.ReaderConfig == nil {
		return nil, errors.New(rc.RunnerName + " reader in config is nil")
	}
	if rc.SendersConfig == nil {
		return nil, errors.New(rc.RunnerName + " senders in config is nil")
	}
	if rc.ParserConf == nil {
		log.Warn(rc.RunnerName + " parser conf is nil, use raw parser as default")
		rc.ParserConf = conf.MapConf{config.KeyParserType: config.TypeRaw}
	}
	rc.ReaderConfig[GlobalKeyName] = rc.RunnerName
	rc.ReaderConfig[KeyRunnerName] = rc.RunnerName
	if rc.ExtraInfo {
		rc.ReaderConfig[ExtraInfo] = Bool2String(rc.ExtraInfo)
	}
	for i := range rc.SendersConfig {
		rc.SendersConfig[i][KeyRunnerName] = rc.RunnerName
	}
	rc.ParserConf[KeyRunnerName] = rc.RunnerName
	//配置文件适配
	rc = Compatible(rc)
	var (
		rd reader.Reader
		cl *cleaner.Cleaner
	)
	mode := rc.ReaderConfig["mode"]
	if mode == ModeCloudTrail || mode == ModeCloudTrailV2 {
		syncDir := rc.ReaderConfig[KeySyncDirectory]
		if syncDir == "" {
			bucket, prefix, region, ak, sk, _ := cloudtrail.GetS3UserInfo(rc.ReaderConfig)
			syncDir = cloudtrail.GetDefaultSyncDir(bucket, prefix, region, ak, sk, rc.RunnerName)
		}
		rc.ReaderConfig[KeyLogPath] = syncDir
		if len(rc.CleanerConfig) == 0 {
			rc.CleanerConfig = conf.MapConf{
				"delete_enable":       "true",
				"delete_interval":     "60",
				"reserve_file_number": "50",
			}
		}
	}
	meta, err := reader.NewMetaWithConf(rc.ReaderConfig)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && rd != nil {
			rd.Close()
		}
	}()
	if len(rc.CleanerConfig) > 0 {
		rd, err = rr.NewReaderWithMeta(rc.ReaderConfig, meta, false)
		if err != nil {
			return nil, err
		}
		cl, err = cleaner.NewCleaner(rc.CleanerConfig, meta, cleanChan, meta.LogPath())
		if err != nil {
			return nil, err
		}
	} else {
		rd, err = rr.NewReaderWithMeta(rc.ReaderConfig, meta, false)
		if err != nil {
			return nil, err
		}
	}
	ps, err := pr.NewLogParser(rc.ParserConf)
	if err != nil {
		return nil, err
	}

	var serverConfigs = make([]map[string]interface{}, 0, 10)
	if serverParser, ok := ps.(parser.ServerParser); ok {
		if serverParser.ServerConfig() != nil {
			serverConfigs = append(serverConfigs, serverParser.ServerConfig())
		}
	}

	transformers, err := createTransformers(rc)
	if err != nil {
		return nil, err
	}
	for _, transform := range transformers {
		if serverTransformer, ok := transform.(transforms.ServerTansformer); ok {
			if serverTransformer.ServerConfig() != nil {
				serverConfigs = append(serverConfigs, serverTransformer.ServerConfig())
			}
		}
	}
	senders := make([]sender.Sender, 0)
	for i, senderConfig := range rc.SendersConfig {
		if rc.SendRaw {
			senderConfig[senderConf.InnerSendRaw] = "true"
		}
		if senderConfig[senderConf.KeySenderType] == senderConf.TypePandora {
			if rc.ExtraInfo {
				//如果已经开启了，不要重复加
				senderConfig[senderConf.KeyPandoraExtraInfo] = "false"
			}
			if senderConfig[senderConf.KeyPandoraDescription] == "" {
				senderConfig[senderConf.KeyPandoraDescription] = LogkitAutoCreateDescription
			}
		}
		if senderType, ok := senderConfig[senderConf.KeySenderType]; ok && senderType == senderConf.TypeOpenFalconTransfer {
			if meta.GetMode() == ModeSnmp {
				intervalStr, _ := rc.ReaderConfig.GetStringOr(KeySnmpReaderInterval, "30s")
				interval, err := time.ParseDuration(intervalStr)
				if err != nil {
					return nil, err
				}
				senderConfig[senderConf.KeyCollectInterval] = fmt.Sprintf("%d", int64(interval.Seconds()))
				senderConfig[senderConf.KeyName] = rc.RunnerName
			}
			log.Infof("senderConfig = %+v", senderConfig)
		}
		senderConfig, err := setPandoraServerConfig(senderConfig, serverConfigs)
		if err != nil {
			return nil, err
		}
		s, err := sr.NewSender(senderConfig, meta.FtSaveLogPath())
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
		delete(rc.SendersConfig[i], senderConf.InnerUserAgent)
		delete(rc.SendersConfig[i], senderConf.KeyPandoraDescription)
		delete(rc.SendersConfig[i], senderConf.InnerSendRaw)
	}

	senderCnt := len(senders)
	router, err := router.NewSenderRouter(rc.Router, senderCnt)
	if err != nil {
		return nil, fmt.Errorf("runner %v add sender router error, %v", rc.RunnerName, err)
	}
	runner, err = NewLogExportRunnerWithService(runnerInfo, rd, cl, ps, transformers, senders, router, meta)
	if err != nil {
		return runner, err
	}
	if runner.LogAudit {
		if rc.AuditChan == nil {
			runner.LogAudit = false
		} else {
			runner.auditChan = rc.AuditChan
		}
	}
	return runner, nil
}

func createTransformers(rc RunnerConfig) ([]transforms.Transformer, error) {
	transformers := make([]transforms.Transformer, 0)
	for idx := range rc.Transforms {
		tConf := rc.Transforms[idx]
		tp := tConf[KeyType]
		if tp == nil {
			return nil, fmt.Errorf("transformer config type is empty %v", tConf)
		}
		strTP, ok := tp.(string)
		if !ok {
			return nil, fmt.Errorf("transformer config field type %v is not string", tp)
		}
		creator, ok := transforms.Transformers[strTP]
		if !ok {
			return nil, fmt.Errorf("transformer type unsupported: %v", strTP)
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
		//transformer初始化
		if trans, ok := trans.(transforms.Initializer); ok {
			err = trans.Init()
			if err != nil {
				return nil, fmt.Errorf("type %v of transformer init error %v", strTP, err)
			}
		}
		transformers = append(transformers, trans)
	}
	return transformers, nil
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (r *LogExportRunner) tryRawSend(s sender.Sender, datas []string, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	r.rsMutex.Lock()
	if _, ok := r.rs.SenderStats[s.Name()]; !ok {
		r.rs.SenderStats[s.Name()] = StatsInfo{}
	}
	info := r.rs.SenderStats[s.Name()]
	r.rsMutex.Unlock()

	var (
		err             error
		successDatasLen int64
		originDatasLen  = int64(len(datas))
		cnt             = 1
	)
	rawSender, ok := s.(sender.RawSender)
	if !ok {
		log.Errorf("runner[%s]: sender not raw sender, can not use tryRawSend %v", r.RunnerName, err)
		return true
	}

	for {
		// 至少尝试一次。如果任务已经停止，那么只尝试一次
		if cnt > 1 && atomic.LoadInt32(&r.stopped) > 0 {
			return false
		}

		err = rawSender.RawSend(datas)
		if err == nil {
			successDatasLen += int64(len(datas))
			break
		}

		se, ok := err.(*StatsError)
		if ok {
			if se.Errors == 0 {
				successDatasLen += int64(len(datas))
				break
			}

			if se.Ft {
				r.rsMutex.Lock()
				r.rs.Lag.Ftlags = se.FtQueueLag
				r.rsMutex.Unlock()
			}
			if se.SendError != nil {
				successDatasLen += int64(len(datas) - len(se.SendError.GetFailDatas()))
				err = se.SendError
			}
		}

		info.LastError = TruncateStrSize(err.Error(), DefaultTruncateMaxSize)
		r.historyMutex.Lock()
		if r.historyError.SendErrors == nil {
			r.historyError.SendErrors = make(map[string]*equeue.ErrorQueue)
		}
		if r.historyError.SendErrors[s.Name()] == nil {
			r.historyError.SendErrors[s.Name()] = equeue.New(r.ErrorsListCap)
		}
		r.historyError.SendErrors[s.Name()].Put(equeue.NewError(info.LastError))
		r.historyMutex.Unlock()

		//FaultTolerant Sender 正常的错误会在backupqueue里面记录，自己重试，此处无需重试
		if se != nil && se.Ft && se.FtNotRetry {
			break
		}
		time.Sleep(time.Second)
		_, ok = err.(*reqerr.SendError)
		if ok {
			//无限重试的，除非遇到关闭
			if atomic.LoadInt32(&r.stopped) > 0 {
				return false
			}
			log.Errorf("Runner[%v] send error %v for %v times, failed datas length %v will retry send it", r.RunnerName, se.Error(), cnt, len(datas))
			cnt++
			continue
		}

		if err == ErrQueueClosed {
			log.Errorf("Runner[%v] send to closed queue, discard datas, send error %v, failed datas (length %v): %v", r.RunnerName, se.Error(), cnt, datas)
			break
		}
		if times <= 0 || cnt < times {
			cnt++
			continue
		}
		log.Errorf("Runner[%v] retry send %v times, but still error %v, total %v data lines", r.RunnerName, cnt, err, len(datas))
		break
	}
	info.Errors += originDatasLen - successDatasLen
	info.Success += successDatasLen
	r.rsMutex.Lock()
	r.rs.SenderStats[s.Name()] = info
	r.rsMutex.Unlock()
	return true
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (r *LogExportRunner) trySend(s sender.Sender, datas []Data, times int) bool {
	if len(datas) <= 0 {
		return true
	}
	r.rsMutex.Lock()
	if _, ok := r.rs.SenderStats[s.Name()]; !ok {
		r.rs.SenderStats[s.Name()] = StatsInfo{}
	}
	info := r.rs.SenderStats[s.Name()]
	r.rsMutex.Unlock()

	var (
		err             error
		successDatasLen int64
		originDatasLen  = int64(len(datas))
		cnt             = 1
	)

	for {
		// 至少尝试一次。如果任务已经停止，那么只尝试一次
		if cnt > 1 && atomic.LoadInt32(&r.stopped) > 0 {
			return false
		}
		err = s.Send(datas)
		if err == nil {
			successDatasLen += int64(len(datas))
			break
		}

		se, ok := err.(*StatsError)
		if ok {
			if se.Errors == 0 {
				successDatasLen += int64(len(datas))
				break
			}

			if se.Ft {
				r.rsMutex.Lock()
				r.rs.Lag.Ftlags = se.FtQueueLag
				r.rsMutex.Unlock()
			}
			if se.SendError != nil {
				successDatasLen += int64(len(datas) - len(se.SendError.GetFailDatas()))
				err = se.SendError
			}
		}

		info.LastError = TruncateStrSize(err.Error(), DefaultTruncateMaxSize)
		r.historyMutex.Lock()
		if r.historyError.SendErrors == nil {
			r.historyError.SendErrors = make(map[string]*equeue.ErrorQueue)
		}
		if r.historyError.SendErrors[s.Name()] == nil {
			r.historyError.SendErrors[s.Name()] = equeue.New(r.ErrorsListCap)
		}
		r.historyError.SendErrors[s.Name()].Put(equeue.NewError(info.LastError))
		r.historyMutex.Unlock()

		//FaultTolerant Sender 正常的错误会在backupqueue里面记录，自己重试，此处无需重试
		if se != nil && se.Ft && se.FtNotRetry {
			break
		}
		time.Sleep(time.Second)
		sendError, ok := err.(*reqerr.SendError)
		if ok {
			datas = sender.ConvertDatas(sendError.GetFailDatas())
			//无限重试的，除非遇到关闭
			if atomic.LoadInt32(&r.stopped) > 0 {
				return false
			}
			if sendError.ErrorType == sender.TypeMarshalError {
				log.Errorf("Runner[%v] datas marshal failed, discard datas, send error %v, failed datas (length %v): %v", r.RunnerName, se.Error(), cnt, datas)
				break
			}
			log.Errorf("Runner[%v] send error %v for %v times, failed datas length %v will retry send it", r.RunnerName, se.Error(), cnt, len(datas))
			cnt++
			continue
		}

		if err == ErrQueueClosed {
			log.Errorf("Runner[%v] send to closed queue, discard datas, send error %v, failed datas (length %v): %v", r.RunnerName, se.Error(), cnt, datas)
			break
		}
		if times <= 0 || cnt < times {
			cnt++
			continue
		}
		log.Errorf("Runner[%v] retry send %v times, but still error %v, total %v data lines", r.RunnerName, cnt, err, len(datas))
		break
	}

	info.Errors += originDatasLen - successDatasLen
	info.Success += successDatasLen
	r.rsMutex.Lock()
	r.rs.SenderStats[s.Name()] = info
	r.rsMutex.Unlock()
	return true
}

func getSampleContent(line string, maxBatchSize int) string {
	if len(line) <= maxBatchSize {
		return line
	}
	if maxBatchSize <= 1024 {
		if len(line) <= 1024 {
			return line
		}
		return line[0:1024]
	}
	return line[0:maxBatchSize]
}

func (r *LogExportRunner) readDatas(dr reader.DataReader, dataSourceTag string) []Data {
	var (
		datas     []Data
		err       error
		bytes     int64
		data      Data
		encodeTag = r.meta.GetEncodeTag()
	)
	for !utils.BatchFullOrTimeout(r.RunnerName, &r.stopped, r.batchLen, r.batchSize, r.lastSend,
		r.MaxBatchLen, r.MaxBatchSize, r.MaxBatchInterval) {
		data, bytes, err = dr.ReadData()
		if err != nil {
			log.Errorf("Runner[%v] data reader %s - error: %v, sleep 1 second...", r.Name(), r.reader.Name(), err)
			time.Sleep(time.Second)
			break
		}
		if len(data) <= 0 {
			log.Debugf("Runner[%v] data reader %s got empty data", r.Name(), r.reader.Name())
			continue
		}
		if len(dataSourceTag) > 0 {
			data[dataSourceTag] = r.reader.Source()
		}

		if len(encodeTag) > 0 {
			data[encodeTag] = r.meta.GetEncodingWay()
		}
		datas = append(datas, data)
		r.batchLen++
		r.batchSize += bytes
	}
	r.rsMutex.Lock()
	if err != nil {
		r.rs.ReaderStats.LastError = TruncateStrSize(err.Error(), DefaultTruncateMaxSize)
		r.historyMutex.Lock()
		if r.historyError.ReadErrors == nil {
			r.historyError.ReadErrors = equeue.New(r.ErrorsListCap)
		}
		r.historyError.ReadErrors.Put(equeue.NewError(r.rs.ReaderStats.LastError))
		r.historyMutex.Unlock()
	} else {
		r.rs.ReaderStats.LastError = ""
	}
	r.rsMutex.Unlock()
	return datas
}

func (r *LogExportRunner) rawReadLines(dataSourceTag string) (lines, froms []string) {
	var line string
	var err error
	for !utils.BatchFullOrTimeout(r.RunnerName, &r.stopped, r.batchLen, r.batchSize, r.lastSend,
		r.MaxBatchLen, r.MaxBatchSize, r.MaxBatchInterval) {
		line, err = r.reader.ReadLine()
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] reader %s - error: %v, sleep 3 second...", r.Name(), r.reader.Name(), err)
			time.Sleep(3 * time.Second)
			break
		}
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
		if strings.TrimSpace(line) == "" {
			continue
		}
		lines = append(lines, line)
		if dataSourceTag != "" {
			froms = append(froms, r.reader.Source())
		}

		r.batchLen++
		r.batchSize += int64(len(line))
	}
	r.rsMutex.Lock()
	if err != nil && err != io.EOF {
		if os.IsNotExist(err) {
			r.rs.ReaderStats.LastError = "no more file exist to be read"
		} else {
			r.rs.ReaderStats.LastError = TruncateStrSize(err.Error(), DefaultTruncateMaxSize)
		}
		r.historyMutex.Lock()
		if r.historyError.ReadErrors == nil {
			r.historyError.ReadErrors = equeue.New(r.ErrorsListCap)
		}
		r.historyError.ReadErrors.Put(equeue.NewError(r.rs.ReaderStats.LastError))
		r.historyMutex.Unlock()
	} else {
		r.rs.ReaderStats.LastError = ""
	}
	r.rsMutex.Unlock()
	return lines, froms
}

func (r *LogExportRunner) readLines(dataSourceTag string) []Data {
	var (
		err        error
		curTimeStr string
	)
	lines, froms := r.rawReadLines(dataSourceTag)
	r.tracker.Track("finish rawReadLines")
	for i := range r.transformers {
		if r.transformers[i].Stage() == transforms.StageBeforeParser {
			lines, err = r.transformers[i].RawTransform(lines)
			if err != nil {
				log.Errorf("runner[%v]: error %v", r.RunnerName, err)
			}
		}
	}

	if r.ReadTime {
		curTimeStr = time.Now().Format("2006-01-02 15:04:05.999")
	}

	linenums := len(lines)
	if linenums <= 0 {
		log.Debugf("Runner[%v] fetched 0 lines", r.Name())
		_, ok := r.parser.(parser.Flushable)
		if ok {
			lines = []string{config.PandoraParseFlushSignal}
		} else {
			return nil
		}
	}

	// parse data
	var numErrs int64
	datas, err := r.parser.Parse(lines)
	r.tracker.Track("finish parse data")
	se, ok := err.(*StatsError)
	r.rsMutex.Lock()
	if ok {
		if se.Errors == 0 && se.LastError == "" {
			err = nil
		} else {
			numErrs = se.Errors
			err = errors.New(se.LastError)
			r.rs.ParserStats.Errors += se.Errors
		}
		r.rs.ParserStats.Success += se.Success
	} else if err != nil {
		numErrs = 1
		r.rs.ParserStats.Errors++
	} else {
		r.rs.ParserStats.Success += int64(linenums)
	}
	if err != nil {
		r.rs.ParserStats.LastError = TruncateStrSize(err.Error(), DefaultTruncateMaxSize)
		r.historyMutex.Lock()
		if r.historyError.ParseErrors == nil {
			r.historyError.ParseErrors = equeue.New(r.ErrorsListCap)
		}
		r.historyError.ParseErrors.Put(equeue.NewError(r.rs.ParserStats.LastError))
		r.historyMutex.Unlock()
	}
	r.rsMutex.Unlock()
	if err != nil {
		errMsg := fmt.Sprintf("Runner[%v] parser %s error : %v ", r.Name(), r.parser.Name(), err.Error())
		log.Debugf(errMsg)
		(&SchemaErr{}).Output(numErrs, errors.New(errMsg))
	}

	// send data
	if len(datas) <= 0 {
		log.Debugf("Runner[%v] received parsed data length = 0", r.Name())
		return []Data{}
	}

	tags := r.meta.GetTags()
	if r.ExtraInfo {
		tags = MergeEnvTags(r.EnvTag, tags)
	}
	tags = MergeExtraInfoTags(r.meta, tags)
	if r.ReadTime {
		tags["lst"] = curTimeStr
	}
	if len(tags) > 0 {
		datas = AddTagsToData(tags, datas, r.Name())
	}

	// 把 source 加到 data 里，前提是认为 []line 变成 []data 以后是一一对应的，一旦错位就不加
	if dataSourceTag != "" {
		// 只要实际解析后数据不大于 froms 就可以填上
		if len(datas) <= len(froms) {
			datas = addSourceToData(froms, se, datas, dataSourceTag, r.Name())
		} else {
			var selen int
			if se != nil {
				selen = len(se.DatasourceSkipIndex)
				log.Debugf("Runner[%v] datasourcetag add error, datas %v datasourceSkipIndex %v froms %v", r.Name(), datas, se.DatasourceSkipIndex, froms)
			}
			log.Errorf("Runner[%v] datasourcetag add error, datas(TOTAL %v), datasourceSkipIndex(TOTAL %v) not match with froms(TOTAL %v)", r.Name(), len(datas), selen, len(froms))
		}
	}
	encodeTag := r.meta.GetEncodeTag()
	if encodeTag != "" {
		addEncodeToData(datas, encodeTag, r.meta.GetEncodingWay(), r.Name())
	}
	return datas
}

func (r *LogExportRunner) addResetStat() {
	r.rsMutex.Lock()
	r.rs.ReaderStats.Success = r.batchLen
	r.rs.ReadDataCount += r.batchLen
	r.rs.ReadDataSize += r.batchSize
	r.rsMutex.Unlock()

	r.batchLen = 0
	r.batchSize = 0
	r.lastSend = time.Now()
}

func (r *LogExportRunner) syncAndLog(batchlen, batchSize, sendDataLen int64) {
	if r.SyncEvery > 0 {
		r.syncInc = (r.syncInc + 1) % r.SyncEvery
		if r.syncInc == 0 {
			r.reader.SyncMeta()
		}
	}

	//审计日志发送选项开启并且runner在运行
	if r.LogAudit && atomic.LoadInt32(&r.stopped) <= 0 {
		var lag int64
		//当延迟数据是最近一分钟内时才获取，数据更新依赖心跳，但是不影响性能
		r.rsMutex.RLock()
		if r.rs != nil && r.rs.lastState.Add(time.Minute).After(time.Now()) {
			lag = r.rs.Lag.Size
		}
		r.rsMutex.RUnlock()
		r.auditChan <- audit.Message{
			Runnername: r.RunnerName,
			Timestamp:  time.Now().UnixNano() / 1000000,
			ReadBytes:  batchSize,
			ReadLines:  batchlen,
			SendLines:  sendDataLen,
			RunnerNote: r.Note,
			Lag:        lag,
		}
	}
}

func (r *LogExportRunner) Run() {
	if r.SyncEvery == 0 {
		r.SyncEvery = 1
	}
	if dr, ok := r.reader.(reader.DaemonReader); ok {
		if err := dr.Start(); err != nil {
			log.Errorf("Runner[%v] start reader daemon failed: %v", r.RunnerName, err)
		}
	}
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

	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			log.Debugf("Runner[%v] exited from run", r.Name())
			r.reader.SyncMeta()
			if atomic.LoadInt32(&r.stopped) < 2 {
				r.exitChan <- struct{}{}
			}
			return
		}
		r.tracker.Reset()
		if r.SendRaw {
			lines, _ := r.rawReadLines(r.meta.GetDataSourceTag())
			r.tracker.Track("finish rawReadLines")
			batchLen, batchSize := r.batchLen, r.batchSize
			r.addResetStat()
			// send data
			if len(lines) <= 0 {
				log.Debugf("Runner[%v] received read data length = 0", r.Name())
				continue
			}
			log.Debugf("Runner[%v] reader %s start to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
			success := true
			dataLen := len(lines)
			for _, s := range r.senders {
				if !r.tryRawSend(s, lines, r.MaxBatchTryTimes) {
					success = false
					log.Errorf("Runner[%v] failed to send data finally", r.Name())
					break
				}
			}
			r.tracker.Track("finish Sender")
			if success {
				r.syncAndLog(batchLen, batchSize, int64(dataLen))
			}
			log.Debugf("Runner[%v] send %s finish to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
			log.Debug(r.tracker.Print())
			continue
		}
		// read data
		var err error
		var datas []Data
		if dr, ok := r.reader.(reader.DataReader); ok {
			datas = r.readDatas(dr, r.meta.GetDataSourceTag())
			r.tracker.Track("finish readDatas")
		} else {
			datas = r.readLines(r.meta.GetDataSourceTag())
			r.tracker.Track("finish readLines")
		}
		batchLen, batchSize := r.batchLen, r.batchSize
		r.addResetStat()
		if len(datas) <= 0 {
			continue
		}

		for i := range r.transformers {
			if r.transformers[i].Stage() != transforms.StageAfterParser {
				continue
			}
			datas, err = r.transformers[i].Transform(datas)
			tp := r.transformers[i].Type()
			r.rsMutex.Lock()
			tstats, ok := r.rs.TransformStats[formatTransformName(tp, i)]
			if !ok {
				tstats = StatsInfo{}
			}
			se, ok := err.(*StatsError)
			if ok {
				err = errors.New(se.LastError)
				tstats.Errors += se.Errors
				tstats.Success += se.Success
			} else if err != nil {
				tstats.Errors++
			} else {
				tstats.Success++
			}
			if err != nil {
				statesTransformer, ok := r.transformers[i].(transforms.StatsTransformer)
				if ok {
					statesTransformer.SetStats(err.Error())
				}
				tstats.LastError = TruncateStrSize(err.Error(), DefaultTruncateMaxSize)
				r.historyMutex.Lock()
				if r.historyError.TransformErrors == nil {
					r.historyError.TransformErrors = make(map[string]*equeue.ErrorQueue)
				}
				if r.historyError.TransformErrors[tp] == nil {
					r.historyError.TransformErrors[tp] = equeue.New(r.ErrorsListCap)
				}
				r.historyError.TransformErrors[tp].Put(equeue.NewError(tstats.LastError))
				r.historyMutex.Unlock()
			}

			r.rs.TransformStats[tp] = tstats
			r.rsMutex.Unlock()
			if err != nil {
				log.Errorf("runner[%v]: error %v", r.RunnerName, err)
			}
		}
		r.tracker.Track("finish transformers")
		dataLen := len(datas)
		log.Debugf("Runner[%v] reader %s start to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
		success := true
		senderDataList := classifySenderData(r.senders, datas, r.router)
		for index, s := range r.senders {
			if !r.trySend(s, senderDataList[index], r.MaxBatchTryTimes) {
				success = false
				log.Errorf("Runner[%v] failed to send data finally", r.Name())
				break
			}
		}
		r.tracker.Track("finish Sender")

		if success {
			r.syncAndLog(batchLen, batchSize, int64(dataLen))
		}
		log.Debugf("Runner[%v] send %s finish to send at: %v", r.Name(), r.reader.Name(), time.Now().Format(time.RFC3339))
		log.Debug(r.tracker.Print())
	}
}

func classifySenderData(senders []sender.Sender, datas []Data, router *router.Router) [][]Data {
	// 只有一个或是最后一个 sender 的时候无所谓数据污染
	skipCopyAll := len(senders) <= 1
	lastIdx := len(senders) - 1
	hasRouter := router != nil && router.HasRoutes()
	senderDataList := make([][]Data, len(senders))
	for i := range senders {
		if hasRouter {
			senderDataList[i] = make([]Data, 0)
			continue
		}

		skip := false
		if ss, ok := senders[i].(sender.SkipDeepCopySender); ok {
			skip = ss.SkipDeepCopy()
		}
		if skip || skipCopyAll || i == lastIdx {
			senderDataList[i] = datas
		} else {
			// 数据进行深度拷贝，防止数据污染
			var copiedDatas []Data
			utils.DeepCopyByJSON(&copiedDatas, &datas)
			senderDataList[i] = copiedDatas
		}
	}
	if !hasRouter {
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

func addSourceToData(sourceFroms []string, se *StatsError, datas []Data, datasourceTagName, runnerName string) []Data {
	j := 0
	eql := len(sourceFroms) == len(datas)
	for i, v := range sourceFroms {
		if eql {
			j = i
		} else {
			if se != nil && se.ErrorIndexIn(i) {
				continue
			}
		}
		if j >= len(datas) {
			continue
		}

		if dt, ok := datas[j][datasourceTagName]; ok {
			log.Debugf("Runner[%v] datasource tag already has data %v, ignore %v", runnerName, dt, v)
		} else {
			datas[j][datasourceTagName] = v
		}
		j++
	}
	return datas
}

func addEncodeToData(datas []Data, encodeTag, encode, runnerName string) {
	for idx := range datas {
		if dt, ok := datas[idx][encodeTag]; ok {
			log.Debugf("Runner[%v] encode tag already has data %v, ignore %v", runnerName, dt, encode)
		} else {
			datas[idx][encodeTag] = encode
		}
	}
}

// Stop 清理所有使用到的资源, 等待10秒尝试读取完毕
// 先停Reader，不再读取，然后停Run函数，让读取的都转到发送，最后停Sender结束整个过程。
// Parser 无状态，无需stop。
func (r *LogExportRunner) Stop() {
	log.Infof("Runner[%v] wait for reader %v to stop", r.Name(), r.reader.Name())
	err := r.reader.Close()
	if err != nil {
		log.Errorf("Runner[%v] cannot close reader name: %s, err: %v", r.Name(), r.reader.Name(), err)
	} else {
		log.Warnf("Runner[%v] reader %v of runner %v closed", r.Name(), r.reader.Name(), r.Name())
	}

	if r.RunnerInfo.MaxReaderCloseWaitTime > 0 {
		log.Infof("Runner[%v] wait for reader close %ds", r.Name(), r.RunnerInfo.MaxReaderCloseWaitTime)
		time.Sleep(time.Second * time.Duration(r.RunnerInfo.MaxReaderCloseWaitTime))
	}

	atomic.AddInt32(&r.stopped, 1)

	log.Infof("Runner[%v] waiting for Run() stopped signal", r.Name())
	timer := time.NewTimer(time.Second * 10)
	defer timer.Stop()
	select {
	case <-r.exitChan:
		log.Warnf("runner %v has been stopped", r.Name())
	case <-timer.C:
		log.Errorf("runner %v exited timeout, start to force stop", r.Name())
		atomic.AddInt32(&r.stopped, 1)
	}

	for _, t := range r.transformers {
		if c, ok := t.(io.Closer); ok {
			if err := c.Close(); err != nil {
				log.Warnf("Close transform failed, %v", err)
			}
		}
	}

	log.Infof("Runner[%v] wait for sender %v to stop", r.Name(), r.reader.Name())
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
	log.Infof("Runner[%v] stopped successfully", r.Name())
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

func (r *LogExportRunner) Delete() (err error) {
	return r.meta.Delete()
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

func (r *LogExportRunner) LagStats() (rl *LagInfo, err error) {
	lr, ok := r.reader.(reader.LagReader)
	if ok {
		return lr.Lag()
	}
	//接口不支持，不显示错误比较好，有限reader就是不存在lag的概念的。
	rl = &LagInfo{}
	return
}

func getTrend(old, new int64) string {
	if old <= new-1 {
		return SpeedUp
	}
	if old >= new+1 {
		return SpeedDown
	}
	return SpeedStable
}

func (r *LogExportRunner) GetErrors() ErrorsResult {
	r.historyMutex.RLock()
	defer r.historyMutex.RUnlock()
	if r.historyError != nil {
		return r.historyError.List()
	}
	return ErrorsResult{}
}

func (r *LogExportRunner) getStatusFrequently(now time.Time) (bool, float64, RunnerStatus) {
	r.rsMutex.RLock()
	defer r.rsMutex.RUnlock()
	elaspedTime := now.Sub(r.rs.lastState).Seconds()
	if elaspedTime <= 3 {
		return true, elaspedTime, r.lastRs.Clone()
	}
	return false, elaspedTime, RunnerStatus{}
}

func (r *LogExportRunner) Status() (rs RunnerStatus) {
	var isFre bool
	var elaspedTime float64
	now := time.Now()
	if isFre, elaspedTime, rs = r.getStatusFrequently(now); isFre {
		return rs
	}
	return r.getRefreshStatus(elaspedTime)
}

func (r *LogExportRunner) getRefreshStatus(elaspedtime float64) RunnerStatus {
	now := time.Now()
	r.rsMutex.Lock()
	defer r.rsMutex.Unlock()
	r.rs.Error = ""
	r.rs.Logpath = r.meta.LogPath()
	rl, err := r.LagStats()
	if err != nil {
		r.rs.Error = fmt.Sprintf("get lag error: %v", err)
		log.Warn(r.rs.Error)
	}
	if rl != nil {
		r.rs.Lag = *rl
	}

	r.rs.Elaspedtime += elaspedtime
	r.rs.lastState = now
	for i := range r.transformers {
		newtsts := r.transformers[i].Stats()
		ttp := r.transformers[i].Type()
		if oldtsts, ok := r.lastRs.TransformStats[ttp]; ok {
			newtsts.Speed, newtsts.Trend = calcSpeedTrend(oldtsts, newtsts, elaspedtime)
		} else {
			newtsts.Speed, newtsts.Trend = calcSpeedTrend(StatsInfo{}, newtsts, elaspedtime)
		}
		newtsts.LastError = TruncateStrSize(newtsts.LastError, DefaultTruncateMaxSize)
		r.rs.TransformStats[formatTransformName(ttp, i)] = newtsts
	}

	/*
		此处先不用reader的status, Run函数本身对这个ReaderStats赋值
		if str, ok := r.reader.(reader.StatsReader); ok {
			r.rs.ReaderStats = str.Status()
		}
	*/

	r.rs.ReadSpeedKB = int64(float64(r.rs.ReadDataSize-r.lastRs.ReadDataSize) / elaspedtime)
	r.rs.ReadSpeedTrendKb = getTrend(r.lastRs.ReadSpeedKB, r.rs.ReadSpeedKB)
	r.rs.ReadSpeed = int64(float64(r.rs.ReadDataCount-r.lastRs.ReadDataCount) / elaspedtime)
	r.rs.ReadSpeedTrend = getTrend(r.lastRs.ReadSpeed, r.rs.ReadSpeed)
	r.rs.ReaderStats.Speed = r.rs.ReadSpeed
	r.rs.ReaderStats.Trend = r.rs.ReadSpeedTrend
	r.rs.ReaderStats.Success = r.rs.ReadDataCount

	//对于DataReader，不需要Parser，默认全部成功
	if _, ok := r.reader.(reader.DataReader); ok || r.SendRaw {
		r.rs.ParserStats.Success = r.rs.ReadDataCount
		r.rs.ParserStats.Speed = r.rs.ReadSpeed
		r.rs.ParserStats.Trend = r.rs.ReadSpeedTrend
	} else {
		r.rs.ParserStats.Speed, r.rs.ParserStats.Trend = calcSpeedTrend(r.lastRs.ParserStats, r.rs.ParserStats, elaspedtime)
	}

	for i := range r.senders {
		sts, ok := r.senders[i].(sender.StatsSender)
		if ok {
			senderStats := sts.Stats()
			senderStats.LastError = TruncateStrSize(senderStats.LastError, DefaultTruncateMaxSize)
			r.rs.SenderStats[r.senders[i].Name()] = senderStats
		}
	}

	for k, v := range r.rs.SenderStats {
		if lv, ok := r.lastRs.SenderStats[k]; ok {
			v.Speed, v.Trend = calcSpeedTrend(lv, v, elaspedtime)
		} else {
			v.Speed, v.Trend = calcSpeedTrend(StatsInfo{}, v, elaspedtime)
		}
		r.rs.SenderStats[k] = v
	}
	r.rs.RunningStatus = RunnerRunning
	*r.lastRs = r.rs.Clone()
	return *r.lastRs
}

func calcSpeedTrend(old, new StatsInfo, elaspedtime float64) (speed int64, trend string) {
	if elaspedtime < 0.001 {
		speed = old.Speed
	} else {
		speed = int64(float64(new.Success-old.Success) / elaspedtime)
	}
	trend = getTrend(old.Speed, speed)
	return
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
	parserType, err := rc.ParserConf.GetString(config.KeyParserType)
	if err != nil {
		return rc
	}
	pattern, _ := rc.ReaderConfig.GetStringOr(KeyHeadPattern, "")
	if parserType == config.TypeLogv1 && pattern == "" {
		prefix, _ := rc.ParserConf.GetStringOr(qiniu.KeyPrefix, "")
		prefix = strings.TrimSpace(prefix)
		var readpattern string
		if len(prefix) > 0 {
			readpattern = "^" + prefix + " " + qiniu.HeadPatthern
		} else {
			readpattern = "^" + qiniu.HeadPatthern
		}
		rc.ReaderConfig[KeyHeadPattern] = readpattern
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

func restoreErrorStatisic(sts ErrorStatistic) *equeue.ErrorQueue {
	maxSize := sts.GetMaxSize()
	q := equeue.New(maxSize)
	if sts.IsNewVersion() {
		q.Append(sts.ErrorSlice)
		return q
	}

	//对于没有满的情况，即队列没有满的情况，直接从头读到尾
	if sts.Rear > sts.Front && (sts.Rear+1)%maxSize != sts.Front {
		for i := sts.Front; i < sts.Rear && i < len(sts.ErrorSlice); i++ {
			q.Put(sts.ErrorSlice[i])
		}
		return q
	}
	//根本没错误
	if len(sts.ErrorSlice) <= 0 {
		return q
	}
	//到这里说明队列已经满了，那么循环队列大小次数，获得所有值
	idx := sts.Front
	for i := 0; i < maxSize; i++ {
		if idx >= len(sts.ErrorSlice) {
			log.Warnf("statistic restore error, index should never larger than slice capacity")
			break
		}
		q.Put(sts.ErrorSlice[idx])
		idx = (idx + 1) % maxSize
	}
	return q
}

// StatusRestore 除了恢复Status的数据之外，还会恢复historyError数据，因为重构前混到一起，导致备份写到同一个statistics.meta文件中
func (r *LogExportRunner) StatusRestore() {
	rStat, err := r.meta.ReadStatistic()
	if err != nil {
		log.Warnf("Runner[%v] restore status failed: %v", r.RunnerName, err)
		return
	}
	r.rs.ReadDataCount = rStat.ReaderCnt
	r.rs.ParserStats.Success = rStat.ParserCnt[0]
	r.rs.ParserStats.Errors = rStat.ParserCnt[1]

	//恢复Errorlist
	r.historyMutex.Lock()
	defer r.historyMutex.Unlock()
	r.historyError.ReadErrors = restoreErrorStatisic(rStat.ReadErrors)
	r.historyError.ParseErrors = restoreErrorStatisic(rStat.ParseErrors)

	if len(rStat.TransformErrors) > 0 {
		r.historyError.TransformErrors = make(map[string]*equeue.ErrorQueue)
	}
	for idx, t := range r.transformers {
		transformErrors, exist := rStat.TransformErrors[formatTransformName(t.Type(), idx)]
		if !exist {
			continue
		}
		r.historyError.TransformErrors[formatTransformName(t.Type(), idx)] = restoreErrorStatisic(transformErrors)
	}
	if len(rStat.SendErrors) > 0 {
		r.historyError.SendErrors = make(map[string]*equeue.ErrorQueue)
	}
	for _, s := range r.senders {
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
		status, ext := r.rs.SenderStats[name]
		if !ext {
			status = StatsInfo{}
		}
		status.Success = info[0]
		status.Errors = info[1]
		r.rs.SenderStats[name] = status

		if len(rStat.SendErrors) == 0 {
			continue
		}
		sendErrors, exist := rStat.SendErrors[name]
		if !exist {
			continue
		}
		r.historyError.SendErrors[name] = restoreErrorStatisic(sendErrors)
	}
	tmp := r.rs.Clone()
	r.lastRs = &tmp

	log.Infof("runner %v restore status read count: %v, parse count: %v, send count: %v", r.RunnerName,
		rStat.ReaderCnt, rStat.ParserCnt, rStat.SenderCnt)
}

func formatTransformName(tp string, idx int) string {
	return fmt.Sprintf("%s-%v", tp, idx)
}

// StatusBackup 除了备份Status的数据之外，还会备份historyError数据，因为重构前混到一起，导致备份写到同一个statistics.meta文件中
func (r *LogExportRunner) StatusBackup() {
	status := r.Status()
	bStart := &reader.Statistic{
		ReaderCnt: status.ReadDataCount,
		ParserCnt: [2]int64{
			status.ParserStats.Success,
			status.ParserStats.Errors,
		},
		TransCnt:  map[string][2]int64{},
		SenderCnt: map[string][2]int64{},
	}
	r.historyMutex.Lock()
	defer r.historyMutex.Unlock()
	if r.historyError.HasReadErr() {
		bStart.ReadErrors.ErrorSlice = r.historyError.ReadErrors.List()
		bStart.ReadErrors.MaxSize = r.historyError.ReadErrors.GetMaxSize()
	}
	if r.historyError.HasParseErr() {
		bStart.ParseErrors.ErrorSlice = r.historyError.ParseErrors.List()
		bStart.ParseErrors.MaxSize = r.historyError.ParseErrors.GetMaxSize()
	}

	for idx, t := range r.transformers {
		name := formatTransformName(t.Type(), idx)
		sta := t.Stats()
		bStart.SenderCnt[name] = [2]int64{
			sta.Success,
			sta.Errors,
		}
	}

	if r.historyError.HasTransformErr() {
		bStart.TransformErrors = make(map[string]ErrorStatistic)
		for name, transformErrors := range r.historyError.TransformErrors {
			if transformErrors.Empty() {
				continue
			}
			bStart.TransformErrors[name] = ErrorStatistic{
				ErrorSlice: transformErrors.List(),
				MaxSize:    transformErrors.GetMaxSize(),
			}
		}
	}

	for _, s := range r.senders {
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
	if r.historyError.HasSendErr() {
		bStart.SendErrors = make(map[string]ErrorStatistic)
		for send, sendErrors := range r.historyError.SendErrors {
			if sendErrors.Empty() {
				continue
			}
			bStart.SendErrors[send] = ErrorStatistic{
				ErrorSlice: sendErrors.List(),
				MaxSize:    sendErrors.GetMaxSize(),
			}
		}
	}

	err := r.meta.WriteStatistic(bStart)
	if err != nil {
		log.Warnf("runner %v, backup status failed", r.RunnerName)
	}
}

func MergeExtraInfoTags(meta *reader.Meta, tags map[string]interface{}) map[string]interface{} {
	if tags == nil {
		tags = make(map[string]interface{})
	}
	for k, v := range meta.ExtraInfo() {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
	}
	return tags
}

func setPandoraServerConfig(senderConfig conf.MapConf, serverConfigs []map[string]interface{}) (conf.MapConf, error) {
	if senderConfig[senderConf.KeySenderType] != senderConf.TypePandora {
		return senderConfig, nil
	}

	var err error
	for _, serverConfig := range serverConfigs {
		keyType, ok := serverConfig[KeyType].(string)
		if !ok {
			continue
		}
		switch keyType {
		case ip.Name, parserconfig.TypeLinuxAudit:
			if senderConfig, err = setIPConfig(senderConfig, serverConfig); err != nil {
				return senderConfig, err
			}
		}
	}

	return senderConfig, nil
}

func setIPConfig(senderConfig conf.MapConf, serverConfig map[string]interface{}) (conf.MapConf, error) {
	key, keyOk := serverConfig["key"].(string)
	if !keyOk {
		return senderConfig, nil
	}

	autoCreate := senderConfig[senderConf.KeyPandoraAutoCreate]
	processAt, processAtOk := serverConfig[ProcessAt].(string)
	if !processAtOk {
		return senderConfig, nil
	}

	senderConfig[senderConf.KeyPandoraAutoCreate] = removeServerIPSchema(senderConfig[senderConf.KeyPandoraAutoCreate], key)
	if processAt == Local {
		return senderConfig, nil
	}

	if len(GetKeys(key)) > 1 {
		return senderConfig, fmt.Errorf("key: %v ip transform key in server doesn't support dot(.)", key)
	}

	if autoCreate == "" {
		senderConfig[senderConf.KeyPandoraAutoCreate] = fmt.Sprintf("%s %s", key, TypeIP)
		return senderConfig, nil
	}

	if !strings.Contains(senderConfig[senderConf.KeyPandoraAutoCreate], fmt.Sprintf("%s %s", key, TypeIP)) {
		senderConfig[senderConf.KeyPandoraAutoCreate] += fmt.Sprintf(",%s %s", key, TypeIP)
	}
	return senderConfig, nil
}

func removeServerIPSchema(autoCreate, key string) string {
	if len(GetKeys(key)) > 1 {
		return autoCreate
	}

	if autoCreate == "" {
		return ""
	}

	ipSchemaWithComma := fmt.Sprintf("%s %s", key, TypeIP)
	index := strings.Index(autoCreate, ipSchemaWithComma)
	var splitEnd, splitStart int
	for ; index != -1; index = strings.Index(autoCreate, ipSchemaWithComma) {
		splitEnd = index
		splitStart = index + len(ipSchemaWithComma)
		if splitEnd != 0 {
			// 不是开头，则为(,%s %s)
			splitEnd -= 1
		} else {
			if splitStart != len(autoCreate) {
				// 开头非结尾，则为(%s %s,)
				splitStart += 1
			}
		}

		autoCreate = autoCreate[:splitEnd] + autoCreate[splitStart:]
	}

	return autoCreate
}
