package self

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/raw"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/reader/tailx"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/sender/pandora"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	DefaultSendTime        = 3
	DefaultSelfLogRepoName = "logkit_self_log"
	DefaultSelfRunnerName  = "LogkitInternalSelfLogRunner"
	DefaultInternalPrefix  = "LogkitInternal"
)

var (
	readerConfig = conf.MapConf{
		"mode":           "tailx",
		"log_path":       "",
		"read_from":      "oldest",
		"encoding":       "UTF-8",
		"datasource_tag": "datasource",
		"expire":         "24h",
		"submeta_expire": "720h",
		"stat_interval":  "3m",
	}
	parserConfig = conf.MapConf{
		"type": "raw",
		"name": "parser",
		"disable_record_errdata": "true",
	}
	senderConfig = conf.MapConf{
		"sender_type":                     "pandora",
		"pandora_workflow_name":           DefaultSelfLogRepoName,
		"pandora_repo_name":               DefaultSelfLogRepoName,
		"logkit_send_time":                "true",
		"pandora_region":                  "nb",
		"pandora_host":                    "https://nb-pipeline.qiniuapi.com",
		"pandora_schema_free":             "true",
		"pandora_enable_logdb":            "true",
		"pandora_logdb_host":              "https://nb-insight.qiniuapi.com",
		"pandora_enable_tsdb":             "false",
		"pandora_tsdb_host":               "https://nb-tsdb.qiniuapi.com",
		"pandora_enable_kodo":             "false",
		"pandora_kodo_low_frequency_file": "false",
		"pandora_gzip":                    "true",
		"pandora_uuid":                    "false",
		"pandora_withip":                  "false",
		"force_microsecond":               "false",
		"pandora_force_convert":           "false",
		"number_use_float":                "true",
		"ignore_invalid_field":            "true",
		"pandora_auto_convert_date":       "false",
		"pandora_unescape":                "true",
		"insecure_server":                 "false",
	}
)

type LogRunner struct {
	readerConfig conf.MapConf
	parserConfig conf.MapConf
	senderConfig conf.MapConf

	reader reader.Reader
	parser parser.Parser
	sender sender.Sender
	meta   *reader.Meta

	batchLen  int64
	batchSize int64
	lastSend  time.Time

	stopped  int32
	exitChan chan struct{}
}

func NewLogRunner(rdConf, psConf, sdConf conf.MapConf) (*LogRunner, error) {
	if rdConf == nil {
		rdConf = readerConfig
	}
	if parserConfig == nil {
		psConf = parserConfig
	}
	if sdConf == nil {
		sdConf = senderConfig
	}
	if rdConf["log_path"] == "" {
		dir, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("get system current workdir error %v, please set rest_dir config", err)
		}
		readerConfig["log_path"] = filepath.Join(dir, "logkit.log*")
	}

	var (
		rd  reader.Reader
		ps  parser.Parser
		sd  sender.Sender
		err error
	)
	meta, err := reader.NewMetaWithConf(readerConfig)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && rd != nil {
			rd.Close()
		}
	}()

	if rd, err = tailx.NewReader(meta, readerConfig); err != nil {
		return nil, err
	}
	if ps, err = raw.NewParser(parserConfig); err != nil {
		return nil, err
	}
	if sd, err = pandora.NewSender(senderConfig); err != nil {
		return nil, err
	}

	return &LogRunner{
		readerConfig: rdConf,
		parserConfig: psConf,
		senderConfig: sdConf,

		reader:   rd,
		parser:   ps,
		sender:   sd,
		meta:     meta,
		exitChan: make(chan struct{}),
	}, nil
}

func (lr *LogRunner) Run() {
	if dr, ok := lr.reader.(reader.DaemonReader); ok {
		if err := dr.Start(); err != nil {
			log.Errorf("Runner[%s] start reader daemon failed: %v", lr.Name(), err)
		}
	}

	defer close(lr.exitChan)
	defer func() {
		// recover when runner is stopped
		if atomic.LoadInt32(&lr.stopped) <= 0 {
			return
		}
		if r := recover(); r != nil {
			log.Errorf("recover when runner is stopped\npanic: %v\nstack: %s", r, debug.Stack())
		}
	}()

	for {
		if atomic.LoadInt32(&lr.stopped) > 0 {
			log.Debugf("Runner[%s] exited from run", lr.Name())
			lr.exitChan <- struct{}{}
			return
		}

		// read and parse data
		datas := lr.readLines()
		if len(datas) <= 0 {
			log.Debugf("Runner[%s] received parsed data length = 0", lr.Name())
			continue
		}

		lr.batchLen = 0
		lr.batchSize = 0
		lr.lastSend = time.Now()

		// send data
		log.Debugf("Runner[%s] reader %s start to send at: %v", lr.Name(), lr.reader.Name(), time.Now().Format(time.RFC3339))
		success := true
		if !lr.trySend(lr.sender, datas) {
			success = false
			log.Errorf("Runner[%s] failed to send data finally", lr.Name())
			break
		}
		if success {
			lr.reader.SyncMeta()
		}
		log.Debugf("Runner[%s] send %s finish to send at: %v", lr.Name(), lr.reader.Name(), time.Now().Format(time.RFC3339))
	}
	return
}

func (lr *LogRunner) Stop() {
	log.Infof("Runner[%s] wait for reader %v to stop", lr.Name(), lr.reader.Name())
	var errJoin string
	err := lr.reader.Close()
	if err != nil {
		err = fmt.Errorf("Runner[%s] cannot close reader name: %s, err: %v", lr.Name(), lr.reader.Name(), err)
		errJoin += err.Error()
	}

	atomic.StoreInt32(&lr.stopped, 1)
	log.Infof("Runner[%s] waiting for Run() stopped signal", lr.Name())
	timer := time.NewTimer(time.Second * 10)
	select {
	case <-lr.exitChan:
		log.Debugf("runner %s has been stopped", lr.Name())
	case <-timer.C:
		log.Debugf("runner %s exited timeout, start to force stop", lr.Name())
		atomic.StoreInt32(&lr.stopped, 1)
	}

	log.Infof("Runner[%s] wait for sender %v to stop", lr.Name(), lr.reader.Name())

	if err := lr.sender.Close(); err != nil {
		log.Debugf("Runner[%v] cannot close sender name: %s, err: %v", lr.Name(), lr.sender.Name(), err)
	}

	log.Infof("Runner[%s] stopped successfully", lr.Name())
}

func (lr *LogRunner) Name() string {
	return DefaultSelfRunnerName
}

func (lr *LogRunner) readLines() []Data {
	var (
		err   error
		lines []string
		line  string
	)
	for !utils.BatchFullOrTimeout(lr.Name(), &lr.stopped, lr.batchLen, lr.batchSize, lr.lastSend,
		0, DefaultMaxBatchSize, DefaultSendIntervalSeconds) {
		line, err = lr.reader.ReadLine()
		if os.IsNotExist(err) {
			log.Errorf("Runner[%s] reader %s - error: %v, sleep 3 second...", lr.Name(), lr.reader.Name(), err)
			time.Sleep(3 * time.Second)
			break
		}
		if err != nil && err != io.EOF {
			log.Debugf("Runner[%s] reader %s - error: %v, sleep 1 second...", lr.Name(), lr.reader.Name(), err)
			time.Sleep(time.Second)
			break
		}
		if len(line) <= 0 {
			log.Debugf("Runner[%s] reader %s no more content fetched sleep 1 second...", lr.Name(), lr.reader.Name())
			time.Sleep(time.Second)
			continue
		}

		lines = append(lines, line)

		lr.batchLen++
		lr.batchSize += int64(len(line))
	}

	if len(lines) <= 0 {
		log.Debugf("Runner[%s] fetched 0 lines", lr.Name())
		_, ok := lr.parser.(parser.Flushable)
		if ok {
			lines = []string{config.PandoraParseFlushSignal}
		} else {
			return nil
		}
	}

	// parse data
	datas, err := lr.parser.Parse(lines)
	if err != nil {
		log.Debugf("Runner[%s] parser %s error: %v ", lr.Name(), lr.parser.Name(), err)
	}
	return datas
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func (lr *LogRunner) trySend(s sender.Sender, datas []Data) bool {
	if len(datas) <= 0 {
		return true
	}

	var (
		err error
		cnt = 1
	)

	for {
		// 至少尝试一次。如果任务已经停止，那么只尝试一次
		if cnt > 1 && atomic.LoadInt32(&lr.stopped) > 0 {
			return false
		}

		err = s.Send(datas)
		if err == nil {
			break
		}
		if err == ErrQueueClosed {
			log.Debugf("Runner[%s] send to closed queue, discard datas, send error %v, failed datas (length %d): %v", lr.Name(), err, cnt, datas)
			break
		}
		log.Debug(TruncateStrSize(err.Error(), DefaultTruncateMaxSize))

		if sendError, ok := err.(*reqerr.SendError); ok {
			datas = sender.ConvertDatas(sendError.GetFailDatas())
		}

		if cnt < DefaultSendTime {
			cnt++
			continue
		}
		log.Debugf("Runner[%s] retry send %v times, but still error %v, discard datas %v ... total %d lines", lr.Name(), cnt, err, datas, len(datas))
		break
	}

	return true
}

func (lr *LogRunner) TokenRefresh(token AuthTokens) error {
	if lr.Name() != token.RunnerName {
		return fmt.Errorf("tokens.RunnerName[%s] is not match %v", token.RunnerName, lr.Name())
	}
	if tokenSender, ok := lr.sender.(sender.TokenRefreshable); ok {
		return tokenSender.TokenRefresh(token.SenderTokens)
	}
	return nil
}

func (lr *LogRunner) GetReaderConfig() conf.MapConf {
	return lr.readerConfig
}

func (lr *LogRunner) GetSenderConfig() conf.MapConf {
	return lr.senderConfig
}

func SetReaderConfig(rdConf conf.MapConf, path, from string) conf.MapConf {
	path = strings.TrimSpace(path)
	if path != "" {
		rdConf["log_path"] = path
	}

	from = strings.TrimSpace(from)
	if from == "" {
		return rdConf
	}
	switch from {
	case WhenceOldest:
		rdConf["read_from"] = WhenceOldest
	case WhenceNewest:
		rdConf["read_from"] = WhenceNewest
	default:
		log.Debugf("reader from %s unsupported", from)
		rdConf["read_from"] = WhenceOldest
	}

	return rdConf
}

func SetSenderConfig(sdConf conf.MapConf, pandora Pandora) conf.MapConf {
	logDBHost := strings.TrimSpace(pandora.Logdb)
	if logDBHost != "" {
		sdConf["pandora_logdb_host"] = logDBHost
	}

	pandoraHost := strings.TrimSpace(pandora.Pipline)
	if pandoraHost != "" {
		sdConf["pandora_host"] = pandoraHost
	}

	pandoraRegion := strings.TrimSpace(pandora.Region)
	if pandoraRegion != "" {
		sdConf["pandora_region"] = pandoraRegion
	}

	tsdbHost := strings.TrimSpace(pandora.Tsdb)
	if tsdbHost != "" {
		sdConf["pandora_tsdb_host"] = tsdbHost
	}

	name := strings.TrimSpace(pandora.Name)
	if name != "" {
		sdConf["pandora_workflow_name"] = name
		sdConf["pandora_repo_name"] = name
	}

	ak := strings.TrimSpace(pandora.AK)
	if ak != "" {
		sdConf["pandora_ak"] = ak
	}

	sk := strings.TrimSpace(pandora.SK)
	if sk != "" {
		sdConf["pandora_sk"] = sk
	}
	return sdConf
}

func GetReaderConfig() conf.MapConf {
	return readerConfig
}

func GetParserConfig() conf.MapConf {
	return parserConfig
}

func GetSenderConfig() conf.MapConf {
	return senderConfig
}
