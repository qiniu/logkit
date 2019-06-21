package self

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/config"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	parserconfig "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/raw"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/bufreader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/sender/pandora"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	DefaultSendTime        = 3
	DefaultSelfLogRepoName = "logkit_self_log"
	DebugPattern           = `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} \[DEBUG\]`
	ValidFilePattern       = "valid_file_pattern"
)

var (
	debugRegex   = regexp.MustCompile(DebugPattern)
	readerConfig = conf.MapConf{
		"runner_name":        DefaultSelfRunnerName,
		"name":               DefaultSelfRunnerName,
		"encoding":           "UTF-8",
		"ignore_file_suffix": ".pid,.swap,.go,.conf,.tar.gz,.tar,.zip,.a,.o,.so",
		"ignore_hidden":      "true",
		"log_path":           "",
		"mode":               "dir",
		"newfile_newline":    "false",
		"read_from":          WhenceNewest,
		"read_same_inode":    "false",
		"skip_first_line":    "false",
		ValidFilePattern:     "logkit.log-*",
		"head_pattern":       `^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} \[(WARN)|(INFO)|(ERROR)]|(DEBUG)\])`,
	}
	parserConfig = conf.MapConf{
		"type":                   "raw",
		"name":                   "parser",
		"disable_record_errdata": "true",
	}
	senderConfig = conf.MapConf{
		"sender_type":               "pandora",
		"pandora_workflow_name":     DefaultSelfLogRepoName,
		"pandora_repo_name":         DefaultSelfLogRepoName,
		"logkit_send_time":          "true",
		"pandora_region":            "nb",
		"pandora_host":              config.DefaultPipelineEndpoint,
		"pandora_schema_free":       "true",
		"pandora_enable_logdb":      "true",
		"pandora_logdb_host":        config.DefaultLogDBEndpoint,
		"pandora_gzip":              "true",
		"pandora_uuid":              "false",
		"pandora_withip":            "false",
		"force_microsecond":         "false",
		"pandora_force_convert":     "false",
		"number_use_float":          "true",
		"ignore_invalid_field":      "true",
		"pandora_auto_convert_date": "false",
		"pandora_unescape":          "true",
		"insecure_server":           "false",
		"runner_name":               DefaultSelfRunnerName,
		"pandora_description":       SelfLogAutoCreateDescription,
	}
)

type LogRunner struct {
	readerConfig      conf.MapConf
	parserConfig      conf.MapConf
	transformerConfig conf.MapConf
	senderConfig      conf.MapConf

	reader reader.Reader
	parser parser.Parser
	sender sender.Sender
	meta   *reader.Meta

	batchLen  int64
	batchSize int64
	lastSend  time.Time
	envTag    string

	stopped  int32
	exitChan chan struct{}
}

func NewLogRunner(rdConf, psConf, sdConf conf.MapConf, envTag string) (*LogRunner, error) {
	if rdConf == nil {
		rdConf = conf.DeepCopy(readerConfig)
	}
	if psConf == nil {
		psConf = conf.DeepCopy(parserConfig)
	}
	if sdConf == nil {
		sdConf = conf.DeepCopy(senderConfig)
	}
	logPath := rdConf["log_path"]
	if logPath == "" {
		dir, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("get system current workdir error %v", err)
		}
		rdConf["log_path"] = filepath.Join(dir, "logkit.log*")
	} else {
		path, err := filepath.Abs(logPath)
		if err != nil {
			return nil, fmt.Errorf("get system current workdir error %v", err)
		}
		rdConf["log_path"] = path
	}

	var (
		rd  reader.Reader
		ps  parser.Parser
		sd  sender.Sender
		err error
	)
	meta, err := reader.NewMetaWithConf(rdConf)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && rd != nil {
			rd.Close()
		}
	}()

	if rd, err = bufreader.NewFileDirReader(meta, rdConf); err != nil {
		return nil, err
	}
	if ps, err = raw.NewParser(psConf); err != nil {
		return nil, err
	}

	if sd, err = pandora.NewSender(sdConf); err != nil {
		return nil, err
	}
	// ft sender
	if sd, err = sender.NewFtSender(sd, sdConf, meta.FtSaveLogPath()); err != nil {
		return nil, err
	}

	return NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, envTag), nil
}

func NewLogRunnerWithService(rdConf, psConf, sdConf conf.MapConf,
	meta *reader.Meta, rd reader.Reader, ps parser.Parser, sd sender.Sender, envTag string) *LogRunner {
	return &LogRunner{
		readerConfig: rdConf,
		parserConfig: psConf,
		senderConfig: sdConf,
		envTag:       envTag,

		reader:   rd,
		parser:   ps,
		sender:   sd,
		meta:     meta,
		exitChan: make(chan struct{}),
	}
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
		lr.batchLen = 0
		lr.batchSize = 0
		lr.lastSend = time.Now()
		if len(datas) <= 0 {
			log.Debugf("Runner[%s] received parsed data length = 0", lr.Name())
			continue
		}

		// send data
		log.Debugf("Runner[%s] reader %s start to send at: %v", lr.Name(), lr.reader.Name(), time.Now().Format(time.RFC3339))
		if err := lr.sender.Send(datas); err != nil {
			log.Debugf("Runner[%s] failed to send data finally, error: %v", lr.Name(), err)
		} else {
			lr.reader.SyncMeta()
		}
		log.Debugf("Runner[%s] send %s finish to send at: %v", lr.Name(), lr.reader.Name(), time.Now().Format(time.RFC3339))
	}
}

func (lr *LogRunner) Stop() error {
	log.Debugf("Runner[%s] wait for reader %v to stop", lr.Name(), lr.reader.Name())
	var errJoin string
	err := lr.reader.Close()
	if err != nil {
		err = fmt.Errorf("Runner[%s] cannot close reader name: %s, err: %v ", lr.Name(), lr.reader.Name(), err)
		errJoin += err.Error()
	}

	atomic.StoreInt32(&lr.stopped, 1)
	log.Debugf("Runner[%s] waiting for Run() stopped signal", lr.Name())
	timer := time.NewTimer(time.Second * 10)
	defer timer.Stop()
	select {
	case <-lr.exitChan:
		log.Debugf("runner %s has been stopped", lr.Name())
	case <-timer.C:
		log.Debugf("runner %s exited timeout, start to force stop", lr.Name())
		atomic.StoreInt32(&lr.stopped, 1)
	}

	log.Debugf("Runner[%s] wait for sender %v to stop", lr.Name(), lr.reader.Name())

	if err := lr.sender.Close(); err != nil {
		log.Debugf("Runner[%v] cannot close sender name: %s, err: %v", lr.Name(), lr.sender.Name(), err)
		errJoin += "\n" + err.Error()
	}

	if err = lr.meta.Delete(); err != nil {
		log.Debugf("Runner[%v] cannot delete meta, err: %v", lr.Name(), err)
		errJoin += "\n" + err.Error()
	}

	if errJoin != "" {
		return errors.New(errJoin)
	}
	log.Infof("Runner[%s] stopped successfully", lr.Name())
	return nil
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
			log.Debugf("Runner[%s] reader %s - error: %v, sleep 3 second...", lr.Name(), lr.reader.Name(), err)
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

		if debugRegex.MatchString(line) {
			continue
		}
		if strings.Contains(line, DefaultSelfRunnerName) {
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
			lines = []string{parserconfig.PandoraParseFlushSignal}
		} else {
			return nil
		}
	}

	// parse data
	datas, err := lr.parser.Parse(lines)
	if err != nil {
		log.Debugf("Runner[%s] parser %s error: %v ", lr.Name(), lr.parser.Name(), err)
	}

	if lr.envTag != "" {
		tags := MergeEnvTags(lr.envTag, nil)
		if len(tags) > 0 {
			datas = AddTagsToData(tags, datas, lr.Name())
		}
	}
	return datas
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

func (lr *LogRunner) GetParserConfig() conf.MapConf {
	return lr.parserConfig
}

func (lr *LogRunner) GetTransformerConfig() conf.MapConf {
	return lr.transformerConfig
}

func (lr *LogRunner) GetSenderConfig() conf.MapConf {
	return lr.senderConfig
}

func SetReaderConfig(readConf conf.MapConf, logpath, filePattern, metapath, from string) conf.MapConf {
	rdConf := conf.DeepCopy(readConf)
	logpath = strings.TrimSpace(logpath)
	if logpath != "" {
		rdConf["log_path"] = logpath
	}
	if filePattern != "" {
		rdConf[ValidFilePattern] = filePattern
	}
	if metapath != "" {
		path, err := filepath.Abs(metapath)
		if err != nil {
			log.Debugf("got metapath[%s] absolute filepath failed: %v", metapath, err)
			rdConf["meta_path"] = path
		} else {
			rdConf["meta_path"] = metapath
		}
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
		rdConf["read_from"] = WhenceNewest
	}

	return rdConf
}

func SetSenderConfig(sendConf conf.MapConf, pandora Pandora) conf.MapConf {
	sdConf := conf.DeepCopy(sendConf)
	logDBHost := strings.TrimSpace(pandora.LogDB)
	if logDBHost != "" {
		sdConf["pandora_logdb_host"] = logDBHost
	}

	pandoraHost := strings.TrimSpace(pandora.Pipeline)
	if pandoraHost != "" {
		sdConf["pandora_host"] = pandoraHost
	}

	pandoraRegion := strings.TrimSpace(pandora.Region)
	if pandoraRegion != "" {
		sdConf["pandora_region"] = pandoraRegion
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
