package pandora

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	gouuid "github.com/satori/go.uuid"

	"github.com/qiniu/log"
	pipelinebase "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/pipeline"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

var osInfo = []string{KeyCore, KeyHostName, KeyOsInfo, KeyLocalIp}

const (
	SendTypeRaw    = "raw"
	SendTypeNormal = "normal"
)

// pandora sender
type Sender struct {
	client             pipeline.PipelineAPI
	schemas            map[string]pipeline.RepoSchemaEntry
	schemasMux         sync.RWMutex
	lastUpdate         time.Time
	UserSchema         UserSchema
	alias2key          map[string]string // map[alias]name
	opt                PandoraOption
	microsecondCounter uint64
	extraInfo          map[string]string
	sendType           string
}

// UserSchema was parsed pandora schema from user's raw schema
type UserSchema struct {
	DefaultAll bool
	Fields     map[string]string
}

type Tokens struct {
	LogDBTokens      pipeline.AutoExportLogDBTokens
	TsDBTokens       pipeline.AutoExportTSDBTokens
	KodoTokens       pipeline.AutoExportKodoTokens
	SchemaFreeTokens pipeline.SchemaFreeToken
}

// PandoraOption 创建Pandora Sender的选项
type PandoraOption struct {
	sendType string

	runnerName     string
	name           string
	repoName       string
	workflowName   string
	region         string
	endpoint       string
	ak             string
	sk             string
	schema         string
	schemaFree     bool   // schemaFree在用户数据有新字段时就更新repo添加字段，如果repo不存在，创建repo。schemaFree功能包含autoCreate
	autoCreate     string // 自动创建用户的repo，dsl语言填写schema
	updateInterval time.Duration
	reqRateLimit   int64
	flowRateLimit  int64
	gzip           bool
	uuid           bool
	withip         string
	extraInfo      bool

	enableLogdb   bool
	logdbReponame string
	logdbendpoint string
	analyzerInfo  pipeline.AnalyzerInfo

	enableTsdb     bool
	tsdbReponame   string
	tsdbSeriesName string
	tsdbendpoint   string
	tsdbTimestamp  string
	tsdbSeriesTags map[string][]string

	enableKodo         bool
	bucketName         string
	email              string
	prefix             string
	format             string
	kodoCompress       bool
	kodoRotateStrategy string
	kodoRotateInterval int
	kodoRotateSize     int
	kodoFileRetention  int

	forceMicrosecond   bool
	forceDataConvert   bool
	ignoreInvalidField bool
	autoConvertDate    bool
	useragent          string
	logkitSendTime     bool
	UnescapeLine       bool
	insecureServer     bool

	isMetrics      bool
	numberUseFloat bool
	expandAttr     []string

	tokens    Tokens
	tokenLock *sync.RWMutex
}

//PandoraMaxBatchSize 发送到Pandora的batch限制
var PandoraMaxBatchSize = 2 * 1024 * 1024

func init() {
	sender.RegisterConstructor(sender.TypePandora, NewSender)
}

// pandora sender
func NewSender(conf conf.MapConf) (pandoraSender sender.Sender, err error) {
	repoName, err := conf.GetString(sender.KeyPandoraRepoName)
	if err != nil {
		return
	}
	if repoName == "" {
		return nil, errors.New("repoName is empty")
	}
	region, err := conf.GetString(sender.KeyPandoraRegion)
	if err != nil {
		return
	}
	host, err := conf.GetString(sender.KeyPandoraHost)
	if err != nil {
		return
	}
	ak, _ := conf.GetString(sender.KeyPandoraAk)
	akFromEnv := GetEnv(ak)
	if akFromEnv == "" {
		akFromEnv = ak
	}

	sk, _ := conf.GetString(sender.KeyPandoraSk)
	skFromEnv := GetEnv(sk)
	if skFromEnv == "" {
		skFromEnv = sk
	}

	workflowName, _ := conf.GetStringOr(sender.KeyPandoraWorkflowName, "")
	useragent, _ := conf.GetStringOr(sender.InnerUserAgent, "")
	schema, _ := conf.GetStringOr(sender.KeyPandoraSchema, "")
	name, _ := conf.GetStringOr(sender.KeyName, fmt.Sprintf("pandoraSender:(%v,repo:%v,region:%v)", host, repoName, region))
	updateInterval, _ := conf.GetInt64Or(sender.KeyPandoraSchemaUpdateInterval, 300)
	schemaFree, _ := conf.GetBoolOr(sender.KeyPandoraSchemaFree, false)
	forceMicrosecond, _ := conf.GetBoolOr(sender.KeyForceMicrosecond, false)
	autoCreateSchema, _ := conf.GetStringOr(sender.KeyPandoraAutoCreate, "")
	reqRateLimit, _ := conf.GetInt64Or(sender.KeyRequestRateLimit, 0)
	flowRateLimit, _ := conf.GetInt64Or(sender.KeyFlowRateLimit, 0)
	gzip, _ := conf.GetBoolOr(sender.KeyPandoraGzip, false)
	uuid, _ := conf.GetBoolOr(sender.KeyPandoraUUID, false)
	withIp, _ := conf.GetBoolOr(sender.KeyPandoraWithIP, false)
	runnerName, _ := conf.GetStringOr(KeyRunnerName, sender.UnderfinedRunnerName)
	extraInfo, _ := conf.GetBoolOr(sender.KeyPandoraExtraInfo, false)

	enableLogdb, _ := conf.GetBoolOr(sender.KeyPandoraEnableLogDB, false)
	logdbreponame, _ := conf.GetStringOr(sender.KeyPandoraLogDBName, repoName)
	logdbhost, _ := conf.GetStringOr(sender.KeyPandoraLogDBHost, "")
	logdbAnalyzer, _ := conf.GetStringListOr(sender.KeyPandoraLogDBAnalyzer, []string{})
	analyzerMap := convertAnalyzerMap(logdbAnalyzer)

	enableTsdb, _ := conf.GetBoolOr(sender.KeyPandoraEnableTSDB, false)
	tsdbReponame, _ := conf.GetStringOr(sender.KeyPandoraTSDBName, repoName)
	tsdbSeriesName, _ := conf.GetStringOr(sender.KeyPandoraTSDBSeriesName, tsdbReponame)
	tsdbHost, _ := conf.GetStringOr(sender.KeyPandoraTSDBHost, "")
	tsdbTimestamp, _ := conf.GetStringOr(sender.KeyPandoraTSDBTimeStamp, "")
	seriesTags, _ := conf.GetStringListOr(sender.KeyPandoraTSDBSeriesTags, []string{})
	tsdbSeriesTags := map[string][]string{tsdbSeriesName: seriesTags}

	enableKodo, _ := conf.GetBoolOr(sender.KeyPandoraEnableKodo, false)
	kodobucketName, _ := conf.GetStringOr(sender.KeyPandoraKodoBucketName, repoName)
	email, _ := conf.GetStringOr(sender.KeyPandoraEmail, "")
	format, _ := conf.GetStringOr(sender.KeyPandoraKodoCompressPrefix, "parquet")
	prefix, _ := conf.GetStringOr(sender.KeyPandoraKodoFilePrefix, "logkitauto/date=$(year)-$(mon)-$(day)/hour=$(hour)/min=$(min)/$(sec)")
	compress, _ := conf.GetBoolOr(sender.KeyPandoraKodoGzip, false)
	kodoRotateStrategy, _ := conf.GetStringOr(sender.KeyPandoraKodoRotateStrategy, "interval")
	kodoRotateSize, _ := conf.GetIntOr(sender.KeyPandoraKodoRotateSize, pipeline.DefaultLogkitRotateSize)
	kodoRotateSize = kodoRotateSize * 1024
	kodoRotateInterval, _ := conf.GetIntOr(sender.KeyPandoraKodoRotateInterval, 10*60)
	kodoFileRetention, _ := conf.GetIntOr(sender.KeyPandoraKodoFileRetention, 0)

	forceconvert, _ := conf.GetBoolOr(sender.KeyForceDataConvert, false)
	ignoreInvalidField, _ := conf.GetBoolOr(sender.KeyIgnoreInvalidField, true)
	autoconvertDate, _ := conf.GetBoolOr(sender.KeyPandoraAutoConvertDate, true)
	logkitSendTime, _ := conf.GetBoolOr(sender.KeyLogkitSendTime, true)
	isMetrics, _ := conf.GetBoolOr(sender.KeyIsMetrics, false)
	numberUseFloat, _ := conf.GetBoolOr(sender.KeyNumberUseFloat, false)
	unescape, _ := conf.GetBoolOr(sender.KeyPandoraUnescape, false)
	insecureServer, _ := conf.GetBoolOr(sender.KeyInsecureServer, false)

	sendType, _ := conf.GetStringOr(sender.KeyPandoraSendType, SendTypeNormal)

	var subErr error
	var tokens Tokens
	if tokens, subErr = getTokensFromConf(conf); subErr != nil {
		log.Debugf(subErr.Error())
	}

	if skFromEnv == "" && tokens.SchemaFreeTokens.PipelinePostDataToken.Token == "" {
		err = fmt.Errorf("your authrization config is empty, need to config ak/sk or tokens")
		log.Error(err)
		return
	}
	// 当 schema free 为 false 时，需要自动创建 pandora_stash 字段，需要自动创建 pandora_separate_id 字段
	if !schemaFree {
		if autoCreateSchema == "" {
			autoCreateSchema = fmt.Sprintf("%v string,%v string", KeyPandoraStash, KeyPandoraSeparateId)
		} else {
			autoCreateSchema += fmt.Sprintf(",%v string,%v string", KeyPandoraStash, KeyPandoraSeparateId)
		}
	}

	opt := &PandoraOption{
		sendType:       sendType,
		runnerName:     runnerName,
		name:           name,
		workflowName:   workflowName,
		repoName:       repoName,
		region:         region,
		endpoint:       host,
		ak:             akFromEnv,
		sk:             skFromEnv,
		schema:         schema,
		autoCreate:     autoCreateSchema,
		schemaFree:     schemaFree,
		updateInterval: time.Duration(updateInterval) * time.Second,
		reqRateLimit:   reqRateLimit,
		flowRateLimit:  flowRateLimit,
		gzip:           gzip,
		uuid:           uuid,
		extraInfo:      extraInfo,

		enableLogdb:   enableLogdb,
		logdbReponame: logdbreponame,
		logdbendpoint: logdbhost,
		analyzerInfo:  pipeline.AnalyzerInfo{Analyzer: analyzerMap},

		enableTsdb:     enableTsdb,
		tsdbReponame:   tsdbReponame,
		tsdbSeriesName: tsdbSeriesName,
		tsdbSeriesTags: tsdbSeriesTags,
		tsdbendpoint:   tsdbHost,
		tsdbTimestamp:  tsdbTimestamp,

		enableKodo:         enableKodo,
		email:              email,
		bucketName:         kodobucketName,
		format:             format,
		prefix:             prefix,
		kodoCompress:       compress,
		kodoRotateStrategy: kodoRotateStrategy,
		kodoRotateInterval: kodoRotateInterval,
		kodoRotateSize:     kodoRotateSize,
		kodoFileRetention:  kodoFileRetention,

		forceMicrosecond:   forceMicrosecond,
		forceDataConvert:   forceconvert,
		ignoreInvalidField: ignoreInvalidField,
		autoConvertDate:    autoconvertDate,
		useragent:          useragent,
		logkitSendTime:     logkitSendTime,

		numberUseFloat: numberUseFloat,
		isMetrics:      isMetrics,
		UnescapeLine:   unescape,
		insecureServer: insecureServer,

		tokens:    tokens,
		tokenLock: new(sync.RWMutex),
	}
	if withIp {
		opt.withip = "logkitIP"
	}

	return newPandoraSender(opt)
}

func convertAnalyzerMap(analyzerStrs []string) map[string]string {
	analyzerMap := map[string]string{
		KeyCore:     logdb.KeyWordAnalyzer,
		KeyOsInfo:   logdb.KeyWordAnalyzer,
		KeyLocalIp:  logdb.KeyWordAnalyzer,
		KeyHostName: logdb.KeyWordAnalyzer,
	}
	for _, v := range analyzerStrs {
		sps := strings.Fields(v)
		if len(sps) >= 2 {
			analyzerMap[sps[0]] = sps[1]
		}
	}
	return analyzerMap
}

func getTokensFromConf(conf conf.MapConf) (tokens Tokens, err error) {
	// schema free tokens
	preFix := SchemaFreeTokensPrefix
	tokens.SchemaFreeTokens.PipelineGetRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_get_repo_token", "")
	tokens.SchemaFreeTokens.PipelinePostDataToken.Token, _ = conf.GetStringOr(preFix+"pipeline_post_data_token", "")
	tokens.SchemaFreeTokens.PipelineCreateRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_create_repo_token", "")
	tokens.SchemaFreeTokens.PipelineUpdateRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_update_repo_token", "")
	tokens.SchemaFreeTokens.PipelineGetWorkflowToken.Token, _ = conf.GetStringOr(preFix+"pipeline_get_workflow_token", "")
	tokens.SchemaFreeTokens.PipelineStopWorkflowToken.Token, _ = conf.GetStringOr(preFix+"pipeline_stop_workflow_token", "")
	tokens.SchemaFreeTokens.PipelineStartWorkflowToken.Token, _ = conf.GetStringOr(preFix+"pipeline_start_workflow_token", "")
	tokens.SchemaFreeTokens.PipelineCreateWorkflowToken.Token, _ = conf.GetStringOr(preFix+"pipeline_create_workflow_token", "")
	tokens.SchemaFreeTokens.PipelineGetWorkflowStatusToken.Token, _ = conf.GetStringOr(preFix+"pipeline_Get_workflow_status_token", "")

	// logDB tokens
	preFix = LogDBTokensPrefix
	tokens.LogDBTokens.PipelineGetRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_get_repo_token", "")
	tokens.LogDBTokens.PipelineCreateRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_create_repo_token", "")
	tokens.LogDBTokens.CreateLogDBRepoToken.Token, _ = conf.GetStringOr(preFix+"create_logdb_repo_token", "")
	tokens.LogDBTokens.UpdateLogDBRepoToken.Token, _ = conf.GetStringOr(preFix+"update_logdb_repo_token", "")
	tokens.LogDBTokens.GetLogDBRepoToken.Token, _ = conf.GetStringOr(preFix+"get_logdb_repo_token", "")
	tokens.LogDBTokens.CreateExportToken.Token, _ = conf.GetStringOr(preFix+"create_export_token", "")
	tokens.LogDBTokens.UpdateExportToken.Token, _ = conf.GetStringOr(preFix+"update_export_token", "")
	tokens.LogDBTokens.GetExportToken.Token, _ = conf.GetStringOr(preFix+"get_export_token", "")
	tokens.LogDBTokens.ListExportToken.Token, _ = conf.GetStringOr(preFix+"list_export_token", "")

	// tsDB tokens
	preFix = TsDBTokensPrefix
	tokens.TsDBTokens.PipelineGetRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_get_repo_token", "")
	tokens.TsDBTokens.CreateTSDBRepoToken.Token, _ = conf.GetStringOr(preFix+"create_tsdb_repo_token", "")
	tokens.TsDBTokens.ListExportToken.Token, _ = conf.GetStringOr(preFix+"list_export_token", "")

	createSeriesTokenStr, _ := conf.GetStringOr(preFix+"create_tsdb_series_token", "")
	createExTokenStr, _ := conf.GetStringOr(preFix+"create_export_token", "")
	updateExTokenStr, _ := conf.GetStringOr(preFix+"update_export_token", "")
	getExTokenStr, _ := conf.GetStringOr(preFix+"get_export_token", "")

	tokens.TsDBTokens.CreateTSDBSeriesTokens = make(map[string]models.PandoraToken)
	for _, v := range strings.Split(createSeriesTokenStr, ",") {
		tmpArr := strings.Split(strings.TrimSpace(v), " ")
		if len(tmpArr) < 2 {
			err = fmt.Errorf("parser create series token error, string[%v] is invalid, will not use token", v)
			return
		} else {
			tokens.TsDBTokens.CreateTSDBSeriesTokens[tmpArr[0]] = models.PandoraToken{Token: strings.Join(tmpArr[1:], " ")}
		}
	}
	tokens.TsDBTokens.CreateExportToken = make(map[string]models.PandoraToken)
	for _, v := range strings.Split(createExTokenStr, ",") {
		tmpArr := strings.Split(strings.TrimSpace(v), " ")
		if len(tmpArr) < 2 {
			err = fmt.Errorf("parser create export token error, string[%v] is invalid, will not use token", v)
			return
		} else {
			tokens.TsDBTokens.CreateExportToken[tmpArr[0]] = models.PandoraToken{Token: strings.Join(tmpArr[1:], " ")}
		}
	}
	tokens.TsDBTokens.UpdateExportToken = make(map[string]models.PandoraToken)
	for _, v := range strings.Split(updateExTokenStr, ",") {
		tmpArr := strings.Split(strings.TrimSpace(v), " ")
		if len(tmpArr) < 2 {
			err = fmt.Errorf("parser update export token error, string[%v] is invalid, will not use token", v)
			return
		} else {
			tokens.TsDBTokens.UpdateExportToken[tmpArr[0]] = models.PandoraToken{Token: strings.Join(tmpArr[1:], " ")}
		}
	}
	tokens.TsDBTokens.GetExportToken = make(map[string]models.PandoraToken)
	for _, v := range strings.Split(getExTokenStr, ",") {
		tmpArr := strings.Split(strings.TrimSpace(v), " ")
		if len(tmpArr) <= 2 {
			err = fmt.Errorf("parser get export token error, string[%v] is invalid, will not use token", v)
			return
		} else {
			tokens.TsDBTokens.GetExportToken[tmpArr[0]] = models.PandoraToken{Token: strings.Join(tmpArr[1:], " ")}
		}
	}

	// kodo tokens
	preFix = KodoTokensPrefix
	tokens.KodoTokens.PipelineGetRepoToken.Token, _ = conf.GetStringOr(preFix+"pipeline_get_repo_token", "")
	tokens.KodoTokens.CreateExportToken.Token, _ = conf.GetStringOr(preFix+"create_export_token", "")
	tokens.KodoTokens.UpdateExportToken.Token, _ = conf.GetStringOr(preFix+"update_export_token", "")
	tokens.KodoTokens.GetExportToken.Token, _ = conf.GetStringOr(preFix+"get_export_token", "")
	tokens.KodoTokens.ListExportToken.Token, _ = conf.GetStringOr(preFix+"list_export_token", "")
	return
}

func (s *Sender) TokenRefresh(mapConf conf.MapConf) error {
	s.opt.tokenLock.Lock()
	defer s.opt.tokenLock.Unlock()
	if tokens, err := getTokensFromConf(mapConf); err != nil {
		return err
	} else {
		s.opt.tokens = tokens
	}
	return nil
}

func newPandoraSender(opt *PandoraOption) (s *Sender, err error) {
	logger := pipelinebase.NewDefaultLogger()

	if opt.reqRateLimit > 0 {
		log.Warnf("Runner[%v] Sender[%v]: you have limited send speed within %v requests/s", opt.runnerName, opt.name, opt.reqRateLimit)
	}
	if opt.flowRateLimit > 0 {
		log.Warnf("Runner[%v] Sender[%v]: you have limited send speed within %v KB/s", opt.runnerName, opt.name, opt.flowRateLimit)
	}
	userSchema := parseUserSchema(opt.repoName, opt.schema)
	s = &Sender{
		opt:        *opt,
		alias2key:  make(map[string]string),
		UserSchema: userSchema,
		schemas:    make(map[string]pipeline.RepoSchemaEntry),
		extraInfo:  utilsos.GetExtraInfo(),
		sendType:   opt.sendType,
	}

	expandAttr := make([]string, 0)
	if s.opt.isMetrics {
		s.opt.tsdbTimestamp = metric.Timestamp
		metricTags := metric.GetMetricTags()
		if s.opt.extraInfo {
			// 将 osInfo 添加到每个 metric 的 tags 中
			for key, val := range metricTags {
				val = append(val, osInfo...)
				metricTags[key] = val
			}

			// 将 osInfo 中的字段导出到每个 series 中
			expandAttr = append(expandAttr, osInfo...)
		}
		s.opt.tsdbSeriesTags = metricTags
		s.opt.analyzerInfo.Default = logdb.KeyWordAnalyzer
	} else if len(s.opt.analyzerInfo.Analyzer) > 4 {
		//超过4个的情况代表有用户手动输入了字段的分词方式，此时不开启全文索引
		s.opt.analyzerInfo.FullText = false
	} else {
		s.opt.analyzerInfo.FullText = true
	}
	s.opt.expandAttr = expandAttr

	//如果是Raw类型，那么没有固定的ak、sk和repo
	if opt.sendType == SendTypeRaw {
		return s, nil
	}

	/*
		以下是 repo 创建相关的，raw类型的不需要处理
	*/

	config := pipeline.NewConfig().
		WithPipelineEndpoint(opt.endpoint).
		WithAccessKeySecretKey(opt.ak, opt.sk).
		WithLogger(logger).
		WithLoggerLevel(pipelinebase.LogInfo).
		WithRequestRateLimit(opt.reqRateLimit).
		WithFlowRateLimit(opt.flowRateLimit).
		WithGzipData(opt.gzip).
		WithHeaderUserAgent(opt.useragent).WithInsecureServer(opt.insecureServer)
	if opt.logdbendpoint != "" {
		config = config.WithLogDBEndpoint(opt.logdbendpoint)
	}
	client, err := pipeline.New(config)
	if err != nil {
		err = fmt.Errorf("cannot init pipelineClient %v", err)
		return
	}
	s.client = client

	dsl := strings.TrimSpace(opt.autoCreate)
	schemas, err := pipeline.DSLtoSchema(dsl)
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v]: auto create pandora repo error: %v, you can create on pandora portal, ignored...", opt.runnerName, opt.name, err)
		err = nil
	}
	if initErr := s.client.InitOrUpdateWorkflow(&pipeline.InitOrUpdateWorkflowInput{
		// 此处要的 schema 为 autoCreate 中用户指定的，所以 SchemaFree 要恒为 true
		InitOptionChange: true,
		SchemaFree:       true,
		Region:           s.opt.region,
		WorkflowName:     s.opt.workflowName,
		RepoName:         s.opt.repoName,
		Schema:           schemas,
		SchemaFreeToken:  s.opt.tokens.SchemaFreeTokens,
		RepoOptions:      &pipeline.RepoOptions{WithIP: s.opt.withip, UnescapeLine: s.opt.UnescapeLine},
		Option: &pipeline.SchemaFreeOption{
			NumberUseFloat: s.opt.numberUseFloat,
			ToLogDB:        s.opt.enableLogdb,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				OmitEmpty:             true,
				OmitInvalid:           false,
				RepoName:              s.opt.repoName,
				LogRepoName:           s.opt.logdbReponame,
				AnalyzerInfo:          s.opt.analyzerInfo,
				AutoExportLogDBTokens: s.opt.tokens.LogDBTokens,
			},
			ToKODO: s.opt.enableKodo,
			AutoExportToKODOInput: pipeline.AutoExportToKODOInput{
				Retention:            s.opt.kodoFileRetention,
				RepoName:             s.opt.repoName,
				BucketName:           s.opt.bucketName,
				Email:                s.opt.email,
				Prefix:               s.opt.prefix,
				Format:               s.opt.format,
				Compress:             s.opt.kodoCompress,
				AutoExportKodoTokens: s.opt.tokens.KodoTokens,
				RotateStrategy:       s.opt.kodoRotateStrategy,
				RotateSize:           s.opt.kodoRotateSize,
				RotateInterval:       s.opt.kodoRotateInterval,
				RotateSizeType:       "B",
				RotateNumber:         s.opt.kodoRotateSize,
			},
			ToTSDB: s.opt.enableTsdb,
			AutoExportToTSDBInput: pipeline.AutoExportToTSDBInput{
				OmitEmpty:            true,
				OmitInvalid:          false,
				IsMetric:             s.opt.isMetrics,
				ExpandAttr:           s.opt.expandAttr,
				RepoName:             s.opt.repoName,
				TSDBRepoName:         s.opt.tsdbReponame,
				SeriesName:           s.opt.tsdbSeriesName,
				SeriesTags:           s.opt.tsdbSeriesTags,
				Timestamp:            s.opt.tsdbTimestamp,
				AutoExportTSDBTokens: s.opt.tokens.TsDBTokens,
			},
			ForceDataConvert: s.opt.forceDataConvert,
		},
	}); initErr != nil {
		log.Errorf("runner[%v] Sender [%v]: init Workflow error %v", opt.runnerName, opt.name, initErr)
	}
	s.UpdateSchemas()
	return
}

func parseUserSchema(repoName, schema string) (us UserSchema) {
	schema = strings.TrimSpace(schema)
	us.Fields = make(map[string]string)
	us.DefaultAll = false
	if schema == "" {
		us.DefaultAll = true
		return
	}
	fields := strings.Split(schema, ",")
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if f == "..." {
			us.DefaultAll = true
			continue
		}
		var name, alias string
		splits := strings.Fields(f)
		switch len(splits) {
		case 1:
			name, alias = splits[0], splits[0]
		case 2:
			name, alias = splits[0], splits[1]
		default:
			log.Errorf("Repo-%s:pandora sender schema parse error %v was splited out %v not 1 or 2 by ',', ignore this splits...", repoName, f, len(splits))
		}
		if name == "" && alias == "" {
			continue
		}
		us.Fields[name] = alias
	}
	return
}

func (s *Sender) UpdateSchemas() {
	if s.opt.sendType == SendTypeRaw {
		return
	}
	schemas, err := s.client.GetUpdateSchemasWithInput(
		&pipeline.GetRepoInput{
			RepoName:     s.opt.repoName,
			PandoraToken: s.opt.tokens.SchemaFreeTokens.PipelineGetRepoToken,
		})
	if err != nil && (!s.opt.schemaFree || !reqerr.IsNoSuchResourceError(err)) {
		log.Warnf("Runner[%v] Sender[%v]: update pandora repo <%v> schema error %v", s.opt.runnerName, s.opt.name, s.opt.repoName, err)
		return
	}
	if schemas == nil {
		return
	}
	s.updateSchemas(schemas)
}

func (s *Sender) updateSchemas(schemas map[string]pipeline.RepoSchemaEntry) {
	alias2Key := fillAlias2Keys(s.opt.repoName, schemas, s.UserSchema)
	s.schemasMux.Lock()
	s.schemas = schemas
	s.alias2key = alias2Key
	s.lastUpdate = time.Now()
	s.schemasMux.Unlock()
	return
}

func fillAlias2Keys(repoName string, schemas map[string]pipeline.RepoSchemaEntry, us UserSchema) (alias2key map[string]string) {
	alias2key = make(map[string]string)
	for _, schema := range schemas {
		if schema.Required || us.DefaultAll {
			alias2key[schema.Key] = schema.Key
		}
	}
	for name, alias := range us.Fields {
		if _, ok := schemas[alias]; !ok {
			continue
		}
		alias2key[alias] = name
	}
	return
}

const (
	PandoraTypeLong       = "long"
	PandoraTypeFloat      = "float"
	PandoraTypeString     = "string"
	PandoraTypeDate       = "date"
	PandoraTypeBool       = "boolean"
	PandoraTypeArray      = "array"
	PandoraTypeMap        = "map"
	PandoraTypeJsonString = "jsonstring"
)

type forceMicrosecondOption struct {
	nanosecond       uint64
	forceMicrosecond bool
}

//临时方案，转换时间，目前sender这边拿到的都是string，很难确定是什么格式的string
//microsecond: 表示要在当前时间基础上加多少偏移量,只在forceMicrosecond为true的情况下才有效
//forceMicrosecond: 表示是否要对当前时间加偏移量
func convertDate(v interface{}, option forceMicrosecondOption) (d interface{}, err error) {
	var s int64
	switch newv := v.(type) {
	case int64:
		s = newv
	case int:
		s = int64(newv)
	case int32:
		s = int64(newv)
	case int16:
		s = int64(newv)
	case string:
		t, err := times.StrToTime(newv)
		if err != nil {
			return v, err
		}
		s = t.UTC().UnixNano()
	case json.Number:
		if s, err = newv.Int64(); err != nil {
			return v, err
		}
	default:
		return v, fmt.Errorf("can not parse %v type %v as date time", v, reflect.TypeOf(v))
	}
	if option.forceMicrosecond {
		s = alignTimestamp(s, option.nanosecond)
	}
	timestampStr := strconv.FormatInt(s, 10)
	for i := len(timestampStr); i < sender.TimestampPrecision; i++ {
		timestampStr += "0"
	}
	timestampStr = timestampStr[0:sender.TimestampPrecision]
	if s, err = strconv.ParseInt(timestampStr, 10, 64); err != nil {
		return v, err
	}
	d = time.Unix(0, s*int64(time.Nanosecond)).Format(time.RFC3339Nano)
	return
}

// alignTimestamp
// 1. 根据输入时间戳的位数来补齐对应的位数
// 2. 对于精度不是微秒的数据点，加一个扰动
func alignTimestamp(t int64, nanosecond uint64) int64 {
	for i := 0; t%10 == 0; i++ {
		t /= 10
	}
	offset := sender.TimestampPrecision - len(strconv.FormatInt(t, 10))
	dividend := int64(math.Pow10(offset))
	if offset > 0 {
		t = t * dividend //补齐相应的位数
		return t + int64(nanosecond%uint64(dividend))
	}
	return t
}

func validSchema(valueType string, value interface{}, numberAsFloat bool) bool {
	if value == nil {
		return false
	}
	switch valueType {
	case PandoraTypeLong:
		if numberAsFloat {
			v := fmt.Sprintf("%v", value)
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return false
			}
		} else {
			v := fmt.Sprintf("%v", value)
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				return false
			}
		}
	case PandoraTypeFloat:
		v := fmt.Sprintf("%v", value)
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return false
		}
	case PandoraTypeString:
		//所有数据在pandora协议中均是string
	case PandoraTypeDate:
		switch vu := value.(type) {
		case string:
			if _, err := time.Parse(time.RFC3339Nano, vu); err != nil {
				return false
			}
			return true
		case time.Time, *time.Time:
			return true
		default:
			return false
		}
	case PandoraTypeArray:
		kd := reflect.ValueOf(value).Kind()
		return kd == reflect.Slice || kd == reflect.Array
	case PandoraTypeMap:
		return reflect.ValueOf(value).Kind() == reflect.Map
	case PandoraTypeBool:
		vu := reflect.ValueOf(value)
		switch vu.Kind() {
		case reflect.String:
			ret, _ := strconv.ParseBool(vu.String())
			return ret
		case reflect.Bool:
			return true
		default:
			return false
		}
	case PandoraTypeJsonString:
		if _, ok := value.(map[string]interface{}); ok {
			return true
		}
		vu := reflect.ValueOf(value)
		var str string
		if vu.Kind() == reflect.String {
			str = vu.String()
		} else {
			str = fmt.Sprintf("%v", value)
		}
		if str == "" {
			return true
		}
		return IsJsonString(str)
	}
	return true
}

func (s *Sender) getSchemasAlias() (map[string]pipeline.RepoSchemaEntry, map[string]string) {
	s.schemasMux.RLock()
	defer s.schemasMux.RUnlock()
	return s.schemas, s.alias2key
}

func (s *Sender) generatePoint(data Data) (point Data) {
	point = make(Data, len(data))
	schemas, alias2key := s.getSchemasAlias()
	for k, v := range schemas {
		name, ok := alias2key[k]
		if !s.UserSchema.DefaultAll && !ok {
			delete(data, name)
			continue // 表示这个值未被选中
		}
		value, ok := data[name]
		if !ok {
			//不存在，但是必填，需要加上默认值
			if v.Required {
				value = s.client.GetDefault(v)
			} else {
				continue
			}
		}
		delete(data, name)
		if v.ValueType == PandoraTypeDate && s.opt.autoConvertDate {
			formatTime, err := convertDate(value, forceMicrosecondOption{
				nanosecond:       s.microsecondCounter,
				forceMicrosecond: s.opt.forceMicrosecond})
			if err != nil {
				log.Error(err)
				continue
			}
			s.microsecondCounter = s.microsecondCounter + 1
			value = formatTime
		}
		if !s.opt.forceDataConvert && s.opt.ignoreInvalidField && !validSchema(v.ValueType, value, s.opt.numberUseFloat) {
			if value != nil {
				log.Errorf("Runner[%v] Sender[%v]: key <%v> value < %v > not match type %v, from data < %v >, ignored this field", s.opt.runnerName, s.opt.name, name, value, v.ValueType, data)
			}
			continue
		}
		point[k] = value
	}
	if s.opt.uuid {
		uuid, _ := gouuid.NewV4()
		point[sender.PandoraUUID] = uuid.String()
	}
	/*
		data中剩余的值，但是在schema中不存在的，根据defaultAll和schemaFree判断是否增加。
	*/
	for k, v := range data {
		name, ok := s.UserSchema.Fields[k]
		if !ok {
			name = k
		}
		if s.UserSchema.DefaultAll || ok {
			point[name] = v
		}
	}
	return
}

func (s *Sender) checkSchemaUpdate() {
	var lastUpdateTime time.Time
	s.schemasMux.RLock()
	lastUpdateTime = s.lastUpdate
	s.schemasMux.RUnlock()
	if lastUpdateTime.Add(s.opt.updateInterval).After(time.Now()) {
		return
	}
	s.UpdateSchemas()
}

func (s *Sender) Send(datas []Data) (se error) {
	switch s.sendType {
	case SendTypeRaw:
		return s.rawSend(datas)
	default:
		return s.schemaFreeSend(datas)
	}
	return nil
}

func (s *Sender) rawSend(datas []Data) (se error) {
	for idx, v := range datas {
		if v["_repo"] == nil || v["_raw"] == nil || v["_ak"] == nil || v["_sk"] == nil {
			errinfo := "_repo or _raw or _ak or _sk not found"
			return &StatsError{
				ErrorDetail: errors.New(errinfo),
				StatsInfo: StatsInfo{
					Success:   int64(idx),
					Errors:    int64(len(datas[idx:]) - idx),
					LastError: errinfo,
				},
				RemainDatas: datas[idx:],
			}
		}
		repoName, ok := v["_repo"].(string)
		if !ok {
			errinfo := "_repo not string type"
			return &StatsError{
				ErrorDetail: errors.New(errinfo),
				StatsInfo: StatsInfo{
					Success:   int64(idx),
					Errors:    int64(len(datas) - idx),
					LastError: errinfo,
				},
				RemainDatas: datas[idx:],
			}
		}
		raw, ok := v["_raw"].([]byte)
		if !ok {
			str, sok := v["_raw"].(string)
			if !sok {
				errinfo := "_raw not []byte or string type"
				return &StatsError{
					ErrorDetail: errors.New(errinfo),
					StatsInfo: StatsInfo{
						Success:   int64(idx),
						Errors:    int64(len(datas[idx:])),
						LastError: errinfo,
					},
					RemainDatas: datas[idx:],
				}
			}

			decodeBytes, err := base64.StdEncoding.DecodeString(str)
			if err != nil {
				errinfo := "can not unbase 64 raw, err = " + err.Error()
				return &StatsError{
					ErrorDetail: errors.New(errinfo),
					StatsInfo: StatsInfo{
						Success:   int64(idx),
						Errors:    int64(len(datas[idx:])),
						LastError: errinfo,
					},
					RemainDatas: datas[idx:],
				}
			}
			raw = decodeBytes
		}
		ak, ok := v["_ak"].(string)
		if !ok {
			errinfo := "_ak not string type"
			return &StatsError{
				ErrorDetail: errors.New(errinfo),
				StatsInfo: StatsInfo{
					Success:   int64(idx),
					Errors:    int64(len(datas[idx:])),
					LastError: errinfo,
				},
				RemainDatas: datas[idx:],
			}
		}
		sk, ok := v["_sk"].(string)
		if !ok {
			errinfo := "_sk not string type"
			return &StatsError{
				ErrorDetail: errors.New(errinfo),
				StatsInfo: StatsInfo{
					Success:   int64(idx),
					Errors:    int64(len(datas[idx:])),
					LastError: errinfo,
				},
				RemainDatas: datas[idx:],
			}
		}

		config := pipeline.NewConfig().
			WithPipelineEndpoint(s.opt.endpoint).
			WithAccessKeySecretKey(ak, sk).
			WithLogger(pipelinebase.NewDefaultLogger()).
			WithLoggerLevel(pipelinebase.LogInfo).
			WithRequestRateLimit(s.opt.reqRateLimit).
			WithFlowRateLimit(s.opt.flowRateLimit).
			WithGzipData(s.opt.gzip).
			WithHeaderUserAgent(s.opt.useragent).WithInsecureServer(s.opt.insecureServer)
		client, err := pipeline.New(config)
		if err != nil {
			err = fmt.Errorf("cannot init pipelineClient %v", err)
			return &StatsError{
				ErrorDetail: err,
				StatsInfo: StatsInfo{
					Success:   int64(idx),
					Errors:    int64(len(datas[idx:]) - idx),
					LastError: err.Error(),
				},
				RemainDatas: datas[idx:],
			}
		}

		err = client.PostDataFromBytes(&pipeline.PostDataFromBytesInput{RepoName: repoName, Buffer: raw})
		if err != nil {
			return &StatsError{
				ErrorDetail: err,
				StatsInfo: StatsInfo{
					Success:   int64(idx),
					Errors:    int64(len(datas) - idx),
					LastError: err.Error(),
				},
				RemainDatas: datas[idx:],
			}
		}
	}
	return &StatsError{
		StatsInfo: StatsInfo{
			Success: int64(len(datas)),
			Errors:  0,
		},
	}
}

func (s *Sender) schemaFreeSend(datas []Data) (se error) {
	s.checkSchemaUpdate()
	if !s.opt.schemaFree && (len(s.schemas) <= 0 || len(s.alias2key) <= 0) {
		se = reqerr.NewSendError("Get pandora schema error, failed to send data", sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		ste := &StatsError{
			StatsInfo: StatsInfo{
				Success:   0,
				Errors:    int64(len(datas)),
				LastError: "Get pandora schema error or repo not exist",
			},
			ErrorDetail: se,
		}
		return ste
	}
	var points pipeline.Datas
	now := time.Now().Format(time.RFC3339Nano)
	for _, d := range datas {
		if len(d) < 0 {
			continue
		}
		if s.opt.logkitSendTime {
			d[sender.KeyLogkitSendTime] = now
		}
		if s.opt.extraInfo {
			for key, val := range s.extraInfo {
				if _, exist := d[key]; !exist {
					d[key] = val
				}
			}
		}
		point := s.generatePoint(d)
		if len(point) < 1 {
			continue
		}
		points = append(points, pipeline.Data(map[string]interface{}(point)))
	}
	s.opt.tokenLock.RLock()
	schemaFreeInput := &pipeline.SchemaFreeInput{
		WorkflowName:    s.opt.workflowName,
		RepoName:        s.opt.repoName,
		NoUpdate:        !s.opt.schemaFree,
		Datas:           points,
		SchemaFreeToken: s.opt.tokens.SchemaFreeTokens,
		RepoOptions:     &pipeline.RepoOptions{WithIP: s.opt.withip, UnescapeLine: s.opt.UnescapeLine},
		Option: &pipeline.SchemaFreeOption{
			NumberUseFloat: s.opt.numberUseFloat,
			ToLogDB:        s.opt.enableLogdb,
			AutoExportToLogDBInput: pipeline.AutoExportToLogDBInput{
				OmitEmpty:             true,
				OmitInvalid:           false,
				RepoName:              s.opt.repoName,
				LogRepoName:           s.opt.logdbReponame,
				AnalyzerInfo:          s.opt.analyzerInfo,
				AutoExportLogDBTokens: s.opt.tokens.LogDBTokens,
			},
			ToKODO: s.opt.enableKodo,
			AutoExportToKODOInput: pipeline.AutoExportToKODOInput{
				Retention:            s.opt.kodoFileRetention,
				RepoName:             s.opt.repoName,
				BucketName:           s.opt.bucketName,
				Email:                s.opt.email,
				Prefix:               s.opt.prefix,
				Format:               s.opt.format,
				Compress:             s.opt.kodoCompress,
				RotateStrategy:       s.opt.kodoRotateStrategy,
				RotateSize:           s.opt.kodoRotateSize,
				RotateInterval:       s.opt.kodoRotateInterval,
				RotateSizeType:       "B",
				RotateNumber:         s.opt.kodoRotateSize,
				AutoExportKodoTokens: s.opt.tokens.KodoTokens,
			},
			ToTSDB: s.opt.enableTsdb,
			AutoExportToTSDBInput: pipeline.AutoExportToTSDBInput{
				OmitEmpty:            true,
				OmitInvalid:          false,
				IsMetric:             s.opt.isMetrics,
				ExpandAttr:           s.opt.expandAttr,
				RepoName:             s.opt.repoName,
				TSDBRepoName:         s.opt.tsdbReponame,
				SeriesName:           s.opt.tsdbSeriesName,
				SeriesTags:           s.opt.tsdbSeriesTags,
				Timestamp:            s.opt.tsdbTimestamp,
				AutoExportTSDBTokens: s.opt.tokens.TsDBTokens,
			},
			ForceDataConvert: s.opt.forceDataConvert,
		},
	}
	s.opt.tokenLock.RUnlock()
	schemas, se := s.client.PostDataSchemaFree(schemaFreeInput)
	if schemas != nil {
		s.updateSchemas(schemas)
	}

	if se != nil {
		nse, ok := se.(*reqerr.SendError)
		ste := &StatsError{
			ErrorDetail: se,
		}
		if ok {
			ste.LastError = nse.Error()
			ste.Errors = int64(len(nse.GetFailDatas()))
			ste.Success = int64(len(datas)) - ste.Errors
		} else {
			ste.LastError = se.Error()
			ste.Errors = int64(len(datas))
		}
		return ste
	}
	ste := &StatsError{
		ErrorDetail: se,
		StatsInfo: StatsInfo{
			Success:   int64(len(datas)),
			LastError: "",
		},
	}
	return ste
}

func (s *Sender) Name() string {
	if len(s.opt.name) <= 0 {
		return "panodra:" + s.opt.repoName
	}
	return s.opt.name
}

func (s *Sender) Close() error {
	return s.client.Close()
}
