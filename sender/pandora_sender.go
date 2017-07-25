package sender

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"

	pipelinebase "github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/pipeline"

	gouuid "github.com/satori/go.uuid"
)

// 可选参数 当sender_type 为pandora 的时候，需要必填的字段
const (
	KeyPandoraAk                   = "pandora_ak"
	KeyPandoraSk                   = "pandora_sk"
	KeyPandoraHost                 = "pandora_host"
	KeyPandoraRepoName             = "pandora_repo_name"
	KeyPandoraRegion               = "pandora_region"
	KeyPandoraSchema               = "pandora_schema"
	KeyPandoraSchemaUpdateInterval = "pandora_schema_update_interval"
	KeyPandoraAutoCreate           = "pandora_auto_create"
	KeyPandoraSchemaFree           = "pandora_schema_free"
	KeyPandoraEnableLogDB          = "pandora_enable_logdb"
	KeyPandoraLogDBName            = "pandora_logdb_name"
	KeyPandoraLogDBHost            = "pandora_logdb_host"
	KeyRequestRateLimit            = "request_rate_limit"
	KeyFlowRateLimit               = "flow_rate_limit"
	KeyPandoraGzip                 = "pandora_gzip"
	KeyPandoraUUID                 = "pandora_uuid"
	KeyForceMicrosecond            = "force_microsecond"

	PandoraUUID = "Pandora_UUID"
)

// PandoraSender pandora sender
type PandoraSender struct {
	client             pipeline.PipelineAPI
	schemas            map[string]pipeline.RepoSchemaEntry
	schemasMux         sync.RWMutex
	lastUpdate         time.Time
	updateMux          sync.Mutex
	UserSchema         UserSchema
	alias2key          map[string]string // map[alias]name
	opt                PandoraOption
	microsecondCounter int64
}

// UserSchema was parsed pandora schema from user's raw schema
type UserSchema struct {
	DefaultAll bool
	Fields     map[string]string
}

// PandoraOption 创建Pandora Sender的选项
type PandoraOption struct {
	runnerName       string
	name             string
	repoName         string
	region           string
	endpoint         string
	ak               string
	sk               string
	schema           string
	schemaFree       bool   // schemaFree在用户数据有新字段时就更新repo添加字段，如果repo不存在，创建repo。schemaFree功能包含autoCreate
	autoCreate       string // 自动创建用户的repo，dsl语言填写schema
	updateInterval   time.Duration
	reqRateLimit     int64
	flowRateLimit    int64
	gzip             bool
	uuid             bool
	enableLogdb      bool
	logdbReponame    string
	logdbendpoint    string
	forceMicrosecond bool
}

//PandoraMaxBatchSize 发送到Pandora的batch限制
var PandoraMaxBatchSize = 2 * 1024 * 1024

// NewPandoraSender pandora sender constructor
func NewPandoraSender(conf conf.MapConf) (sender Sender, err error) {
	repoName, err := conf.GetString(KeyPandoraRepoName)
	if err != nil {
		return
	}
	region, err := conf.GetString(KeyPandoraRegion)
	if err != nil {
		return
	}
	host, err := conf.GetString(KeyPandoraHost)
	if err != nil {
		return
	}
	ak, err := conf.GetString(KeyPandoraAk)
	if err != nil {
		return
	}
	akFromEnv := utils.GetEnv(ak)
	if akFromEnv == "" {
		akFromEnv = ak
	}

	sk, err := conf.GetString(KeyPandoraSk)
	skFromEnv := utils.GetEnv(sk)
	if skFromEnv == "" {
		skFromEnv = sk
	}
	if err != nil {
		return
	}
	schema, _ := conf.GetStringOr(KeyPandoraSchema, "")
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("pandoraSender:(%v,repo:%v,region:%v)", host, repoName, region))
	updateInterval, _ := conf.GetInt64Or(KeyPandoraSchemaUpdateInterval, 300)
	schemaFree, _ := conf.GetBoolOr(KeyPandoraSchemaFree, false)
	forceMicrosecond, _ := conf.GetBoolOr(KeyForceMicrosecond, false)
	autoCreateSchema, _ := conf.GetStringOr(KeyPandoraAutoCreate, "")
	reqRateLimit, _ := conf.GetInt64Or(KeyRequestRateLimit, 0)
	flowRateLimit, _ := conf.GetInt64Or(KeyFlowRateLimit, 0)
	gzip, _ := conf.GetBoolOr(KeyPandoraGzip, false)
	uuid, _ := conf.GetBoolOr(KeyPandoraUUID, false)
	runnerName, _ := conf.GetStringOr(KeyRunnerName, UnderfinedRunnerName)
	enableLogdb, _ := conf.GetBoolOr(KeyPandoraEnableLogDB, false)
	logdbreponame, _ := conf.GetStringOr(KeyPandoraLogDBName, repoName)
	logdbhost, _ := conf.GetStringOr(KeyPandoraLogDBHost, "")
	opt := &PandoraOption{
		runnerName:       runnerName,
		name:             name,
		repoName:         repoName,
		region:           region,
		endpoint:         host,
		ak:               akFromEnv,
		sk:               skFromEnv,
		schema:           schema,
		autoCreate:       autoCreateSchema,
		schemaFree:       schemaFree,
		updateInterval:   time.Duration(updateInterval) * time.Second,
		reqRateLimit:     reqRateLimit,
		flowRateLimit:    flowRateLimit,
		gzip:             gzip,
		uuid:             uuid,
		enableLogdb:      enableLogdb,
		logdbReponame:    logdbreponame,
		logdbendpoint:    logdbhost,
		forceMicrosecond: forceMicrosecond,
	}
	return newPandoraSender(opt)
}

func createPandoraRepo(autoCreateSchema, repoName, region string, client pipeline.PipelineAPI) (err error) {
	dsl := strings.TrimSpace(autoCreateSchema)
	if dsl == "" {
		return
	}
	return client.CreateRepoFromDSL(&pipeline.CreateRepoDSLInput{
		RepoName: repoName,
		Region:   region,
		DSL:      dsl,
	})
}

func newPandoraSender(opt *PandoraOption) (s *PandoraSender, err error) {
	logger := pipelinebase.NewDefaultLogger()
	config := pipeline.NewConfig().
		WithPipelineEndpoint(opt.endpoint).
		WithAccessKeySecretKey(opt.ak, opt.sk).
		WithLogger(logger).
		WithLoggerLevel(pipelinebase.LogInfo).
		WithRequestRateLimit(opt.reqRateLimit).
		WithFlowRateLimit(opt.flowRateLimit).
		WithGzipData(opt.gzip)
	if opt.logdbendpoint != "" {
		config = config.WithLogDBEndpoint(opt.logdbendpoint)
	}
	client, err := pipeline.New(config)
	if err != nil {
		err = fmt.Errorf("Cannot init pipelineClient %v", err)
		return
	}
	if opt.reqRateLimit > 0 {
		log.Warnf("Runner[%v] Sender[%v]: you have limited send speed within %v requests/s", opt.runnerName, opt.name, opt.reqRateLimit)
	}
	if opt.flowRateLimit > 0 {
		log.Warnf("Runner[%v] Sender[%v]: you have limited send speed within %v KB/s", opt.runnerName, opt.name, opt.flowRateLimit)
	}
	userSchema := parseUserSchema(opt.repoName, opt.schema)
	s = &PandoraSender{
		opt:        *opt,
		client:     client,
		alias2key:  make(map[string]string),
		UserSchema: userSchema,
		schemas:    make(map[string]pipeline.RepoSchemaEntry),
	}
	if createErr := createPandoraRepo(opt.autoCreate, opt.repoName, opt.region, client); createErr != nil {
		if !strings.Contains(createErr.Error(), "E18101") {
			log.Errorf("Runner[%v] Sender[%v]: auto create pandora repo error: %v, you can create on pandora portal, ignored...", opt.runnerName, opt.name, createErr)
		}
	}
	// 如果updateSchemas更新schema失败，不会报错，可以正常启动runner，但是在sender时会检查schema是否获取
	// sender时会尝试不断获取pandora schema，若还是获取失败则返回发送错误。
	s.UpdateSchemas()
	if s.opt.enableLogdb && len(s.schemas) > 0 {
		log.Printf("Runner[%v] Sender[%v]: auto create export to logdb (%v)", opt.runnerName, opt.name, opt.logdbReponame)
		err = s.client.AutoExportToLogDB(&pipeline.AutoExportToLogDBInput{
			RepoName:    s.opt.repoName,
			LogRepoName: s.opt.logdbReponame,
		})
		if err != nil {
			log.Warnf("Runner[%v] Sender[%v]: AutoExportToLogDB %v error %v", s.opt.runnerName, s.opt.name, s.opt.logdbReponame, err)
			err = nil
		}
	}
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

func (s *PandoraSender) UpdateSchemas() {
	schemas, err := s.client.GetUpdateSchemas(s.opt.repoName)
	if err != nil && (!s.opt.schemaFree || !reqerr.IsNoSuchResourceError(err)) {
		log.Warnf("Runner[%v] Sender[%v]: update pandora repo <%v> schema error %v", s.opt.runnerName, s.opt.name, s.opt.repoName, err)
		return
	}
	if schemas == nil {
		return
	}
	s.updateMux.Lock()
	defer s.updateMux.Unlock()
	//double check
	if s.lastUpdate.Add(s.opt.updateInterval).After(time.Now()) {
		return
	}
	s.lastUpdate = time.Now()

	s.updateSchemas(schemas)
}

func (s *PandoraSender) updateSchemas(schemas map[string]pipeline.RepoSchemaEntry) {
	alias2Key := fillAlias2Keys(s.opt.repoName, schemas, s.UserSchema)
	s.schemasMux.Lock()
	s.schemas = schemas
	s.alias2key = alias2Key
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
	PandoraTypeLong   = "long"
	PandoraTypeFloat  = "float"
	PandoraTypeString = "string"
	PandoraTypeDate   = "date"
	PandoraTypeBool   = "boolean"
	PandoraTypeArray  = "array"
	PandoraTypeMap    = "map"
)

type forceMicrosecondOption struct {
	microsecond      int64
	forceMicrosecond bool
}

//临时方案，转换时间，目前sender这边拿到的都是string，很难确定是什么格式的string
//microsecond: 表示要在当前时间基础上加多少偏移量,只在forceMicrosecond为true的情况下才有效
//forceMicrosecond: 表示是否要对当前时间加偏移量
func convertDate(v interface{}, option forceMicrosecondOption) (interface{}, error) {
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
		rfctime := t.Format(time.RFC3339Nano)
		if option.forceMicrosecond {
			news := alignTimestamp(t.UTC().UnixNano(), option.microsecond)
			rfctime = time.Unix(0, news*int64(time.Microsecond)).Format(time.RFC3339Nano)
		}

		return rfctime, err
	case json.Number:
		jsonNumber, err := newv.Int64()
		if err != nil {
			return v, err
		}
		s = jsonNumber
	default:
		return v, fmt.Errorf("can not parse %v type %v as date time", v, reflect.TypeOf(v))
	}
	news := s
	if option.forceMicrosecond {
		news = alignTimestamp(s, option.microsecond)
	}
	timestamp := strconv.FormatInt(news, 10)
	timeSecondPrecision := 16
	//补齐16位
	for i := len(timestamp); i < timeSecondPrecision; i++ {
		timestamp += "0"
	}
	// 取前16位，截取精度 微妙
	timestamp = timestamp[0:timeSecondPrecision]
	t, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return v, err
	}
	v = time.Unix(0, t*int64(time.Microsecond)).Format(time.RFC3339Nano)
	return v, nil
}

// alignTimestamp
// 1. 根据输入时间戳的位数来补齐对应的位数
// 2. 对于精度不是微妙的数据点，加一个扰动
func alignTimestamp(t int64, microsecond int64) int64 {
	for i := 0; t%10 == 0; i++ {
		t /= 10
	}
	offset := 16 - len(strconv.FormatInt(t, 10))
	dividend := int64(math.Pow10(offset))
	if offset > 0 {
		t = t * dividend //补齐16位
		return t + microsecond%dividend
	}
	return t
}

func validSchema(valueType string, value interface{}) bool {
	v := fmt.Sprintf("%v", value)
	switch valueType {
	case PandoraTypeLong:
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return false
		}
	case PandoraTypeFloat:
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return false
		}
	case PandoraTypeString:
	case PandoraTypeDate:
	case PandoraTypeArray:
	case PandoraTypeMap:
	case PandoraTypeBool:
	}
	return true
}

func (s *PandoraSender) getSchemasAlias() (map[string]pipeline.RepoSchemaEntry, map[string]string) {
	s.schemasMux.RLock()
	defer s.schemasMux.RUnlock()
	return s.schemas, s.alias2key
}

func (s *PandoraSender) generatePoint(data Data) (point Data) {
	point = Data{}
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
		if v.ValueType == PandoraTypeDate {
			formatTime, err := convertDate(value, forceMicrosecondOption{
				microsecond:      s.microsecondCounter,
				forceMicrosecond: s.opt.forceMicrosecond})
			if err != nil {
				log.Error(err)
				continue
			}
			s.microsecondCounter = (s.microsecondCounter + 1) % (2 << 32)
			value = formatTime
		}

		if !validSchema(v.ValueType, value) {
			log.Errorf("Runner[%v] Sender[%v]: %v not match type %v, from data < %v >, ignored this field", s.opt.runnerName, s.opt.name, value, v.ValueType, data)
			continue
		}
		point[k] = value
	}
	if s.opt.uuid {
		point[PandoraUUID] = gouuid.NewV4().String()
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

func (s *PandoraSender) checkSchemaUpdate() {
	if s.lastUpdate.Add(s.opt.updateInterval).After(time.Now()) {
		return
	}
	s.UpdateSchemas()
}

func (s *PandoraSender) Send(datas []Data) (se error) {
	s.checkSchemaUpdate()
	if !s.opt.schemaFree && (len(s.schemas) <= 0 || len(s.alias2key) <= 0) {
		se = reqerr.NewSendError("Get pandora schema error, faild to send data", ConvertDatasBack(datas), reqerr.TypeDefault)
		return
	}
	var points pipeline.Datas
	for _, d := range datas {
		point := s.generatePoint(d)
		points = append(points, pipeline.Data(map[string]interface{}(point)))
	}
	schemas, se := s.client.PostDataSchemaFree(&pipeline.SchemaFreeInput{
		RepoName: s.opt.repoName,
		NoUpdate: !s.opt.schemaFree,
		Datas:    points,
		Option: &pipeline.SchemaFreeOption{
			ToLogDB:       s.opt.enableLogdb,
			LogDBRepoName: s.opt.logdbReponame,
		},
	})
	if schemas != nil {
		s.updateSchemas(schemas)
	}
	return
}

func (s *PandoraSender) Name() string {
	if len(s.opt.name) <= 0 {
		return "panodra:" + s.opt.repoName
	}
	return s.opt.name
}

func (s *PandoraSender) Close() error {
	return s.client.Close()
}
