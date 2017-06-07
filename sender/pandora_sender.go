package sender

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	KeyRequestRateLimit            = "request_rate_limit"
	KeyFlowRateLimit               = "flow_rate_limit"
	KeyPandoraGzip                 = "pandora_gzip"
)

// PandoraSender pandora sender
type PandoraSender struct {
	name           string
	repoName       string
	client         pipeline.PipelineAPI
	schemas        map[string]pipeline.RepoSchemaEntry
	schemasMux     sync.RWMutex
	lastUpdate     time.Time
	updateMux      sync.Mutex
	updateInterval time.Duration
	UserSchema     UserSchema
	alias2key      map[string]string // map[alias]name
}

// UserSchema was parsed pandora schema from user's raw schema
type UserSchema struct {
	DefaultAll bool
	Fields     map[string]string
}

// PandoraOption 创建Pandora Sender的选项
type PandoraOption struct {
	name           string
	repoName       string
	region         string
	endpoint       string
	ak             string
	sk             string
	schema         string
	autoCreate     string
	updateInterval time.Duration
	reqRateLimit   int64
	flowRateLimit  int64
	gzip           bool
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
	autoCreateSchema, _ := conf.GetStringOr(KeyPandoraAutoCreate, "")
	reqRateLimit, _ := conf.GetInt64Or(KeyRequestRateLimit, 0)
	flowRateLimit, _ := conf.GetInt64Or(KeyFlowRateLimit, 0)
	gzip, _ := conf.GetBoolOr(KeyPandoraGzip, false)
	opt := &PandoraOption{
		name:           name,
		repoName:       repoName,
		region:         region,
		endpoint:       host,
		ak:             akFromEnv,
		sk:             skFromEnv,
		schema:         schema,
		autoCreate:     autoCreateSchema,
		updateInterval: time.Duration(updateInterval) * time.Second,
		reqRateLimit:   reqRateLimit,
		flowRateLimit:  flowRateLimit,
		gzip:           gzip,
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
		WithEndpoint(opt.endpoint).
		WithAccessKeySecretKey(opt.ak, opt.sk).
		WithLogger(logger).
		WithLoggerLevel(pipelinebase.LogInfo).
		WithRequestRateLimit(opt.reqRateLimit).
		WithFlowRateLimit(opt.flowRateLimit).
		WithGzipData(opt.gzip)

	client, err := pipeline.New(config)
	if err != nil {
		err = fmt.Errorf("Cannot init pipelineClient %v", err)
		return
	}
	if opt.reqRateLimit > 0 {
		log.Warnf("you have limited %v pandora sender within %v requests/s", opt.name, opt.reqRateLimit)
	}
	if opt.flowRateLimit > 0 {
		log.Warnf("you have limited %v pandora sender within %v KB/s", opt.name, opt.flowRateLimit)
	}
	userSchema := parseUserSchema(opt.repoName, opt.schema)
	s = &PandoraSender{
		name:           opt.name,
		repoName:       opt.repoName,
		client:         client,
		alias2key:      make(map[string]string),
		updateInterval: opt.updateInterval,
		UserSchema:     userSchema,
		schemas:        make(map[string]pipeline.RepoSchemaEntry),
	}
	if createErr := createPandoraRepo(opt.autoCreate, opt.repoName, opt.region, client); createErr != nil {
		if !strings.Contains(createErr.Error(), "E18101") {
			log.Errorf("auto create pandora repo error: %v, you can create on pandora portal, ignored...", createErr)
		}
	}
	// 如果updateSchemas更新schema失败，不会报错，可以正常启动runner，但是在sender时会检查schema是否获取
	// sender时会尝试不断获取pandora schema，若还是获取失败则返回发送错误。
	s.updateSchemas()
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

func (s *PandoraSender) updateSchemas() {
	repo, err := s.client.GetRepo(&pipeline.GetRepoInput{
		RepoName: s.repoName,
	})

	if err != nil {
		log.Errorf("sender <%v> update pandora repo <%v> schema error %v", s.name, s.repoName, err)
		return
	}
	schemas := make(map[string]pipeline.RepoSchemaEntry)
	for _, sc := range repo.Schema {
		schemas[sc.Key] = sc
	}
	alias2Key := fillAlias2Keys(s.repoName, schemas, s.UserSchema)
	s.updateMux.Lock()
	defer s.updateMux.Unlock()
	//double check
	if s.lastUpdate.Add(s.updateInterval).After(time.Now()) {
		return
	}
	s.lastUpdate = time.Now()

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
			log.Errorf("Repo-%s:select schema or alias <%v> for key <%v> was not found in pandora schema, ignore this key...", repoName, alias, name)
			continue
		}
		alias2key[alias] = name
	}
	return
}

func getDefault(t pipeline.RepoSchemaEntry) (result interface{}) {
	switch t.ValueType {
	case "long":
		result = 0
	case "float":
		result = 0.0
	case "string":
		result = ""
	case "date":
		result = time.Now().Format(time.RFC3339Nano)
	case "boolean":
		result = false
	}
	return
}

//临时方案，转换时间，目前sender这边拿到的都是string，很难确定是什么格式的string
func convertDate(v interface{}) (interface{}, error) {
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
	timestamp := strconv.FormatInt(s, 10)
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

func validSchema(valueType string, value interface{}) bool {
	v := fmt.Sprintf("%v", value)
	switch valueType {
	case "long":
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return false
		}
	case "float":
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return false
		}
	case "string":
	case "date":
	case "array":
	case "map":
	case "boolean":
	}
	return true
}

func (s *PandoraSender) getSchemasAlias() (map[string]pipeline.RepoSchemaEntry, map[string]string) {
	s.schemasMux.RLock()
	defer s.schemasMux.RUnlock()
	return s.schemas, s.alias2key
}

func (s *PandoraSender) generatePoint(data Data) (point pipeline.Point) {
	point = pipeline.Point{}
	schemas, alias2key := s.getSchemasAlias()
	for k, v := range schemas {
		name, ok := alias2key[k]
		if !ok {
			continue // 表示这个值未被选中
		}
		value, ok := data[name]
		if !ok {
			//不存在，但是必填，需要加上默认值
			if v.Required {
				value = getDefault(v)
			} else {
				continue
			}
		}

		if v.ValueType == "date" {
			formatTime, err := convertDate(value)
			if err != nil {
				log.Error(err)
				continue
			}
			value = formatTime
		}

		if !validSchema(v.ValueType, value) {
			log.Errorf("%v not match type %v, from data < %v >, ignored this field", value, v.ValueType, data)
			continue
		}

		point.Fields = append(point.Fields, pipeline.PointField{
			Key:   k,
			Value: value,
		})
	}
	//data中剩余的值，但是在schema中不存在的，默认认为是用户不需要的。
	return
}

func (s *PandoraSender) checkSchemaUpdate() {
	if s.lastUpdate.Add(s.updateInterval).After(time.Now()) {
		return
	}
	s.updateSchemas()
}

func (s *PandoraSender) Send(datas []Data) (se error) {
	s.checkSchemaUpdate()
	if len(s.schemas) <= 0 || len(s.alias2key) <= 0 {
		se = NewSendError("Get pandora schema error, faild to send data", datas, TypeDefault)
		return
	}
	var lastErr error
	contexts := s.unpack(datas)
	var failDatas = []Data{}
	errType := TypeDefault
	for _, pContext := range contexts {
		err := s.client.PostDataFromBytes(pContext.inputs)
		if err != nil {
			reqErr, ok := err.(*reqerr.RequestError)
			if ok {
				switch reqErr.ErrorType {
				case reqerr.InvalidDataSchemaError, reqerr.EntityTooLargeError:
					errType = TypeBinaryUnpack
				}
			}
			failDatas = append(failDatas, pContext.datas...)
			lastErr = err
		}
	}
	if len(failDatas) > 0 {
		se = NewSendError("Cannot send data to pandora, "+lastErr.Error(), failDatas, errType)
	}
	return
}

type pointContext struct {
	datas  []Data
	inputs *pipeline.PostDataFromBytesInput
}

func (s *PandoraSender) unpack(datas []Data) (packages []pointContext) {
	packages = []pointContext{}
	var buf bytes.Buffer
	var start = 0
	for i, d := range datas {
		point := s.generatePoint(d)
		pointString := pointToString(point)
		// 当buf中有数据，并且加入该条数据后就超过了最大的限制，则提交这个input
		if start < i && buf.Len() > 0 && buf.Len()+len(pointString) >= PandoraMaxBatchSize {
			packages = append(packages, pointContext{
				datas: datas[start:i],
				inputs: &pipeline.PostDataFromBytesInput{
					RepoName: s.repoName,
					Buffer:   buf.Bytes(),
				},
			})
			buf.Reset()
			start = i
		}
		buf.WriteString(pointString)
	}
	packages = append(packages, pointContext{
		datas: datas[start:],
		inputs: &pipeline.PostDataFromBytesInput{
			RepoName: s.repoName,
			Buffer:   buf.Bytes(),
		},
	})
	return
}

func pointToString(point pipeline.Point) (bs string) {
	var buf bytes.Buffer
	for _, field := range point.Fields {
		buf.WriteString(field.String())
	}
	if len(point.Fields) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte('\n')
	return buf.String()
}

func (s *PandoraSender) Name() string {
	if len(s.name) <= 0 {
		return "panodra:" + s.repoName
	}
	return s.name
}

func (s *PandoraSender) Close() error {
	return s.client.Close()
}
