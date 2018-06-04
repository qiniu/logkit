package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const (
	defaultRegion      = "nb"
	VariableTimeType   = "time"
	VariableStringType = "string"
)

//PandoraMaxBatchSize 发送到Pandora的batch限制
var PandoraMaxBatchSize = 2 * 1024 * 1024

const (
	schemaKeyPattern      = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	groupNamePattern      = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	repoNamePattern       = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	transformNamePattern  = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	exportNamePattern     = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	datasourceNamePattern = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	jobNamePattern        = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	jobExportNamePattern  = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	workflowNamePattern   = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	nodeNamePattern       = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	pluginNamePattern     = "^[a-zA-Z][a-zA-Z0-9_\\.]{0,127}[a-zA-Z0-9_]$"
	variableNameRex       = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
)

//Pandora Types
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

const (
	ExportTypeLogDB = "logdb"
	ExportTypeTSDB  = "tsdb"
	ExportTypeKODO  = "kodo"
	ExportTypeHTTP  = "http"
	ExportTypeMongo = "mongo"
	ExportTypeHDFS  = "hdfs"
)

type Data map[string]interface{}
type Datas []Data

var schemaTypes = map[string]bool{
	PandoraTypeFloat:      true,
	PandoraTypeString:     true,
	PandoraTypeLong:       true,
	PandoraTypeDate:       true,
	PandoraTypeArray:      true,
	PandoraTypeMap:        true,
	PandoraTypeBool:       true,
	PandoraTypeJsonString: true,
}

func validateGroupName(g string) error {
	matched, err := regexp.MatchString(groupNamePattern, g)
	if err != nil {
		return reqerr.NewInvalidArgs("GroupName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("GroupName", fmt.Sprintf("invalid group name: %s", g))
	}
	return nil
}

func validateRepoName(r string) error {
	matched, err := regexp.MatchString(repoNamePattern, r)
	if err != nil {
		return reqerr.NewInvalidArgs("RepoName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("RepoName", fmt.Sprintf("invalid repo name: %s", r)).WithComponent("pipleline")
	}
	return nil
}

func validateTransformName(t string) error {
	matched, err := regexp.MatchString(transformNamePattern, t)
	if err != nil {
		return reqerr.NewInvalidArgs("TransformName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("TransformName", fmt.Sprintf("invalid transform name: %s", t)).WithComponent("pipleline")
	}
	return nil
}

func validateExportName(e string) error {
	matched, err := regexp.MatchString(exportNamePattern, e)
	if err != nil {
		return reqerr.NewInvalidArgs("ExportName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("ExportName", fmt.Sprintf("invalid export name: %s", e)).WithComponent("pipleline")
	}
	return nil
}

func validatePluginName(p string) error {
	matched, err := regexp.MatchString(pluginNamePattern, p)
	if err != nil {
		return reqerr.NewInvalidArgs("PluginName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("PluginName", fmt.Sprintf("invalid plugin name: %s", p)).WithComponent("pipleline")
	}
	return nil
}

func validateDatasouceName(d string) error {
	matched, err := regexp.MatchString(datasourceNamePattern, d)
	if err != nil {
		return reqerr.NewInvalidArgs("DatasourceName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("DatasourceName", fmt.Sprintf("invalid datasource name: %s", d)).WithComponent("pipleline")
	}
	return nil
}

func validateJobName(j string) error {
	matched, err := regexp.MatchString(datasourceNamePattern, j)
	if err != nil {
		return reqerr.NewInvalidArgs("JobName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("JobName", fmt.Sprintf("invalid job name: %s", j)).WithComponent("pipleline")
	}
	return nil
}

func validateJobexportName(e string) error {
	matched, err := regexp.MatchString(datasourceNamePattern, e)
	if err != nil {
		return reqerr.NewInvalidArgs("JobexportName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("JobexportName", fmt.Sprintf("invalid job export name: %s", e)).WithComponent("pipleline")
	}
	return nil
}

func validateWorkflowName(r string) error {
	matched, err := regexp.MatchString(workflowNamePattern, r)
	if err != nil {
		return reqerr.NewInvalidArgs("Workflow", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("Workflow", fmt.Sprintf("invalid workflow name: %s", r)).WithComponent("pipleline")
	}
	return nil
}

func validateNodeName(r string) error {
	matched, err := regexp.MatchString(nodeNamePattern, r)
	if err != nil {
		return reqerr.NewInvalidArgs("Workflow NodeName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("Workflow NodeName", fmt.Sprintf("invalid workflow node name: %s", r)).WithComponent("pipleline")
	}
	return nil
}

func validateVariableName(r string) error {
	matched, err := regexp.MatchString(variableNameRex, r)
	if err != nil {
		return reqerr.NewInvalidArgs("VariableName", err.Error()).WithComponent("pipleline")
	}
	if !matched {
		return reqerr.NewInvalidArgs("VariableName", fmt.Sprintf("invalid variable name: %s", r)).WithComponent("pipleline")
	}
	return nil
}

func validateVariableType(varType string) (err error) {
	if varType != VariableTimeType && varType != VariableStringType {
		err = reqerr.NewInvalidArgs("type", "variable type must be `time` or `string`").WithComponent("pipleline")
		return
	}
	return
}

type Container struct {
	Type   string `json:"type"`
	Count  int    `json:"count"`
	Status string `json:"status,omitempty"`
}

func (c *Container) Validate() (err error) {
	if c.Type == "" {
		err = reqerr.NewInvalidArgs("ContainerType", "container type should not be empty").WithComponent("pipleline")
		return
	}
	if c.Count < 1 || c.Count > 128 {
		err = reqerr.NewInvalidArgs("ContainerCount", fmt.Sprintf("invalid container count: %d", c.Count)).WithComponent("pipleline")
		return
	}
	return
}

type CreateGroupInput struct {
	PandoraToken
	GroupName       string     `json:"-"`
	Region          string     `json:"region"`
	Container       *Container `json:"container"`
	AllocateOnStart bool       `json:"allocateOnStart,omitempty"`
}

func (g *CreateGroupInput) Validate() (err error) {
	if err = validateGroupName(g.GroupName); err != nil {
		return
	}
	if g.Region == "" {
		err = reqerr.NewInvalidArgs("Region", "region should not be empty").WithComponent("pipleline")
		return
	}
	if g.Container == nil {
		err = reqerr.NewInvalidArgs("Container", "container should not be empty").WithComponent("pipleline")
		return
	}
	if err = g.Container.Validate(); err != nil {
		return
	}
	return
}

type UpdateGroupInput struct {
	PandoraToken
	GroupName string     `json:"-"`
	Container *Container `json:"container"`
}

func (g *UpdateGroupInput) Validate() (err error) {
	if err = validateGroupName(g.GroupName); err != nil {
		return
	}
	if g.Container == nil {
		err = reqerr.NewInvalidArgs("Container", "container should not be empty").WithComponent("pipleline")
		return
	}
	if err = g.Container.Validate(); err != nil {
		return
	}
	return
}

type StartGroupTaskInput struct {
	PandoraToken
	GroupName string
}

type StopGroupTaskInput struct {
	PandoraToken
	GroupName string
}

type GetGroupInput struct {
	PandoraToken
	GroupName string
}

type GetGroupOutput struct {
	Region     string     `json:"region"`
	Container  *Container `json:"container"`
	CreateTime string     `json:"createTime"`
	UpdateTime string     `json:"updateTime"`
}

type DeleteGroupInput struct {
	PandoraToken
	GroupName string
}

type GroupDesc struct {
	GroupName string     `json:"name"`
	Region    string     `json:"region"`
	Container *Container `json:"container"`
}

type ListGroupsInput struct {
	PandoraToken
}

type ListGroupsOutput struct {
	Groups []GroupDesc `json:"groups"`
}

type RepoSchemaEntry struct {
	Key         string            `json:"key"`
	ValueType   string            `json:"valtype"`
	Required    bool              `json:"required"`
	ElemType    string            `json:"elemtype,omitempty"`
	Schema      []RepoSchemaEntry `json:"schema,omitempty"`
	Description string            `json:"description,omitempty"`
}

func (e RepoSchemaEntry) String() string {
	bytes, _ := json.Marshal(e)
	return string(bytes)
}

func (e *RepoSchemaEntry) Validate() (err error) {
	matched, err := regexp.MatchString(schemaKeyPattern, e.Key)
	if err != nil {
		err = reqerr.NewInvalidArgs("Schema", err.Error()).WithComponent("pipleline")
		return
	}
	if !matched {
		err = reqerr.NewInvalidArgs("Schema", fmt.Sprintf("invalid field key: %s", e.Key)).WithComponent("pipleline")
		return

	}
	if !schemaTypes[e.ValueType] {
		err = reqerr.NewInvalidArgs("Schema", fmt.Sprintf("invalid field type: %s, field type should be one of \"float\", \"string\", \"date\", \"long\", \"boolean\", \"array\", \"map\" and \"jsonstring\"", e.ValueType)).WithComponent("pipleline")
		return
	}
	if e.ValueType == "array" {
		if e.ElemType != PandoraTypeFloat && e.ElemType != PandoraTypeLong && e.ElemType != PandoraTypeString {
			err = reqerr.NewInvalidArgs("Schema", fmt.Sprintf("invalid field type in array: %s, field type should be one of \"float\", \"string\", and \"long\"", e.ValueType)).WithComponent("pipleline")
			return
		}
	}
	if e.ValueType == PandoraTypeMap {
		for _, ns := range e.Schema {
			if err = ns.Validate(); err != nil {
				return
			}
		}
	}

	return
}

type CreateRepoDSLInput struct {
	PandoraToken
	RepoName    string
	Region      string       `json:"region"`
	DSL         string       `json:"dsl"`
	Options     *RepoOptions `json:"options"`
	GroupName   string       `json:"group"`
	Workflow    string       `json:"workflow"`
	Description string       `json:"description,omitempty"`
}

/*
DSL创建的规则为`<字段名称> <类型>`,字段名称和类型用空格符隔开，不同字段用逗号隔开。若字段必填，则在类型前加`*`号表示。
    * pandora date类型：`date`,`DATE`,`d`,`D`
    * pandora long类型：`long`,`LONG`,`l`,`L`
    * pandora float类型: `float`,`FLOAT`,`F`,`f`
    * pandora string类型: `string`,`STRING`,`S`,`s`
    * pandora bool类型:  `bool`,`BOOL`,`B`,`b`,`boolean`
    * pandora jsonstring类型： `json`,"JSON","jsonstring","JSONSTRING","j","J"
    * pandora array类型: `array`,`ARRAY`,`A`,`a`;括号中跟具体array元素的类型，如a(l)，表示array里面都是long。
    * pandora map类型: `map`,`MAP`,`M`,`m`;使用花括号表示具体类型，表达map里面的元素，如map{a l,b map{c b,x s}}, 表示map结构体里包含a字段，类型是long，b字段又是一个map，里面包含c字段，类型是bool，还包含x字段，类型是string。
*/

func getRawType(tp string) (schemaType string, err error) {
	schemaType = strings.ToLower(tp)
	switch schemaType {
	case "l", "long":
		schemaType = PandoraTypeLong
	case "f", "float":
		schemaType = PandoraTypeFloat
	case "s", "string":
		schemaType = PandoraTypeString
	case "d", "date":
		schemaType = PandoraTypeDate
	case "a", "array":
		err = errors.New("arrary type must specify data type surrounded by ( )")
		return
	case "m", "map":
		schemaType = PandoraTypeMap
	case "b", "bool", "boolean":
		schemaType = PandoraTypeBool
	case "j", "json", "jsonstring":
		schemaType = PandoraTypeJsonString
	case "": //这个是一种缺省
	default:
		err = fmt.Errorf("schema type %v not supperted", schemaType)
		return
	}
	return
}

func getField(f string) (key, valueType, elementType string, required bool, err error) {
	f = strings.TrimSpace(f)
	if f == "" {
		return
	}
	splits := strings.Fields(f)
	switch len(splits) {
	case 1:
		key = splits[0]
		return
	case 2:
		key, valueType = splits[0], splits[1]
	default:
		err = fmt.Errorf("Raw field schema parse error: <%v> was invalid", f)
		return
	}
	if key == "" {
		err = fmt.Errorf("field schema %v key can not be empty", f)
		return
	}
	required = false
	if strings.HasPrefix(valueType, "*") || strings.HasSuffix(valueType, "*") {
		required = true
		valueType = strings.Trim(valueType, "*")
	}
	//处理arrary类型
	if beg := strings.Index(valueType, "("); beg != -1 {
		ed := strings.Index(valueType, ")")
		if ed <= beg {
			err = fmt.Errorf("field schema %v has no type specified", f)
			return
		}
		elementType, err = getRawType(valueType[beg+1 : ed])
		if err != nil {
			err = fmt.Errorf("array 【%v】: %v, key %v valuetype %v", f, err, key, valueType)
		}
		valueType = "array"
		return
	}
	valueType, err = getRawType(valueType)
	if err != nil {
		err = fmt.Errorf("normal 【%v】: %v, key %v valuetype %v", f, err, key, valueType)
	}
	return
}

func DSLtoSchema(dsl string) (schemas []RepoSchemaEntry, err error) {
	return toSchema(dsl, 0)
}

func SchemaToDSL(schemas []RepoSchemaEntry, indent string) (dsl string) {
	return getFormatDSL(schemas, 0, indent)
}

func toSchema(dsl string, depth int) (schemas []RepoSchemaEntry, err error) {
	if depth > base.NestLimit {
		err = reqerr.NewInvalidArgs("Schema", fmt.Sprintf("RepoSchemaEntry are nested out of limit %v", base.NestLimit)).WithComponent("pipleline")
		return
	}
	schemas = make([]RepoSchemaEntry, 0)
	dsl = strings.TrimSpace(dsl)
	start := 0
	nestbalance := 0
	neststart, nestend := -1, -1
	dsl += "," //增加一个','保证一定是以","为终结
	dupcheck := make(map[string]struct{})
	for end, c := range dsl {
		if start > end {
			err = errors.New("parse dsl inner error: start index is larger than end")
			return
		}
		switch c {
		case '{':
			if nestbalance == 0 {
				neststart = end
			}
			nestbalance++
		case '}':
			nestbalance--
			if nestbalance == 0 {
				nestend = end
				if nestend <= neststart {
					err = errors.New("parse dsl error: nestend should never less or equal than neststart")
					return
				}
				subschemas, err := toSchema(dsl[neststart+1:nestend], depth+1)
				if err != nil {
					return nil, err
				}
				if neststart <= start {
					return nil, errors.New("parse dsl error: map{} not specified")
				}
				key, valueType, _, required, err := getField(dsl[start:neststart])
				if err != nil {
					return nil, err
				}
				if key != "" {
					if _, ok := dupcheck[key]; ok {
						return nil, errors.New("parse dsl error: " + key + " is duplicated key")
					}
					dupcheck[key] = struct{}{}
					if valueType == "" {
						valueType = "map"
					}
					schemas = append(schemas, RepoSchemaEntry{
						Key:       key,
						ValueType: valueType,
						Required:  required,
						Schema:    subschemas,
					})
				}
				start = end + 1
			}
		case ',', '\n':
			if nestbalance == 0 {
				if start < end {
					key, valueType, elemtype, required, err := getField(strings.TrimSpace(dsl[start:end]))
					if err != nil {
						return nil, err
					}
					if key != "" {
						if _, ok := dupcheck[key]; ok {
							return nil, errors.New("parse dsl error: " + key + " is duplicated key")
						}
						dupcheck[key] = struct{}{}
						if valueType == "" {
							valueType = PandoraTypeString
						}
						schemas = append(schemas, RepoSchemaEntry{
							Key:       key,
							ValueType: valueType,
							Required:  required,
							ElemType:  elemtype,
						})
					}
				}
				start = end + 1
			}
		}
	}
	if nestbalance != 0 {
		err = errors.New("parse dsl error: { and } not match")
		return
	}
	return
}

func getDepthIndent(depth int, indent string) (ds string) {
	for i := 0; i < depth; i++ {
		ds += indent
	}
	return
}

func getFormatDSL(schemas []RepoSchemaEntry, depth int, indent string) (dsl string) {
	for _, v := range schemas {
		dsl += getDepthIndent(depth, indent)
		dsl += v.Key + " "
		if v.Required {
			dsl += "*"
		}
		dsl += v.ValueType
		switch v.ValueType {
		case PandoraTypeMap:
			dsl += "{\n"
			dsl += getFormatDSL(v.Schema, depth+1, indent)
			dsl += getDepthIndent(depth, indent) + "}"
		case PandoraTypeArray:
			dsl += "(" + v.ElemType + ")"
		default:
		}
		dsl += "\n"
	}
	return
}

type AnalyzerInfo struct {
	FullText bool //fulltext为true的话使用标准分词的全文索引。
	Default  string
	Analyzer map[string]string
}

type AutoExportToLogDBInput struct {
	RepoName    string
	LogRepoName string
	Retention   string
	Region      string
	OmitInvalid bool
	OmitEmpty   bool
	Description string
	AnalyzerInfo
	AutoExportLogDBTokens
}
type AutoExportLogDBTokens struct {
	PipelineCreateRepoToken PandoraToken
	PipelineGetRepoToken    PandoraToken
	CreateLogDBRepoToken    PandoraToken
	UpdateLogDBRepoToken    PandoraToken
	GetLogDBRepoToken       PandoraToken
	CreateExportToken       PandoraToken
	UpdateExportToken       PandoraToken
	GetExportToken          PandoraToken
	ListExportToken         PandoraToken
}

type CreateRepoForLogDBInput struct {
	RepoName    string
	LogRepoName string
	Region      string
	Schema      []RepoSchemaEntry
	Retention   string
	OmitInvalid bool
	OmitEmpty   bool
	Description string
	AnalyzerInfo
	AutoExportLogDBTokens
}

type CreateRepoForLogDBDSLInput struct {
	RepoName    string
	LogRepoName string
	Region      string
	Schema      string
	Retention   string
	Description string
	AutoExportLogDBTokens
}

type AutoExportToTSDBInput struct {
	RepoName     string
	TSDBRepoName string
	Retention    string
	SeriesName   string
	OmitInvalid  bool
	OmitEmpty    bool
	Timestamp    string
	IsMetric     bool
	ExpandAttr   []string
	SeriesTags   map[string][]string
	AutoExportTSDBTokens
}

type AutoExportTSDBTokens struct {
	PipelineGetRepoToken   PandoraToken
	CreateTSDBRepoToken    PandoraToken
	CreateTSDBSeriesTokens map[string]PandoraToken
	CreateExportToken      map[string]PandoraToken
	UpdateExportToken      map[string]PandoraToken
	GetExportToken         map[string]PandoraToken
	ListExportToken        PandoraToken
}

type CreateRepoForTSDBInput struct {
	RepoName     string
	TSDBRepoName string
	Region       string
	Schema       []RepoSchemaEntry
	Retention    string
	SeriesName   string
	Tags         []string
	OmitInvalid  bool
	OmitEmpty    bool
	Timestamp    string
	AutoExportTSDBTokens
}

type CreateRepoForKodoInput struct {
	Retention int
	Ak        string
	Email     string
	Region    string
	Bucket    string
	RepoName  string
	Prefix    string
	Compress  bool
	Format    string
	Schema    []RepoSchemaEntry
	AutoExportKodoTokens
}

type AutoExportKodoTokens struct {
	PipelineGetRepoToken PandoraToken
	CreateExportToken    PandoraToken
	UpdateExportToken    PandoraToken
	GetExportToken       PandoraToken
	ListExportToken      PandoraToken
}

type AutoExportToKODOInput struct {
	RepoName   string
	BucketName string
	Prefix     string
	Compress   bool
	Format     string
	Email      string
	Retention  int //数字，单位为天
	AutoExportKodoTokens
}

type SeriesInfo struct {
	SeriesName string
	Tags       []string
	TimeStamp  string
	Schema     []RepoSchemaEntry
}

type CreateRepoForMutiExportTSDBInput struct {
	RepoName     string
	TSDBRepoName string
	Region       string
	Retention    string
	OmitInvalid  bool
	OmitEmpty    bool
	SeriesMap    map[string]SeriesInfo
	AutoExportTSDBTokens
}

func IsTag(key string, tags []string) bool {
	if tags == nil || len(tags) <= 0 {
		return false
	}
	for _, k := range tags {
		if key == k {
			return true
		}
	}
	return false
}

type RepoOptions struct {
	WithIP        string `json:"withIP"`
	WithTimestamp string `json:"withTimestamp"`
	UnescapeLine  bool   `json:"unescapeLine"`
}

type CreateRepoInput struct {
	PandoraToken
	RepoName    string
	Region      string            `json:"region"`
	Schema      []RepoSchemaEntry `json:"schema"`
	Options     *RepoOptions      `json:"options"`
	GroupName   string            `json:"group"`
	Workflow    string            `json:"workflow"`
	RuleNames   *[]string         `json:"ruleNames"`
	Description string            `json:"description,omitempty"`
}

func (r *CreateRepoInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}
	if r.Workflow != "" {
		if err = validateWorkflowName(r.Workflow); err != nil {
			return
		}
	}

	if r.Schema == nil || len(r.Schema) == 0 {
		err = reqerr.NewInvalidArgs("Schema", "schema should not be empty").WithComponent("pipleline")
		return
	}
	for _, schema := range r.Schema {
		if err = schema.Validate(); err != nil {
			return
		}
	}

	if r.GroupName != "" {
		if err = validateGroupName(r.GroupName); err != nil {
			return
		}
	}

	if r.Region == "" {
		err = reqerr.NewInvalidArgs("Region", "region should not be empty").WithComponent("pipleline")
		return
	}
	return
}

// ExportType选项表示同时更新的下游export和repo
// 目前支持 tsdb、logdb、kodo、all
type UpdateRepoInput struct {
	PandoraToken
	PipelineGetRepoToken PandoraToken
	RepoName             string
	workflow             string
	Schema               []RepoSchemaEntry `json:"schema"`
	Option               *SchemaFreeOption
	RepoOptions          *RepoOptions `json:"options"`
	RuleNames            *[]string    `json:"ruleNames"`
	Description          string       `json:"description,omitempty"`
}

func (r *UpdateRepoInput) IsTag(key string) bool {
	tags := r.Option.SeriesTags[r.Option.SeriesName]
	if r == nil || r.Option == nil || len(tags) <= 0 {
		return false
	}
	for _, k := range tags {
		if key == k {
			return true
		}
	}
	return false
}

func (r *UpdateRepoInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}

	if r.Schema == nil || len(r.Schema) == 0 {
		err = reqerr.NewInvalidArgs("Schema", "schema should not be empty").WithComponent("pipleline")
		return
	}
	for _, schema := range r.Schema {
		if err = schema.Validate(); err != nil {
			return
		}
	}
	return
}

type GetRepoInput struct {
	PandoraToken
	RepoName string
}

type RepoExistInput GetRepoInput

func (r *RepoExistInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}
	return
}

type RepoExistOutput struct {
	Exist bool `json:"exist"`
}

type GetSampleDataInput struct {
	PandoraToken
	RepoName string
	Count    int //最多10条
}

type GetRepoOutput struct {
	Region      string            `json:"region"`
	Schema      []RepoSchemaEntry `json:"schema"`
	GroupName   string            `json:"group"`
	Options     *RepoOptions      `json:"options"`
	DerivedFrom string            `json:"derivedFrom"`
	FromDag     bool              `json:"fromDag"`
	Workflow    string            `json:"workflow"`
	RuleNames   *[]string         `json:"ruleNames"`
	Description string            `json:"description,omitempty"`
}

type SampleDataOutput struct {
	Values []map[string]interface{} `json:"records"`
}

type RepoDesc struct {
	RepoName    string    `json:"name"`
	Region      string    `json:"region"`
	GroupName   string    `json:"group"`
	DerivedFrom string    `json:"derivedFrom"`
	FromDag     bool      `json:"fromDag"`
	Workflow    string    `json:"workflow"`
	RuleNames   *[]string `json:"ruleNames"`
	Description string    `json:"description,omitempty"`
}

type ListReposInput struct {
	PandoraToken
	WithDag bool `json:"-"`
}

type ListReposOutput struct {
	Repos []RepoDesc `json:"repos"`
}

type DeleteRepoInput struct {
	PandoraToken
	RepoName string
}

type PointField struct {
	Key   string
	Value interface{}
}

func (p *PointField) Bytes() []byte {
	if p == nil || p.Value == nil {
		return []byte("")
	}
	typ := reflect.TypeOf(p.Value).Kind()
	var value []byte
	if typ == reflect.Map || typ == reflect.Slice {
		v, _ := json.Marshal(p.Value)
		value = escapeBytesField(v)
	} else if typ == reflect.String {
		value = escapeBytesField([]byte(reflect.ValueOf(p.Value).String()))
	} else {
		value = escapeBytesField([]byte(fmt.Sprintf("%v", p.Value)))
	}
	ret := []byte(p.Key)
	ret = append(ret, '=')
	ret = append(ret, value...)
	ret = append(ret, '\t')
	return ret
}

func (p *PointField) String() string {
	if p == nil || p.Value == nil {
		return ""
	}
	typ := reflect.TypeOf(p.Value).Kind()
	var value string
	if typ == reflect.Map || typ == reflect.Slice {
		v, _ := json.Marshal(p.Value)
		value = escapeStringField(string(v))
	} else if typ == reflect.String {
		value = escapeStringField(reflect.ValueOf(p.Value).String())
	} else {
		value = escapeStringField(fmt.Sprintf("%v", p.Value))
	}
	return p.Key + "=" + value + "\t"
}

type Point struct {
	Fields []PointField
}

func (p Point) ToString() (bs string) {
	var buf bytes.Buffer
	for _, field := range p.Fields {
		buf.WriteString(field.String())
	}
	if len(p.Fields) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte('\n')
	return buf.String()
}

func (p Point) ToBytes() []byte {
	var buf bytes.Buffer
	for _, field := range p.Fields {
		buf.Write(field.Bytes())
	}
	if len(p.Fields) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte('\n')
	return buf.Bytes()
}

type Points []Point

func (ps Points) Buffer() []byte {
	var buf bytes.Buffer
	for _, p := range ps {
		for _, field := range p.Fields {
			buf.Write(field.Bytes())
		}
		if len(p.Fields) > 0 {
			buf.Truncate(buf.Len() - 1)
		}
		buf.WriteByte('\n')
	}
	if len(ps) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	return buf.Bytes()
}

func escapeStringField(in string) string {
	var out []byte
	for i := 0; i < len(in); i++ {
		switch in[i] {
		case '\t': // escape tab
			out = append(out, '\\')
			out = append(out, 't')
		case '\n': // escape new line
			out = append(out, '\\')
			out = append(out, 'n')
		default:
			out = append(out, in[i])
		}
	}
	return string(out)
}

func escapeBytesField(in []byte) []byte {
	var out []byte
	for i := 0; i < len(in); i++ {
		switch in[i] {
		case '\t': // escape tab
			out = append(out, '\\')
			out = append(out, 't')
		case '\n': // escape new line
			out = append(out, '\\')
			out = append(out, 'n')
		default:
			out = append(out, in[i])
		}
	}
	return out
}

type PostDataInput struct {
	PandoraToken
	ResourceOwner string
	RepoName      string
	Points        Points
}

type SchemaFreeInput struct {
	SchemaFreeToken
	Datas        Datas
	NoUpdate     bool
	Region       string
	RepoName     string
	WorkflowName string
	Description  string
	Option       *SchemaFreeOption
	RepoOptions  *RepoOptions
}

type SchemaFreeToken struct {
	PipelineCreateRepoToken        PandoraToken
	PipelinePostDataToken          PandoraToken
	PipelineGetRepoToken           PandoraToken
	PipelineUpdateRepoToken        PandoraToken
	PipelineGetWorkflowToken       PandoraToken
	PipelineCreateWorkflowToken    PandoraToken
	PipelineStartWorkflowToken     PandoraToken
	PipelineStopWorkflowToken      PandoraToken
	PipelineGetWorkflowStatusToken PandoraToken
}

type InitOrUpdateWorkflowInput struct {
	SchemaFreeToken
	InitOptionChange bool
	SchemaFree       bool
	Region           string
	RepoName         string
	WorkflowName     string
	RepoOptions      *RepoOptions
	Schema           []RepoSchemaEntry
	Option           *SchemaFreeOption
	Description      string
}

type SchemaFreeOption struct {
	ToLogDB          bool
	ToTSDB           bool
	ToKODO           bool
	ForceDataConvert bool
	NumberUseFloat   bool
	AutoExportToLogDBInput
	AutoExportToKODOInput
	AutoExportToTSDBInput
}

type PostDataFromFileInput struct {
	PandoraToken
	RepoName string
	FilePath string
}

type PostDataFromReaderInput struct {
	PandoraToken
	RepoName   string
	Reader     io.ReadSeeker
	BodyLength int64
}

type PostDataFromBytesInput struct {
	PandoraToken
	RepoName string
	Buffer   []byte
}

type UploadPluginInput struct {
	PandoraToken
	ResourceOwner string
	PluginName    string
	Buffer        *bytes.Buffer
}

type UploadPluginFromFileInput struct {
	PandoraToken
	ResourceOwner string
	PluginName    string
	FilePath      string
}

type GetPluginInput struct {
	PandoraToken
	ResourceOwner string
	PluginName    string
}

type PluginDesc struct {
	PluginName string `json:"name"`
	CreateTime string `json:"createTime"`
}

type GetPluginOutput struct {
	PluginDesc
}

type VerifyPluginInput struct {
	PandoraToken
	ResourceOwner string
	PluginName    string
}

type OutputField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type VerifyPluginOutput struct {
	OutputFields []OutputField `json:"outputFields"`
}

type ListPluginsInput struct {
	PandoraToken
	ResourceOwner string
}

type ListPluginsOutput struct {
	Plugins []PluginDesc `json:"plugins"`
}

type DeletePluginInput struct {
	PandoraToken
	ResourceOwner string
	PluginName    string
}

type TransformPluginOutputEntry struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

type TransformPlugin struct {
	Name   string                       `json:"name"`
	Output []TransformPluginOutputEntry `json:"output"`
}

type TransformSpec struct {
	Plugin    *TransformPlugin  `json:"plugin,omitempty"`
	Mode      string            `json:"mode,omitempty"`
	Code      string            `json:"code,omitempty"`
	Interval  string            `json:"interval,omitempty"`
	Container *Container        `json:"container,omitempty"`
	Whence    string            `json:"whence,omitempty"`
	Schema    []RepoSchemaEntry `json:"destrepo,omitempty"`
}

func (t *TransformSpec) Validate() (err error) {
	if t.Mode == "" && t.Code == "" && t.Plugin == nil {
		err = reqerr.NewInvalidArgs("TransformSpec", "all mode, code and plugin can not be empty").WithComponent("pipleline")
		return
	}
	if t.Container != nil {
		if err = t.Container.Validate(); err != nil {
			return
		}
	}
	return
}

type CreateTransformInput struct {
	PandoraToken
	SrcRepoName   string
	TransformName string
	DestRepoName  string
	Spec          *TransformSpec
}

func (t *CreateTransformInput) Validate() (err error) {
	if err = validateRepoName(t.SrcRepoName); err != nil {
		return
	}
	if err = validateRepoName(t.DestRepoName); err != nil {
		return
	}
	if err = validateTransformName(t.TransformName); err != nil {
		return
	}
	if t.SrcRepoName == t.DestRepoName {
		err = reqerr.NewInvalidArgs("DestRepoName", "dest repo name should be different to src repo name").WithComponent("pipleline")
		return
	}
	return t.Spec.Validate()
}

type UpdateTransformInput struct {
	PandoraToken
	SrcRepoName   string
	TransformName string
	Spec          *TransformSpec
}

func (t *UpdateTransformInput) Validate() (err error) {
	if err = validateRepoName(t.SrcRepoName); err != nil {
		return
	}
	if err = validateTransformName(t.TransformName); err != nil {
		return
	}
	return t.Spec.Validate()
}

type TransformDesc struct {
	TransformName string         `json:"name"`
	DestRepoName  string         `json:"to"`
	Spec          *TransformSpec `json:"spec"`
	Workflow      string         `json:"workflow"`
}

type GetTransformInput struct {
	PandoraToken
	RepoName      string
	TransformName string
}

type TransformExistInput GetTransformInput

func (r *TransformExistInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}
	if err = validateTransformName(r.TransformName); err != nil {
		return
	}
	return
}

type TransformExistOutput struct {
	Exist bool `json:"exist"`
}

type GetTransformOutput struct {
	TransformDesc
}

type DeleteTransformInput struct {
	PandoraToken
	RepoName      string
	TransformName string
}

type ListTransformsInput struct {
	PandoraToken
	RepoName string
}

type ListTransformsOutput struct {
	Transforms []TransformDesc `json:"transforms"`
}

type ExportTsdbSpec struct {
	DestRepoName string            `json:"destRepoName"`
	SeriesName   string            `json:"series"`
	Tags         map[string]string `json:"tags"`
	Fields       map[string]string `json:"fields"`
	OmitInvalid  bool              `json:"omitInvalid"`
	OmitEmpty    bool              `json:"omitEmpty"`
	Timestamp    string            `json:"timestamp,omitempty"`
}

func (s *ExportTsdbSpec) Validate() (err error) {
	if s.DestRepoName == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "dest repo name should not be empty").WithComponent("pipleline")
		return
	}
	if s.SeriesName == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "series name should not be empty").WithComponent("pipleline")
		return
	}
	return
}

type ExportMongoSpec struct {
	Host      string                 `json:"host"`
	DbName    string                 `json:"dbName"`
	CollName  string                 `json:"collName"`
	Mode      string                 `json:"mode"`
	UpdateKey []string               `json:"updateKey,omitempty"`
	Doc       map[string]interface{} `json:"doc"`
	Version   string                 `json:"version,omitempty"`
}

func (s *ExportMongoSpec) Validate() (err error) {
	if s.Host == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "host should not be empty").WithComponent("pipleline")
		return
	}
	if s.DbName == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "dbname should not be empty").WithComponent("pipleline")
		return
	}
	if s.CollName == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "collection name should not be empty").WithComponent("pipleline")
		return
	}
	if s.Mode != "UPSERT" && s.Mode != "INSERT" && s.Mode != "UPDATE" {
		err = reqerr.NewInvalidArgs("ExportSpec", fmt.Sprintf("invalid mode: %s, mode should be one of \"UPSERT\", \"INSERT\" and \"UPDATE\"", s.Mode)).WithComponent("pipleline")
		return
	}
	return
}

type ExportLogDBSpec struct {
	DestRepoName string                 `json:"destRepoName"`
	Doc          map[string]interface{} `json:"doc"`
	OmitInvalid  bool                   `json:"omitInvalid"`
	OmitEmpty    bool                   `json:"omitEmpty"`
}

func (s *ExportLogDBSpec) Validate() (err error) {
	if s.DestRepoName == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "dest repo name should not be empty").WithComponent("pipleline")
		return
	}
	return
}

type ExportKodoSpec struct {
	Bucket         string            `json:"bucket"`
	KeyPrefix      string            `json:"keyPrefix"`
	Fields         map[string]string `json:"fields"`
	RotateStrategy string            `json:"rotateStrategy,omitempty"`
	RotateSize     int               `json:"rotateSize,omitempty"`
	RotateInterval int               `json:"rotateInterval,omitempty"`
	Email          string            `json:"email"`
	AccessKey      string            `json:"accessKey"`
	Format         string            `json:"format"`
	Delimiter      string            `json:"delimiter,omitempty"`
	Compress       bool              `json:"compress"`
	Retention      int               `json:"retention"`
}

func (s *ExportKodoSpec) Validate() (err error) {
	if s.Bucket == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "bucket should not be empty").WithComponent("pipleline")
		return
	}
	return
}

type ExportHttpSpec struct {
	Host   string `json:"host"`
	Uri    string `json:"uri"`
	Format string `json:"format,omitempty"`
}

func (s *ExportHttpSpec) Validate() (err error) {
	if s.Host == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "host should not be empty").WithComponent("pipleline")
		return
	}
	if s.Uri == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "uri should not be empty").WithComponent("pipleline")
		return
	}
	return
}

type ExportHDFSSpec struct {
	Path           string            `json:"path"`
	User           string            `json:"user"`
	Fields         map[string]string `json:"fields"`
	RotateStrategy string            `json:"rotateStrategy"`
	RotateSize     int               `json:"rotateSize"`
	RotateInterval int               `json:"rotateInterval"`
	Format         string            `json:"format"`
	Delimiter      string            `json:"delimiter"`
	Compress       bool              `json:"compress"`
}

func (s *ExportHDFSSpec) Validate() (err error) {
	if s.Path == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "path should not be empty").WithComponent("pipleline")
		return
	}
	if s.User == "" {
		err = reqerr.NewInvalidArgs("ExportSpec", "user should not be empty").WithComponent("pipleline")
		return
	}
	if len(s.Fields) == 0 {
		err = reqerr.NewInvalidArgs("ExportSpec", "fields should not be empty").WithComponent("pipleline")
		return
	}
	return
}

type CreateExportInput struct {
	PandoraToken
	RepoName   string      `json:"-"`
	ExportName string      `json:"-"`
	Type       string      `json:"type"`
	Spec       interface{} `json:"spec"`
	Whence     string      `json:"whence,omitempty"`
}

func (e *CreateExportInput) Validate() (err error) {
	if err = validateRepoName(e.RepoName); err != nil {
		return
	}
	if err = validateExportName(e.ExportName); err != nil {
		return
	}
	if e.Spec == nil {
		err = reqerr.NewInvalidArgs("ExportSpec", "spec should not be nil").WithComponent("pipleline")
		return
	}
	if e.Whence != "" && e.Whence != "oldest" && e.Whence != "newest" {
		err = reqerr.NewInvalidArgs("ExportSpec", "whence must be empty, \"oldest\" or \"newest\"").WithComponent("pipleline")
		return
	}

	switch e.Spec.(type) {
	case *ExportTsdbSpec, ExportTsdbSpec:
		e.Type = ExportTypeTSDB
	case *ExportMongoSpec, ExportMongoSpec:
		e.Type = ExportTypeMongo
	case *ExportLogDBSpec, ExportLogDBSpec:
		e.Type = ExportTypeLogDB
	case *ExportKodoSpec, ExportKodoSpec:
		e.Type = ExportTypeKODO
	case *ExportHttpSpec, ExportHttpSpec:
		e.Type = ExportTypeHTTP
	case *ExportHDFSSpec, ExportHDFSSpec:
		e.Type = ExportTypeHDFS
	default:
		return
	}

	vv, ok := e.Spec.(base.Validator)
	if !ok {
		err = reqerr.NewInvalidArgs("ExportSpec", "export spec cannot cast to validator").WithComponent("pipleline")
		return
	}
	return vv.Validate()
}

type UpdateExportInput struct {
	PandoraToken
	RepoName   string      `json:"-"`
	ExportName string      `json:"-"`
	Spec       interface{} `json:"spec"`
}

func (e *UpdateExportInput) Validate() (err error) {
	if err = validateRepoName(e.RepoName); err != nil {
		return
	}
	if err = validateExportName(e.ExportName); err != nil {
		return
	}
	if e.Spec == nil {
		err = reqerr.NewInvalidArgs("ExportSpec", "spec should not be nil").WithComponent("pipleline")
		return
	}
	switch e.Spec.(type) {
	case *ExportTsdbSpec, ExportTsdbSpec, *ExportMongoSpec, ExportMongoSpec,
		*ExportLogDBSpec, ExportLogDBSpec, *ExportKodoSpec, ExportKodoSpec,
		*ExportHttpSpec, ExportHttpSpec, *ExportHDFSSpec, ExportHDFSSpec:
	default:
		return
	}
	vv, ok := e.Spec.(base.Validator)
	if !ok {
		err = reqerr.NewInvalidArgs("ExportSpec", "export spec cannot cast to validator").WithComponent("pipleline")
		return
	}
	return vv.Validate()
}

type ExportDesc struct {
	Name     string                 `json:"name,omitempty"`
	Type     string                 `json:"type"`
	Spec     map[string]interface{} `json:"spec"`
	Whence   string                 `json:"whence,omitempty"`
	Workflow string                 `json:"workflow"`
}

type GetExportInput struct {
	PandoraToken
	RepoName   string
	ExportName string
}

type GetExportOutput struct {
	ExportDesc
}

type ExportExistInput GetExportInput

func (r *ExportExistInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}
	if err = validateExportName(r.ExportName); err != nil {
		return
	}
	return
}

type ExportExistOutput struct {
	Exist bool `json:"exist"`
}

type ListExportsInput struct {
	PandoraToken
	RepoName string
}

type ListExportsOutput struct {
	Exports []ExportDesc `json:"exports"`
}

type DeleteExportInput struct {
	PandoraToken
	RepoName   string
	ExportName string
}

type VerifyTransformInput struct {
	PandoraToken
	Schema []RepoSchemaEntry `json:"schema"`
	Spec   *TransformSpec    `json:"spec"`
}

func (v *VerifyTransformInput) Validate() (err error) {
	if v.Schema == nil || len(v.Schema) == 0 {
		err = reqerr.NewInvalidArgs("Schema", "schema should not be empty").WithComponent("pipleline")
		return
	}
	for _, item := range v.Schema {
		if err = item.Validate(); err != nil {
			return
		}
	}

	return v.Spec.Validate()
}

type VerifyTransformOutput struct {
	Schema []RepoSchemaEntry `json:"schema"`
}

type VerifyExportInput struct {
	PandoraToken
	Schema []RepoSchemaEntry `json:"schema"`
	Type   string            `json:"type"`
	Spec   interface{}       `json:"spec"`
	Whence string            `json:"whence,omitempty"`
}

func (v *VerifyExportInput) Validate() (err error) {
	if v.Schema == nil || len(v.Schema) == 0 {
		err = reqerr.NewInvalidArgs("VerifyExportSpec", "schema should not be empty").WithComponent("pipleline")
		return
	}
	for _, item := range v.Schema {
		if err = item.Validate(); err != nil {
			return
		}
	}

	if v.Spec == nil {
		err = reqerr.NewInvalidArgs("ExportSpec", "spec should not be nil").WithComponent("pipleline")
		return
	}

	if v.Whence != "" && v.Whence != "oldest" && v.Whence != "newest" {
		err = reqerr.NewInvalidArgs("ExportSpec", "whence must be empty, \"oldest\" or \"newest\"").WithComponent("pipleline")
		return
	}

	switch v.Spec.(type) {
	case *ExportTsdbSpec, ExportTsdbSpec:
		v.Type = "tsdb"
	case *ExportMongoSpec, ExportMongoSpec:
		v.Type = "mongo"
	case *ExportLogDBSpec, ExportLogDBSpec:
		v.Type = "logdb"
	case *ExportKodoSpec, ExportKodoSpec:
		v.Type = "kodo"
	case *ExportHttpSpec, ExportHttpSpec:
		v.Type = "http"
	case *ExportHDFSSpec, ExportHDFSSpec:
		v.Type = "hdfs"
	default:
		return
	}

	vv, ok := v.Spec.(base.Validator)
	if !ok {
		err = reqerr.NewInvalidArgs("ExportSpec", "export spec cannot cast to validator").WithComponent("pipleline")
		return
	}
	return vv.Validate()
}

type KodoSourceSpec struct {
	Bucket      string   `json:"bucket"`
	KeyPrefixes []string `json:"keyPrefixes"`
	FileType    string   `json:"fileType"`
}

func (k *KodoSourceSpec) Validate() (err error) {
	if k.Bucket == "" {
		return reqerr.NewInvalidArgs("Bucket", fmt.Sprintf("bucket name should not be empty")).WithComponent("pipleline")
	}
	if k.FileType == "" {
		return reqerr.NewInvalidArgs("FileType", fmt.Sprintf("fileType should not be empty")).WithComponent("pipleline")
	}

	return
}

type HdfsSourceSpec struct {
	Paths    []string `json:"paths"`
	FileType string   `json:"fileType"`
}

func (h *HdfsSourceSpec) Validate() (err error) {
	if len(h.Paths) == 0 {
		return reqerr.NewInvalidArgs("Paths", fmt.Sprintf("paths should not be empty")).WithComponent("pipleline")
	}
	for _, path := range h.Paths {
		if path == "" {
			return reqerr.NewInvalidArgs("Path", fmt.Sprintf("path in paths should not be empty")).WithComponent("pipleline")
		}
	}
	if h.FileType == "" {
		return reqerr.NewInvalidArgs("FileType", fmt.Sprintf("fileType should not be empty")).WithComponent("pipleline")
	}

	return
}

type FusionSourceSpec struct {
	Domains    []string `json:"domains"`
	FileFilter string   `json:"fileFilter,omitempty"`
}

func (h *FusionSourceSpec) Validate() (err error) {
	if len(h.Domains) == 0 {
		return reqerr.NewInvalidArgs("Domains", fmt.Sprintf("domains should not be empty")).WithComponent("pipleline")
	}
	// 目前仅支持单域名
	if len(h.Domains) != 1 {
		return reqerr.NewInvalidArgs("Domains", fmt.Sprintf("only one domain is supported")).WithComponent("pipleline")
	}
	for _, domain := range h.Domains {
		if domain == "" {
			return reqerr.NewInvalidArgs("Domain", fmt.Sprintf("domain in domains should not be empty")).WithComponent("pipleline")
		}
	}
	if h.FileFilter == "" {
		return reqerr.NewInvalidArgs("FileFilter", fmt.Sprintf("fileFilter should not be empty")).WithComponent("pipleline")
	}

	return
}

type RetrieveSchemaInput struct {
	PandoraToken
	Type string      `json:"type"`
	Spec interface{} `json:"spec"`
}

func (r *RetrieveSchemaInput) Validate() (err error) {
	switch r.Spec.(type) {
	case *KodoSourceSpec, KodoSourceSpec:
		r.Type = "kodo"
	case *HdfsSourceSpec, HdfsSourceSpec:
		r.Type = "hdfs"
	case *FusionSourceSpec, FusionSourceSpec:
		r.Type = "fusion"
	default:
		return
	}

	vv, ok := r.Spec.(base.Validator)
	if !ok {
		err = reqerr.NewInvalidArgs("Spec", "data source spec cannot cast to validator").WithComponent("pipleline")
		return
	}
	return vv.Validate()
}

type RetrieveSchemaOutput struct {
	Schema []RepoSchemaEntry `json:"schema"`
}

type CreateDatasourceInput struct {
	PandoraToken
	DatasourceName string            `json:"-"`
	Region         string            `json:"region"`
	Type           string            `json:"type"`
	Spec           interface{}       `json:"spec"`
	Schema         []RepoSchemaEntry `json:"schema"`
	NoVerifySchema bool              `json:"noVerifySchema"` // 是否触发推断 schema，可选项，默认值为 false
	Workflow       string            `json:"workflow"`
}

func (c *CreateDatasourceInput) Validate() (err error) {
	if c.DatasourceName == "" {
		return reqerr.NewInvalidArgs("DatasourceName", fmt.Sprintf("datasource name should not be empty")).WithComponent("pipleline")
	}
	if c.Type == "" {
		return reqerr.NewInvalidArgs("Type", fmt.Sprintf("type of datasource should not be empty")).WithComponent("pipleline")
	}
	if c.Workflow != "" {
		if err = validateWorkflowName(c.Workflow); err != nil {
			return
		}
	}
	if len(c.Schema) == 0 {
		return reqerr.NewInvalidArgs("Schema", fmt.Sprintf("schema of datasource should not be empty")).WithComponent("pipleline")
	}
	for _, schema := range c.Schema {
		if err = schema.Validate(); err != nil {
			return
		}
	}

	switch c.Spec.(type) {
	case *KodoSourceSpec, KodoSourceSpec:
		c.Type = "kodo"
	case *HdfsSourceSpec, HdfsSourceSpec:
		c.Type = "hdfs"
	case *FusionSourceSpec, FusionSourceSpec:
		c.Type = "fusion"
	default:
		return
	}

	vv, ok := c.Spec.(base.Validator)
	if !ok {
		err = reqerr.NewInvalidArgs("Spec", "data source spec cannot cast to validator").WithComponent("pipleline")
		return
	}
	return vv.Validate()
}

type GetDatasourceInput struct {
	PandoraToken
	DatasourceName string
}

type GetDatasourceOutput struct {
	Region   string            `json:"region"`
	Type     string            `json:"type"`
	Spec     interface{}       `json:"spec"`
	Schema   []RepoSchemaEntry `json:"schema"`
	FromDag  bool              `json:"fromDag,omitempty"`
	Workflow string            `json:"workflow"`
}

type DatasourceExistInput GetDatasourceInput

func (r *DatasourceExistInput) Validate() (err error) {
	if err = validateDatasouceName(r.DatasourceName); err != nil {
		return
	}
	return
}

type DatasourceExistOutput struct {
	Exist bool `json:"exist"`
}

type DatasourceDesc struct {
	Name     string            `json:"name"`
	Region   string            `json:"region"`
	Type     string            `json:"type"`
	Spec     interface{}       `json:"spec"`
	Schema   []RepoSchemaEntry `json:"schema"`
	Workflow string            `json:"workflow"`
}

type ListDatasourcesOutput struct {
	Datasources []DatasourceDesc `json:"datasources"`
}

type DeleteDatasourceInput struct {
	PandoraToken
	DatasourceName string
}

type JobSrc struct {
	SrcName    string `json:"name"`
	FileFilter string `json:"fileFilter"`
	Type       string `json:"type"`
	TableName  string `json:"tableName"`
}

func (s *JobSrc) Validate() (err error) {
	if s.SrcName == "" {
		return reqerr.NewInvalidArgs("SrcName", fmt.Sprintf("source name should not be empty")).WithComponent("pipleline")
	}
	if s.Type == "" {
		return reqerr.NewInvalidArgs("Type", fmt.Sprintf("source type should not be empty")).WithComponent("pipleline")
	}
	if s.TableName == "" {
		return reqerr.NewInvalidArgs("TableName", fmt.Sprintf("table name should not be empty")).WithComponent("pipleline")
	}

	return
}

type Computation struct {
	Code string `json:"code"`
	Type string `json:"type"`
}

func (c *Computation) Validate() (err error) {
	if c.Code == "" {
		return reqerr.NewInvalidArgs("Code", fmt.Sprintf("code in computation should not be empty")).WithComponent("pipleline")
	}
	if c.Type == "" {
		return reqerr.NewInvalidArgs("Type", fmt.Sprintf("type in computation should not be empty")).WithComponent("pipleline")
	}

	return
}

type JobSchedulerSpec struct {
	Crontab string `json:"crontab,omitempty"`
	Loop    string `json:"loop,omitempty"`
}

type JobScheduler struct {
	Type string            `json:"type"`
	Spec *JobSchedulerSpec `json:"spec,omitempty"`
}

type Param struct {
	Name    string `json:"name"`
	Value   string `json:"value,omitempty"`
	Default string `json:"default"`
}

type CreateJobInput struct {
	PandoraToken
	JobName     string        `json:"-"`
	Srcs        []JobSrc      `json:"srcs"`
	Computation Computation   `json:"computation"`
	Container   *Container    `json:"container,omitempty"`
	Scheduler   *JobScheduler `json:"scheduler,omitempty"`
	Params      []Param       `json:"params,omitempty"`
}

func (c *CreateJobInput) Validate() (err error) {
	if c.JobName == "" {
		return reqerr.NewInvalidArgs("JobName", fmt.Sprintf("job name should not be empty")).WithComponent("pipleline")
	}
	if len(c.Srcs) == 0 {
		return reqerr.NewInvalidArgs("Srcs", fmt.Sprintf("must have at least one src inside the job srcs")).WithComponent("pipleline")
	}
	for _, src := range c.Srcs {
		if err = src.Validate(); err != nil {
			return
		}
	}
	if err = c.Computation.Validate(); err != nil {
		return
	}

	return
}

type GetJobInput struct {
	PandoraToken
	JobName string
}

type JobExistInput GetJobInput

func (r *JobExistInput) Validate() (err error) {
	if err = validateJobName(r.JobName); err != nil {
		return
	}
	return
}

type JobExistOutput struct {
	Exist bool `json:"exist"`
}

type GetJobOutput struct {
	Srcs        []JobSrc      `json:"srcs"`
	Computation Computation   `json:"computation"`
	Container   *Container    `json:"container,omitempty"`
	Scheduler   *JobScheduler `json:"scheduler,omitempty"`
	Params      []Param       `json:"params,omitempty"`
	Workflow    string        `json:"workflow"`
}

type JobDesc struct {
	Name        string        `json:"name"`
	Srcs        []JobSrc      `json:"srcs"`
	Computation Computation   `json:"computation"`
	Container   *Container    `json:"container,omitempty"`
	Scheduler   *JobScheduler `json:"scheduler,omitempty"`
	Params      []Param       `json:"params,omitempty"`
}

type ListJobsInput struct {
	PandoraToken
	SrcJobName        string
	SrcDatasourceName string
}

type ListJobsOutput struct {
	Jobs []JobDesc `json:"jobs"`
}

type DeleteJobInput struct {
	PandoraToken
	JobName string
}

type StartJobInput struct {
	PandoraToken
	JobName string  `json:"-"`
	Params  []Param `json:"params,omitempty"`
}

func (s *StartJobInput) Validate() (err error) {
	if s.JobName == "" {
		return reqerr.NewInvalidArgs("JobName", fmt.Sprintf("job name should not be empty")).WithComponent("pipleline")
	}

	return
}

type StopJobInput struct {
	PandoraToken
	JobName string
}

type GetJobHistoryInput struct {
	PandoraToken
	ResourceOwner string
	JobName       string
}

type JobHistory struct {
	RunId     int64  `json:"runId"`
	BatchTime string `json:"batchTime"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	Duration  int64  `json:"duration"`
	Status    string `json:"status"`
	Message   string `json:"message"`
}

type GetJobHistoryOutput struct {
	Total   int64        `json:"total"`
	History []JobHistory `json:"history"`
}

type StopJobBatchInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	JobName       string `json:"jobName"`
	RunId         int    `json:"runId"`
}

func (s *StopJobBatchInput) Validate() (err error) {
	if s.JobName == "" {
		return reqerr.NewInvalidArgs("JobName", fmt.Sprintf("job name should not be empty")).WithComponent("pipleline")
	}
	return
}

type StopJobBatchOutput struct {
	PreStatus  string `json:"preStatus"`
	PostStatus string `json:"postStatus"`
}

type RerunJobBatchInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	JobName       string `json:"jobName"`
	RunId         int    `json:"runId"`
}

func (s *RerunJobBatchInput) Validate() (err error) {
	if s.JobName == "" {
		return reqerr.NewInvalidArgs("JobName", fmt.Sprintf("job name should not be empty")).WithComponent("pipleline")
	}
	return
}

type RerunJobBatchOutput struct {
	PreStatus  string `json:"preStatus"`
	PostStatus string `json:"postStatus"`
	RerunCount int    `json:"rerunCount"`
}

type JobExportKodoSpec struct {
	Bucket         string   `json:"bucket"`
	KeyPrefix      string   `json:"keyPrefix"`
	Format         string   `json:"format"`
	Compression    string   `json:"compression,omitempty"`
	Retention      int      `json:"retention"`
	PartitionBy    []string `json:"partitionBy,omitempty"`
	FileCount      int      `json:"fileCount"`
	SaveMode       string   `json:"saveMode"`
	Delimiter      string   `json:"delimiter,omitempty"`
	ContainsHeader bool     `json:"containsHeader"`
}

func (e *JobExportKodoSpec) Validate() (err error) {
	if e.Bucket == "" {
		return reqerr.NewInvalidArgs("Bucket", fmt.Sprintf("bucket name should not be empty")).WithComponent("pipleline")
	}
	if e.Format == "" {
		return reqerr.NewInvalidArgs("Format", fmt.Sprintf("format should not be empty")).WithComponent("pipleline")
	}
	if strings.ToLower(e.Format) == "csv" {
		if e.Delimiter == "" {
			return reqerr.NewInvalidArgs("Delimiter", fmt.Sprintf("csv's delimiter should not be empty")).WithComponent("pipleline")
		}
	}
	if e.FileCount <= 0 {
		return reqerr.NewInvalidArgs("FileCount", fmt.Sprintf("fileCount should be larger than 0")).WithComponent("pipleline")
	}
	return
}

type JobExportHdfsSpec struct {
	Path        string   `json:"path"`
	Format      string   `json:"format"`
	Compression string   `json:"compression,omitempty"`
	PartitionBy []string `json:"partitionBy"`
	FileCount   int      `json:"fileCount"`
	SaveMode    string   `json:"saveMode"`
}

func (e *JobExportHdfsSpec) Validate() (err error) {
	return nil
}

type JobExportLogdbSpec struct {
	DestRepoName string            `json:"destRepoName"`
	OmitInvalid  bool              `json:"omitInvalid,omitempty"`
	Doc          map[string]string `json:"doc"`
}

func (e *JobExportLogdbSpec) Validate() (err error) {
	return nil
}

type JobExportTsdbSpec struct {
	DestRepoName string            `json:"destRepoName"`
	OmitInvalid  bool              `json:"omitInvalid,omitempty"`
	SeriesName   string            `json:"series"`
	Tags         map[string]string `json:"tags"`
	Fields       map[string]string `json:"fields"`
	Timestamp    string            `json:"timestamp,omitempty"`
}

func (e *JobExportTsdbSpec) Validate() (err error) {
	return nil
}

type JobExportHttpSpec struct {
	Host   string `json:"host"`
	Uri    string `json:"uri"`
	Format string `json:"format,omitempty"`
}

func (e *JobExportHttpSpec) Validate() (err error) {
	if e.Host == "" {
		return reqerr.NewInvalidArgs("Host", fmt.Sprintf("host should not be empty")).WithComponent("pipleline")
	}
	if !strings.HasPrefix(e.Uri, "/") {
		return reqerr.NewInvalidArgs("Uri", fmt.Sprintf("uri should start with '/'")).WithComponent("pipleline")
	}
	return nil
}

type CreateJobExportInput struct {
	PandoraToken
	JobName    string      `json:"-"`
	ExportName string      `json:"-"`
	Type       string      `json:"type"`
	Spec       interface{} `json:"spec"`
}

func (e *CreateJobExportInput) Validate() (err error) {
	if err = validateJobName(e.JobName); err != nil {
		return
	}
	if err = validateJobexportName(e.ExportName); err != nil {
		return
	}

	switch e.Spec.(type) {
	case *JobExportKodoSpec, JobExportKodoSpec:
		e.Type = "kodo"
	case *JobExportHdfsSpec, JobExportHdfsSpec:
		e.Type = "hdfs"
	case *JobExportLogdbSpec, JobExportLogdbSpec:
		e.Type = "logdb"
	case *JobExportTsdbSpec, JobExportTsdbSpec:
		e.Type = "tsdb"
	case *JobExportHttpSpec, JobExportHttpSpec:
		e.Type = "http"
	default:
		return
	}

	vv, ok := e.Spec.(base.Validator)
	if !ok {
		err = reqerr.NewInvalidArgs("JobExportSpec", "job export spec cannot cast to validator").WithComponent("pipleline")
		return
	}
	return vv.Validate()
}

type GetJobExportInput struct {
	PandoraToken
	JobName    string
	ExportName string
}

type JobExportExistInput GetJobExportInput

func (r *JobExportExistInput) Validate() (err error) {
	if err = validateJobName(r.JobName); err != nil {
		return
	}
	if err = validateJobexportName(r.ExportName); err != nil {
		return
	}
	return
}

type JobExportExistOutput struct {
	Exist bool `json:"exist"`
}

type GetJobExportOutput struct {
	Type     string      `json:"type"`
	Spec     interface{} `json:"spec"`
	Workflow string      `json:"workflow"`
}

type JobExportDesc struct {
	ExportName string      `json:"name"`
	Type       string      `json:"type"`
	Spec       interface{} `json:"spec"`
}

type ListJobExportsInput struct {
	PandoraToken
	JobName string
}

type ListJobExportsOutput struct {
	Exports []JobExportDesc `json:"exports"`
}

type DeleteJobExportInput struct {
	PandoraToken
	JobName    string
	ExportName string
}

type UploadUdfInput struct {
	PandoraToken
	UdfName string
	Buffer  *bytes.Buffer
}

type UploadUdfFromFileInput struct {
	PandoraToken
	UdfName  string
	FilePath string
}

type PutUdfMetaInput struct {
	PandoraToken
	UdfName     string `json:"-"`
	Description string `json:"description"`
}

const MaxDescriptionLen = 1500

func (e *PutUdfMetaInput) Validate() error {
	if (len(e.Description)) > MaxDescriptionLen {
		return reqerr.NewInvalidArgs("PutUdfMeta", fmt.Sprintf("udf description must not be larger than %d", MaxDescriptionLen)).WithComponent("pipleline")
	}
	return nil
}

type DeleteUdfInfoInput struct {
	PandoraToken
	UdfName string
}

type PageRequest struct {
	From int
	Size int
	Sort string
}

type ListUdfsInput struct {
	PandoraToken
	PageRequest
	ResourceOwner string
}

type UdfInfoOutput struct {
	JarName     string `json:"jarName"`
	Description string `json:"description"`
	UploadTime  string `json:"uploadTime"`
}

type ListUdfsOutput struct {
	Result []UdfInfoOutput `json:"result"`
}

type RegisterUdfFunctionInput struct {
	PandoraToken
	FuncName        string `json:"-"`
	JarName         string `json:"jarName"`
	ClassName       string `json:"className"`
	FuncDeclaration string `json:"funcDeclaration"`
	Description     string `json:"description"`
}

func (e *RegisterUdfFunctionInput) Validate() error {
	if (len(e.Description)) > MaxDescriptionLen {
		return reqerr.NewInvalidArgs("RegisterUdfFunctionInput", fmt.Sprintf("udf function description must not be larger than %d", MaxDescriptionLen)).WithComponent("pipleline")
	}
	if (len(e.FuncDeclaration)) > MaxDescriptionLen {
		return reqerr.NewInvalidArgs("RegisterUdfFunctionInput", fmt.Sprintf("udf function declaration must not be larger than %d", MaxDescriptionLen)).WithComponent("pipleline")
	}
	return nil
}

type DeregisterUdfFunctionInput struct {
	PandoraToken
	FuncName string
}

type ListUdfFunctionsInput struct {
	PandoraToken
	PageRequest
	ResourceOwner string
	JarNamesIn    []string
	FuncNamesIn   []string
}

type UdfFunctionInfoOutput struct {
	JarName         string `json:"jarName"`
	FuncName        string `json:"funcName"`
	ClassName       string `json:"className"`
	FuncDeclaration string `json:"funcDeclaration"`
	Description     string `json:"description"`
}

type ListUdfFunctionsOutput struct {
	Result []UdfFunctionInfoOutput `json:"result"`
}

type ListBuiltinUdfFunctionsInput struct {
	PandoraToken
	PageRequest
	Categories []string
}

type ListUdfBuiltinFunctionsOutput struct {
	Result []UdfBuiltinFunctionInfoOutput `json:"result"`
}

type UdfBuiltinFunctionInfoOutput struct {
	FuncName        string `json:"funcName"`
	Category        string `json:"category"`
	FuncDeclaration string `json:"funcDeclaration"`
	Description     string `json:"description"`
}

type NodeMetadata struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Node struct {
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Parents  []NodeMetadata `json:"parents,omitempty"`
	Children []NodeMetadata `json:"children,omitempty"`
	Data     interface{}    `json:"data"`
}

type CreateWorkflowInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	WorkflowName  string `json:"name"`
	Region        string `json:"region"`
	Comment       string `json:"comment,omitempty"`
}

type UpdateWorkflowInput struct {
	PandoraToken
	ResourceOwner string           `json:"-"`
	WorkflowName  string           `json:"name"`
	Region        string           `json:"region"`
	Nodes         map[string]*Node `json:"nodes"`
}

type DeleteWorkflowInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	WorkflowName  string `json:"name"`
}

func (r *DeleteWorkflowInput) Validate() (err error) {
	if err = validateWorkflowName(r.WorkflowName); err != nil {
		return
	}
	return
}

type GetWorkflowInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	WorkflowName  string `json:"name"`
}

func (r *GetWorkflowInput) Validate() (err error) {
	if err = validateWorkflowName(r.WorkflowName); err != nil {
		return
	}
	return
}

type GetWorkflowOutput struct {
	Name             string           `json:"name,omitempty"`
	Region           string           `json:"region"`
	Nodes            map[string]*Node `json:"nodes"`
	Comment          string           `json:"comment"`
	CreateTime       string           `json:"createTime"`
	UpdateTime       string           `json:"updateTime"`
	Status           string           `json:"status"`
	CanStart         bool             `json:"canStart"`
	IsManualWorkflow bool             `json:"isManualWorkflow"`
}

type GetWorkflowStatusInput GetWorkflowInput

func (r *GetWorkflowStatusInput) Validate() (err error) {
	if err = validateWorkflowName(r.WorkflowName); err != nil {
		return
	}
	return
}

type GetWorkflowStatusOutput struct {
	Name        string       `json:"name"`
	Region      string       `json:"region"`
	NodesStatus []NodeStatus `json:"nodes"`
	Status      string       `json:"status"`
}

type NodeStatus struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

type ListWorkflowInput struct {
	PandoraToken
	ResourceOwner string
}

func (c *ListWorkflowInput) Validate() error {
	return nil
}

type ListWorkflowOutput []GetWorkflowOutput

func validateWorkflow(name, region string, nodes map[string]*Node) (err error) {
	if err = validateWorkflowName(name); err != nil {
		return
	}
	if nodes != nil {
		for _, node := range nodes {
			if err = validateNodeName(node.Name); err != nil {
				return err
			}
		}
	}
	if region == "" {
		err = reqerr.NewInvalidArgs("Region", "region should not be empty").WithComponent("pipleline")
		return
	}
	return
}

func (r *CreateWorkflowInput) Validate() (err error) {
	if err = validateWorkflow(r.WorkflowName, r.Region, nil); err != nil {
		return
	}
	return
}

func (r *UpdateWorkflowInput) Validate() (err error) {
	if err = validateWorkflow(r.WorkflowName, r.Region, r.Nodes); err != nil {
		return
	}
	return
}

type StartWorkflowInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	WorkflowName  string `json:"name"`
}

func (r *StartWorkflowInput) Validate() (err error) {
	if err = validateWorkflowName(r.WorkflowName); err != nil {
		return
	}
	return
}

type StopWorkflowInput StartWorkflowInput

func (r *StopWorkflowInput) Validate() (err error) {
	if err = validateWorkflowName(r.WorkflowName); err != nil {
		return
	}
	return
}

type DagLogSearchInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	WorkflowName  string `json:"-"`
	Type          string `json:"type"`
	Name          string `json:"name"`
	Repo          string `json:"reponame"`
	Query         string `json:"query"`
	Size          int    `json:"size"`
	Region        string `json:"region"`
	StartTime     int64  `json:"startTime"`
	EndTime       int64  `json:"endTime"`
}

func (r *DagLogSearchInput) Validate() (err error) {
	return
}

type LogMessage struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
	RawLog    string `json:"log"`
}

type StatusMessage struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
}

type MetricMessage struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Timestamp  string `json:"timestamp"`
	ExportType string `json:"exporttype"`
	Total      int64  `json:"total"`
	Success    int64  `json:"success"`
	Failed     int64  `json:"failed"`
	Lag        int64  `json:"lag"`
}

type RecommendMessage struct {
	TimeStamp string `json:"timestamp"`
	Detail    string `json:"detail"`
}

type WorkflowSearchRet struct {
	Status    []StatusMessage    `json:"status"`
	Log       []LogMessage       `json:"log"`
	Recommend []RecommendMessage `json:"recommend"`
	Metric    []MetricMessage    `json:"metric"`
}

type CreateVariableInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Value         string `json:"value"`
	Format        string `json:"format"`
}

func (r *CreateVariableInput) Validate() (err error) {
	if r.Type == VariableTimeType && r.Format == "" {
		err = reqerr.NewInvalidArgs("format", "time variable's format should not be empty").WithComponent("pipleline")
		return
	}
	if err = validateVariableName(r.Name); err != nil {
		return
	}
	if err = validateVariableType(r.Type); err != nil {
		return
	}
	return
}

type UpdateVariableInput CreateVariableInput

func (r *UpdateVariableInput) Validate() (err error) {
	if r.Type == VariableTimeType && r.Format == "" {
		err = reqerr.NewInvalidArgs("format", "time variable's format should not be empty").WithComponent("pipleline")
		return
	}
	if err = validateVariableName(r.Name); err != nil {
		return
	}
	if err = validateVariableType(r.Type); err != nil {
		return
	}
	return
}

type DeleteVariableInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	Name          string `json:"name"`
}

func (r *DeleteVariableInput) Validate() (err error) {
	if err = validateVariableName(r.Name); err != nil {
		return
	}
	return
}

type GetVariableInput struct {
	PandoraToken
	ResourceOwner string `json:"-"`
	Name          string `json:"name"`
}

func (r *GetVariableInput) Validate() (err error) {
	if err = validateVariableName(r.Name); err != nil {
		return
	}
	return
}

type GetVariableOutput struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Value  string `json:"value"`
	Format string `json:"format"`
}

type ListVariablesInput struct {
	PandoraToken
	ResourceOwner string
}

type ListVariablesOutput struct {
	Variables []GetVariableOutput `json:"variables"`
}
