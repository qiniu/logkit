package logdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const (
	schemaKeyPattern = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	repoNamePattern  = "^[a-z][a-z0-9_]{0,127}$"
	retentionPattern = "^(0|[1-9][0-9]*)d$"
	noRetentionRepo  = "-1"
)

const (
	minRetentionDay = 0
	maxRetentionDay = 1800
)

const (
	StandardAnalyzer   = "standard"
	SimpleAnalyzer     = "simple"
	WhitespaceAnalyzer = "whitespace"
	StopAnalyzer       = "stop"
	AnsjAnalyzer       = "index_ansj"
	DicAnajAnalyzer    = "dic_ansj"
	SearchAnsjAnalyzer = "search_ansj"
	ToAnsjAnalyzer     = "to_ansj"
	UserAnsjAnalyzer   = "user_ansj"
	KeyWordAnalyzer    = "keyword"
	PathAnalyzer       = "path"
)

const (
	TypeString   = "string"
	TypeLong     = "long"
	TypeFloat    = "float"
	TypeBoolean  = "boolean"
	TypeDate     = "date"
	TypeGeoPoint = "geo_point"
	TypeIP       = "ip"
	TypeObject   = "object"
)

var schemaTypes = map[string]bool{
	TypeString:   true,
	TypeLong:     true,
	TypeFloat:    true,
	TypeBoolean:  true,
	TypeDate:     true,
	TypeIP:       true,
	TypeGeoPoint: true,
	TypeObject:   true}

var Analyzers = map[string]bool{
	StandardAnalyzer:   true,
	SimpleAnalyzer:     true,
	WhitespaceAnalyzer: true,
	StopAnalyzer:       true,
	AnsjAnalyzer:       true,
	DicAnajAnalyzer:    true,
	SearchAnsjAnalyzer: true,
	ToAnsjAnalyzer:     true,
	UserAnsjAnalyzer:   true,
	KeyWordAnalyzer:    true,
	PathAnalyzer:       true,
}

func validateRepoName(r string) error {
	matched, err := regexp.MatchString(repoNamePattern, r)
	if err != nil {
		return reqerr.NewInvalidArgs("RepoName", err.Error()).WithComponent("tsdb")
	}
	if !matched {
		return reqerr.NewInvalidArgs("RepoName", fmt.Sprintf("invalid repo name: %s", r)).WithComponent("tsdb")
	}
	return nil
}

type RepoSchemaEntry struct {
	Key       string                 `json:"key"`
	ValueType string                 `json:"valtype"`
	Analyzer  string                 `json:"analyzer,omitempty"` //替代SearchWay
	Primary   bool                   `json:"primary,omitempty"`  //默认值是false
	Schemas   []RepoSchemaEntry      `json:"nested,omitempty"`
	Options   map[string]interface{} `json:"options,omitempty"` // 对于一些特殊的类型，比如IP或者geo_point会有一些特殊的属性。
}

func (e RepoSchemaEntry) String() string {
	bytes, _ := json.Marshal(e)
	return string(bytes)
}

func (e *RepoSchemaEntry) Validate() (err error) {
	matched, err := regexp.MatchString(schemaKeyPattern, e.Key)
	if err != nil {
		return reqerr.NewInvalidArgs("Schema", err.Error())
	}
	if !matched {
		return reqerr.NewInvalidArgs("Schema", fmt.Sprintf("invalid field key: %s", e.Key))
	}
	if !schemaTypes[e.ValueType] {
		return reqerr.NewInvalidArgs("Schema", fmt.Sprintf("invalid field type: %s, invalid field type should be one of \"float\", \"string\", \"date\", \"object\" , \"boolean\", \"ip\", \"geo_point\"  and \"long\"", e.ValueType))
	}

	//remove check first by @wonderflow
	/*
		if e.Analyzer != "" {
			if e.ValueType != "string" {
				return reqerr.NewInvalidArgs("Schema", fmt.Sprintf("only string valueType support searchWay, but now is %v", e.ValueType))
			}
			if !Analyzers[e.Analyzer] {
				return reqerr.NewInvalidArgs("Schema", fmt.Sprintf("invalid Analyzer type: %s", e.SearchWay))
			}
		}
	*/

	return
}

type CreateRepoDSLInput struct {
	PandoraToken
	RepoName  string
	Region    string `json:"region"`
	Retention string `json:"retention"`
	DSL       string `json:"dsl"`
}

/*
DSL创建的规则为`<字段名称> <类型> <分词方式>`,字段名称和类型用空格符隔开，不同字段用逗号隔开。若字段为主键，则在类型前加`*`号表示，主键只能有一个。
    * pandora date类型：`date`,`DATE`,`d`,`D`
    * pandora long类型：`long`,`LONG`,`l`,`L`
    * pandora float类型: `float`,`FLOAT`,`F`,`f`
    * pandora string类型: `string`,`STRING`,`S`,`s`
    * pandora bool类型:  `bool`,`BOOL`,`B`,`b`,`boolean`
    * pandora array类型: `array`,`ARRAY`,`A`,`a`;括号中跟具体array元素的类型，如`a(l)`或者直接写`(l)`，表示array里面都是long。
    * pandora map（object）类型: `map`,`MAP`,`M`,`m`,`object`,`o`;使用花括号表示具体类型，表达map里面的元素，如map{a l,b map{c b,x s}}, 表示map结构体里包含a字段，类型是long，b字段又是一个map，里面包含c字段，类型是bool，还包含x字段，类型是string。
*/

func getRawType(tp string) (schemaType string, err error) {
	schemaType = strings.ToLower(tp)
	switch schemaType {
	case "l", "long":
		schemaType = TypeLong
	case "f", "float":
		schemaType = TypeFloat
	case "s", "string":
		schemaType = TypeString
	case "d", "date":
		schemaType = TypeDate
	case "a", "array":
		err = errors.New("arrary type must specify data type surrounded by ( )")
		return
	case "m", "map", "o", "object": //兼容object
		schemaType = TypeObject
	case "b", "bool", "boolean":
		schemaType = TypeBoolean
	case "": //这个是一种缺省
	default:
		err = fmt.Errorf("schema type %v not supperted", schemaType)
		return
	}
	return
}

func getField(f string) (key, valueType, analyzer string, primary bool, err error) {
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
	case 3:
		key, valueType, analyzer = splits[0], splits[1], splits[2]
	default:
		err = fmt.Errorf("Raw field schema parse error: <%v> was invalid", f)
		return
	}
	if key == "" {
		err = fmt.Errorf("field schema %v key can not be empty", f)
		return
	}
	if analyzer != "" {
		if _, ok := Analyzers[analyzer]; !ok {
			err = fmt.Errorf("field schema %v key define uknown analyzer %v", f, analyzer)
			return
		}
	}
	primary = false
	if strings.HasPrefix(valueType, "*") || strings.HasSuffix(valueType, "*") {
		primary = true
		valueType = strings.Trim(valueType, "*")
	}
	//处理arrary类型
	if beg := strings.Index(valueType, "("); beg != -1 {
		ed := strings.Index(valueType, ")")
		if ed <= beg {
			err = fmt.Errorf("field schema %v has no type specified", f)
			return
		}
		valueType, err = getRawType(valueType[beg+1 : ed])
		if err != nil {
			err = fmt.Errorf("array 【%v】: %v, key %v valuetype %v", f, err, key, valueType)
		}
		//valueType = "array"  目前logdb这边array和普通元素一样表达，都写元素类型，写点的时候直接写入，不需要专门表示array类型。
		return
	}
	valueType, err = getRawType(valueType)
	if err != nil {
		err = fmt.Errorf("normal 【%v】: %v, key %v valuetype %v", f, err, key, valueType)
	}
	return
}

func getRepoEntry(key, valueType, analyzer string, primary bool, subschemas []RepoSchemaEntry) RepoSchemaEntry {
	entry := RepoSchemaEntry{
		Key:       key,
		ValueType: valueType,
		Primary:   primary,
	}
	if analyzer != "" && valueType == TypeString {
		entry.Analyzer = analyzer
	}
	if subschemas != nil && valueType == TypeObject {
		entry.Schemas = subschemas
	}
	return entry
}

func checkPrimary(hasPrimary, key, valueType string, depth int) error {
	if depth > 1 || valueType != "string" {
		return errors.New("primary key 只能在最外层指定 且valueType只能是string")
	}
	if hasPrimary != "" {
		return fmt.Errorf("you have defined more than one primary key: %v or %v ?", hasPrimary, key)
	}
	return nil
}

// DSLtoSchema 是将DSL字符串转化为schema
func DSLtoSchema(dsl string) (schemas []RepoSchemaEntry, err error) {
	return toSchema(dsl, 0)
}

// SchemaToDSL 会把schema信息转换为格式化的dsl字符串，其中indent是格式化的占位符，一般是"\t"或者2个空格。
func SchemaToDSL(schemas []RepoSchemaEntry, indent string) (dsl string) {
	return getFormatDSL(schemas, 0, indent)
}

func toSchema(dsl string, depth int) (schemas []RepoSchemaEntry, err error) {
	if depth > base.NestLimit {
		err = reqerr.NewInvalidArgs("Schema", fmt.Sprintf("RepoSchemaEntry are nested out of limit %v", base.NestLimit))
		return
	}
	schemas = make([]RepoSchemaEntry, 0)
	dsl = strings.TrimSpace(dsl)
	start := 0
	nestbalance := 0
	neststart, nestend := -1, -1
	dsl += "," //增加一个','保证一定是以","为终结
	var hasPrimary string
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
				key, valueType, analyzer, primary, err := getField(dsl[start:neststart])
				if err != nil {
					return nil, err
				}
				if key != "" {
					if _, ok := dupcheck[key]; ok {
						return nil, errors.New("parse dsl error: " + key + " is duplicated key")
					}
					dupcheck[key] = struct{}{}
					if valueType == "" {
						valueType = TypeObject
					}
					if primary {
						if perr := checkPrimary(hasPrimary, key, valueType, depth); perr != nil {
							return nil, perr
						}
						hasPrimary = key
					}
					schemas = append(schemas, getRepoEntry(key, valueType, analyzer, primary, subschemas))
				}
				start = end + 1
			}
		case ',', '\n':
			if nestbalance == 0 {
				if start < end {
					key, valueType, analyzer, primary, err := getField(strings.TrimSpace(dsl[start:end]))
					if err != nil {
						return nil, err
					}

					if key != "" {
						if _, ok := dupcheck[key]; ok {
							return nil, errors.New("parse dsl error: " + key + " is duplicated key")
						}
						dupcheck[key] = struct{}{}
						if valueType == "" {
							valueType = TypeString
						}
						if primary {
							if perr := checkPrimary(hasPrimary, key, valueType, depth); perr != nil {
								return nil, perr
							}
							hasPrimary = key
						}
						schemas = append(schemas, getRepoEntry(key, valueType, analyzer, primary, nil))
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
		if v.Primary {
			dsl += "*"
		}
		dsl += v.ValueType
		switch v.ValueType {
		case TypeObject:
			dsl += "{\n"
			dsl += getFormatDSL(v.Schemas, depth+1, indent)
			dsl += getDepthIndent(depth, indent) + "}"
		case TypeString:
			dsl += " " + v.Analyzer
		default:
		}
		dsl += "\n"
	}
	return
}

//FullText 用于创建和指定全文索引
type FullText struct {
	Enabled  bool   `json:"enabled"`
	Analyzer string `json:"analyzer"`
}

//NewFullText  增加全文索引的选项
func NewFullText(analyzer string) FullText {
	return FullText{
		true,
		analyzer,
	}
}

type CreateRepoInput struct {
	PandoraToken
	RepoName     string
	Region       string            `json:"region"`
	Retention    string            `json:"retention"`
	Schema       []RepoSchemaEntry `json:"schema"`
	PrimaryField string            `json:"primaryField"`
	FullText     FullText          `json:"fullText"`
}

func (r *CreateRepoInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}

	if r.Schema == nil || len(r.Schema) == 0 {
		return reqerr.NewInvalidArgs("Schema", "schema should not be empty")
	}
	for _, schema := range r.Schema {
		if err = schema.Validate(); err != nil {
			return
		}
	}

	return checkRetention(r.Retention)
}

func checkRetention(retention string) error {
	if retention == noRetentionRepo {
		return nil
	}
	matched, err := regexp.MatchString(retentionPattern, retention)
	if err != nil {
		return reqerr.NewInvalidArgs("Retention", "parse retention time failed")
	}
	if !matched {
		return reqerr.NewInvalidArgs("Retention", "invalid retention time format")
	}
	retentionInt, err := strconv.Atoi(strings.Replace(retention, "d", "", -1))
	if err != nil {
		return reqerr.NewInvalidArgs("Retention", "invalid retention time format")
	}

	if retentionInt > maxRetentionDay || retentionInt < minRetentionDay {
		return reqerr.NewInvalidArgs("Retention", "invalid retention range")
	}
	return nil
}

type UpdateRepoInput struct {
	PandoraToken
	RepoName  string
	Retention string            `json:"retention"`
	Schema    []RepoSchemaEntry `json:"schema"`
}

func (r *UpdateRepoInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}
	if r.Schema == nil || len(r.Schema) == 0 {
		err = reqerr.NewInvalidArgs("Schema", "schema should not be empty")
		return
	}
	for _, schema := range r.Schema {
		if err = schema.Validate(); err != nil {
			return
		}
	}
	return checkRetention(r.Retention)
}

type GetRepoInput struct {
	PandoraToken
	RepoName string
}

type GetRepoOutput struct {
	Region       string            `json:"region"`
	Retention    string            `json:"retention"`
	Schema       []RepoSchemaEntry `json:"schema"`
	PrimaryField string            `json:"primaryField"`
	CreateTime   string            `json:"createTime"`
	UpdateTime   string            `json:"updateTime"`
	FullText     FullText          `json:"fullText"`
}

type RepoDesc struct {
	RepoName     string   `json:"name"`
	Region       string   `json:"region"`
	PrimaryField string   `json:"primaryField"`
	Retention    string   `json:"retention"`
	CreateTime   string   `json:"createTime"`
	UpdateTime   string   `json:"updateTime"`
	FullText     FullText `json:"fullText"`
}

type ListReposInput struct {
	PandoraToken
}

type ListReposOutput struct {
	Repos []RepoDesc `json:"repos"`
}

type DeleteRepoInput struct {
	PandoraToken
	RepoName string
}

type Log map[string]interface{}

type Logs []Log

func (ls Logs) Buf() (buf []byte, err error) {
	buf, err = json.Marshal(ls)
	if err != nil {
		return
	}
	return
}

type SendLogInput struct {
	PandoraToken
	RepoName       string `json:"-"`
	OmitInvalidLog bool   `json:"-"`
	Logs           Logs
}

type SendLogOutput struct {
	Success int `json:"success"`
	Failed  int `json:"failed"`
	Total   int `json:"total"`
}

type SchemaRefInput struct {
	PandoraToken
	SampleData map[string]interface{} `json:"sample_data"`
}

func (srf SchemaRefInput) Buf() (buf []byte, err error) {
	buf, err = json.Marshal(srf.SampleData)
	if err != nil {
		return
	}
	return
}

type SchemaRefOut []RepoSchemaEntry

type Highlight struct {
	PreTags           []string               `json:"pre_tags"`
	PostTags          []string               `json:"post_tags"`
	Fields            map[string]interface{} `json:"fields"`
	RequireFieldMatch bool                   `json:"require_field_match"`
	FragmentSize      int                    `json:"fragment_size"`
}

func (h *Highlight) Validate() error {
	return nil
}

type QueryLogInput struct {
	PandoraToken
	RepoName  string
	Query     string
	Sort      string
	From      int
	Size      int
	Scroll    string
	Highlight *Highlight
}

type QueryScrollInput struct {
	PandoraToken
	RepoName string `json:"-"`
	ScrollId string `json:"scroll_id"`
	Scroll   string `json:"scroll"`
}

func (input *QueryScrollInput) Buf() (buf []byte, err error) {
	buf, err = json.Marshal(input)
	if err != nil {
		return
	}
	return
}

type QueryLogOutput struct {
	ScrollId       string                   `json:"scroll_id,omitempty"`
	Total          int                      `json:"total"`
	PartialSuccess bool                     `json:"partialSuccess"`
	Data           []map[string]interface{} `json:"data"`
}

type QueryHistogramLogInput struct {
	PandoraToken
	RepoName string
	Query    string
	Field    string
	From     int64
	To       int64
}

type LogHistogramDesc struct {
	Key   int64 `json:"key"`
	Count int64 `json:"count"`
}

type QueryHistogramLogOutput struct {
	Total          int                `json:"total"`
	PartialSuccess bool               `json:"partialSuccess"`
	Buckets        []LogHistogramDesc `json:"buckets"`
}

type PutRepoConfigInput struct {
	PandoraToken
	RepoName      string
	TimeFieldName string `json:"timeFieldName"`
}

func (r *PutRepoConfigInput) Validate() (err error) {
	return nil
}

type GetRepoConfigInput struct {
	PandoraToken
	RepoName string
}

type GetRepoConfigOutput struct {
	TimeFieldName string `json:"timeFieldName"`
}

type PartialQueryInput struct {
	PandoraToken
	RepoName  string `json:"-"`
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Highlight struct {
		PostTag string `json:"post_tag"`
		PreTag  string `json:"pre_tag"`
	} `json:"highlight"`
	QueryString string `json:"query_string"`
	SearchType  int    `json:"searchType"`
	Size        int    `json:"size"`
	Sort        string `json:"sort"`
}

const (
	PartialQuerySearchTypeA int = iota //混合模式
	PartialQuerySearchTypeQ            //searching模式
	PartialQuerySearchTypeH            //直方图模式
)

func (partialQueryInput *PartialQueryInput) Buf() (buf []byte, err error) {
	buf, err = json.Marshal(partialQueryInput)
	if err != nil {
		return
	}
	return
}

type PartialQueryOutput struct {
	Buckets []struct {
		Count int `json:"count"`
		Key   int `json:"key"`
	} `json:"buckets"`
	Hits           []map[string]interface{} `json:"hits"`
	PartialSuccess bool                     `json:"partialSuccess"`
	Process        float64                  `json:"process"`
	Took           int                      `json:"took"`
	Total          int                      `json:"total"`
}
