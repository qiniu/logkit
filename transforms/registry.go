package transforms

import (
	. "github.com/qiniu/logkit/utils/models"
)

const (
	TransformAt = "transform_at"

	TransformTypeString  = "string"
	TransformTypeLong    = "long"
	TransformTypeFloat   = "float"
	TransformTypeBoolean = "bool"
	TransformTypeByte    = "[]byte"
)

const (
	StageBeforeParser = "before_parser"
	StageAfterParser  = "after_parser"
)

//Transformer plugin做数据变换的接口
// 注意： transform的规则是，出错要把数据原样返回
type Transformer interface {
	Description() string
	SampleConfig() string
	ConfigOptions() []Option
	Type() string
	Transform([]Data) ([]Data, error)
	RawTransform([]string) ([]string, error)
	Stage() string
	Stats() StatsInfo
}

type ServerTansformer interface {
	ServerConfig() map[string]interface{}
}

// StatsTransformer 代表了一个带有统计功能的转换器
type StatsTransformer interface {
	SetStats(string) StatsInfo
}

// transformer 初始化方法接口,err 不为空表示初始化失败
type Initializer interface {
	Init() error
}

type Creator func() Transformer

var Transformers = map[string]Creator{}

func Add(name string, creator Creator) {
	Transformers[name] = creator
}

var (
	KeyStage = Option{
		KeyName:       "stage",
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{StageAfterParser, StageBeforeParser},
		Default:       StageAfterParser,
		DefaultNoUse:  false,
		Description:   "transform运行的阶段(parser前还是parser后)(stage)",
		ToolTip:       "transform在parser前或在parser后进行运行",
		Type:          TransformTypeString,
		Advance:       true,
	}
	KeyFieldName = Option{
		KeyName:      "key",
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "要进行Transform变化的键(key)",
		ToolTip:      "对该字段的值进行transform变换",
		Type:         TransformTypeString,
	}
	KeyFieldNew = Option{
		KeyName:      "new",
		ChooseOnly:   false,
		Default:      "",
		Placeholder:  "new_field_keyname",
		DefaultNoUse: false,
		Description:  "新的字段名(new)",
		CheckRegex:   CheckPatternKey,
		ToolTip:      "生成的字段名称，不改变原有的字段",
		Type:         TransformTypeString,
	}
	KeyFieldNewRequired = Option{
		KeyName:      "new",
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "new_field_keyname",
		DefaultNoUse: false,
		Description:  "新的字段名(new)",
		CheckRegex:   CheckPatternKey,
		ToolTip:      "生成的字段名称，不改变原有的字段",
		Type:         TransformTypeString,
	}
	KeyTimezoneoffset = Option{
		KeyName:    "offset",
		ChooseOnly: true,
		ChooseOptions: []interface{}{0, -1, -2, -3, -4,
			-5, -6, -7, -8, -9, -10, -11, -12,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12},
		Default:      0,
		DefaultNoUse: false,
		Description:  "时区偏移量(offset)",
		Advance:      true,
		CheckRegex:   "*",
		Type:         TransformTypeLong,
		ToolTip:      "如果key中带有时区信息，则以该时区作为offset的基础时区，否则以UTC时区为基础时区",
	}
	KeyKeepString = Option{
		KeyName:       "keep",
		Element:       Radio,
		ChooseOnly:    true,
		Default:       false,
		ChooseOptions: []interface{}{false, true},
		DefaultNoUse:  false,
		Description:   "值解析为字符串，不进行转换",
		Advance:       true,
		Type:          TransformTypeBoolean,
	}
	KeyOverride = Option{
		KeyName:       "override",
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{false, true},
		Default:       false,
		DefaultNoUse:  false,
		Description:   "要进行Transform变化的键已存在时，是否覆盖原有的值(override)",
		Type:          TransformTypeBoolean,
		Advance:       true,
	}
)

type TransformInfo struct {
	CurData Data
	Index   int
}

type TransformResult struct {
	Index    int
	CurData  Data
	CurDatas []Data
	Err      error
	ErrNum   int
}

type TransformResultSlice []TransformResult

func (slice TransformResultSlice) Len() int {
	return len(slice)
}

func (slice TransformResultSlice) Less(i, j int) bool {
	return slice[i].Index < slice[j].Index
}

func (slice TransformResultSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type RawTransformInfo struct {
	CurData string
	Index   int
}

type RawTransformResult struct {
	Index   int
	CurData string
	Err     error
	ErrNum  int
}

type RawTransformResultSlice []RawTransformResult

func (slice RawTransformResultSlice) Len() int {
	return len(slice)
}

func (slice RawTransformResultSlice) Less(i, j int) bool {
	return slice[i].Index < slice[j].Index
}

func (slice RawTransformResultSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
