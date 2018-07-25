package transforms

import (
	. "github.com/qiniu/logkit/utils/models"
)

const (
	KeyType = "type"
)

const (
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

// DataReader 代表了一个可直接读取内存数据结构的读取器
type StatsTransformer interface {
	// ReadData 用于读取一条数据以及数据的实际读取字节
	SetStats(string) StatsInfo
}

// transformer 初始化方法接口,err 不为空表示初始化失败
type Initializer interface {
	Init() error
}

type Closer interface {
	Close() error
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
		Type:          TransformTypeString,
	}
	KeyFieldName = Option{
		KeyName:      "key",
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "要进行Transform变化的键(key)",
		Type:         TransformTypeString,
	}
	KeyFieldNew = Option{
		KeyName:      "new",
		ChooseOnly:   false,
		Default:      "",
		Placeholder:  "new_field_keyname",
		DefaultNoUse: false,
		Description:  "新的字段名(new)",
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
		CheckRegex:   "*",
		Type:         TransformTypeLong,
	}
)
