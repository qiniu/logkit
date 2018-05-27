package metric

const (
	ConfigTypeBool   = "bool"
	ConfigTypeArray  = "array"
	ConsifTypeString = "string"

	OptionString     = "options"
	AttributesString = "attributes"
	Timestamp        = "timestamp"
)

//Collector 收集metrics的接口
type Collector interface {
	Name() string
	Tags() []string
	Usages() string
	Config() map[string]interface{}
	Collect() ([]map[string]interface{}, error)
}

// 供外部插件实现
// SyncConfig 与 Config 对应 获取配置项后同步配置给插件
type ExtCollector interface {
	Collector
	SyncConfig(map[string]interface{}) error
}

type Creator func() Collector

var Collectors = map[string]Creator{}

func Add(name string, creator Creator) {
	Collectors[name] = creator
}
