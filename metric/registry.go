package metric

const (
	ConfigTypeBool   = "bool"
	ConfigTypeArray  = "array"
	ConfigTypeString = "string"

	OptionString     = "options"
	AttributesString = "attributes"
	Timestamp        = "timestamp"
)

//Collector 收集metrics的接口
type Collector interface {
	// Name returns the name.
	Name() string
	// Tags returns available tags.
	Tags() []string
	// Usages returns usage information.
	Usages() string
	// Config returns config fields and options.
	Config() map[string]interface{}
	// Collect gathers metric infomration.
	Collect() ([]map[string]interface{}, error)
}

// 供外部插件实现
// SyncConfig 与 Config 对应 获取配置项后同步配置给插件
type ExtCollector interface {
	Collector
	// SyncConfig updates config options specifically for external metrics.
	SyncConfig(map[string]interface{}) error
}

type Creator func() Collector

var Collectors = map[string]Creator{}

func Add(name string, creator Creator) {
	Collectors[name] = creator
}
