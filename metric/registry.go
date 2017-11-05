package metric

import "github.com/qiniu/logkit/utils"

const (
	ConfigTypeBool   = "bool"
	ConfigTypeArray  = "array"
	ConsifTypeString = "string"
)

//Collector 收集metrics的接口
type Collector interface {
	Name() string
	Usages() string
	Config() []utils.Option
	Attributes() []utils.KeyValue
	Collect() ([]map[string]interface{}, error)
}

type Creator func() Collector

var Collectors = map[string]Creator{}

func Add(name string, creator Creator) {
	Collectors[name] = creator
}
