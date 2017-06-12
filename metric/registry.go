package metric

//Collector 收集metrics的接口
type Collector interface {
	Name() string
	Collect() map[string]interface{}
}

type Creator func() Collector

var Collectors = map[string]Creator{}

func Add(name string, creator Creator) {
	Collectors[name] = creator
}
