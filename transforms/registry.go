package transforms

import "github.com/qiniu/logkit/sender"

//Transformer plugin做数据变换的接口
type Transformer interface {
	Description() string
	SampleConfig() string
	Transform([]sender.Data) ([]sender.Data, error)
}

type Creator func() Transformer

var Transformers = map[string]Creator{}

func Add(name string, creator Creator) {
	Transformers[name] = creator
}
