package transforms

import (
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
)

const (
	KeyType = "type"
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
	Transform([]sender.Data) ([]sender.Data, error)
	RawTransform([]string) ([]string, error)
	Stage() string
	Stats() utils.StatsInfo
}

type Creator func() Transformer

var Transformers = map[string]Creator{}

func Add(name string, creator Creator) {
	Transformers[name] = creator
}
