package transforms

import (
	"github.com/qiniu/logkit/utils"
)

func GetTransformerUsages() []utils.KeyValue {
	var ModeUsages []utils.KeyValue
	for _, v := range Transformers {
		cr := v()
		ModeUsages = append(ModeUsages, utils.KeyValue{
			Key:   cr.Type(),
			Value: cr.Description(),
		})
	}
	return ModeUsages
}

func GetTransformerOptions() map[string][]utils.Option {
	ModeKeyOptions := make(map[string][]utils.Option)
	for _, v := range Transformers {
		cr := v()
		ModeKeyOptions[cr.Type()] = cr.ConfigOptions()
	}
	return ModeKeyOptions
}
