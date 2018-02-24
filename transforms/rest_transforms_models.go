package transforms

import (
	. "github.com/qiniu/logkit/utils/models"
)

func GetTransformerUsages() []KeyValue {
	var ModeUsages []KeyValue
	for _, v := range Transformers {
		cr := v()
		ModeUsages = append(ModeUsages, KeyValue{
			Key:   cr.Type(),
			Value: cr.Description(),
		})
	}
	return ModeUsages
}

func GetTransformerOptions() map[string][]Option {
	ModeKeyOptions := make(map[string][]Option)
	for _, v := range Transformers {
		cr := v()
		ModeKeyOptions[cr.Type()] = cr.ConfigOptions()
	}
	return ModeKeyOptions
}
