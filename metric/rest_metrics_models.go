package metric

import "github.com/qiniu/logkit/utils"

func GetMetricTypeKey() map[string][]utils.KeyValue {
	typeKey := make(map[string][]utils.KeyValue)
	for key, collector := range Collectors {
		coll := collector()
		config := coll.Config()
		if attributes, ex := config[AttributesString]; ex {
			if attr, ok := attributes.([]utils.KeyValue); ok {
				typeKey[key] = attr
			}
		}
	}
	return typeKey
}

func GetMetricUsages() []utils.Option {
	metricOptions := make([]utils.Option, 0)
	for key, collector := range Collectors {
		option := utils.Option{
			KeyName:       key,
			ChooseOnly:    true,
			ChooseOptions: []string{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   collector().Usages(),
		}
		metricOptions = append(metricOptions, option)
	}
	return metricOptions
}

func GetMetricOptions() map[string][]utils.Option {
	metricOptions := make(map[string][]utils.Option)
	for key, collector := range Collectors {
		coll := collector()
		config := coll.Config()
		if option, ex := config[OptionString]; ex {
			if opt, ok := option.([]utils.Option); ok {
				metricOptions[key] = opt
			}
		}
	}
	return metricOptions
}
