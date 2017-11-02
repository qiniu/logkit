package metric

import "github.com/qiniu/logkit/utils"

func GetMetricTypeKey() map[string][]utils.KeyValue {
	typeKey := map[string][]utils.KeyValue{}
	for key, collector := range Collectors {
		typeKey[key] = collector().Attributes()
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
	metricOptions := map[string][]utils.Option{}
	for key, collector := range Collectors {
		option := collector().Config()
		metricOptions[key] = option
	}
	return metricOptions
}
