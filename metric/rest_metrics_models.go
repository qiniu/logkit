package metric

import (
	"sort"

	. "github.com/qiniu/logkit/utils/models"
)

func GetMetricTypeKey() map[string][]KeyValue {
	typeKey := make(map[string][]KeyValue)
	for key, collector := range Collectors {
		coll := collector()
		config := coll.Config()
		if attributes, ex := config[AttributesString]; ex {
			if attr, ok := attributes.([]KeyValue); ok {
				typeKey[key] = attr
			}
		}
	}
	return typeKey
}

func GetMetricUsages() []Option {
	metricOptions := make([]Option, 0)
	for key, collector := range Collectors {
		option := Option{
			KeyName:       key,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   collector().Usages(),
		}
		metricOptions = append(metricOptions, option)
	}
	// 使传递到前端的数组有序
	sort.Slice(metricOptions, func(i, j int) bool {
		return metricOptions[i].KeyName < metricOptions[j].KeyName
	})
	return metricOptions
}

func GetMetricOptions() map[string][]Option {
	metricOptions := make(map[string][]Option)
	for key, collector := range Collectors {
		coll := collector()
		config := coll.Config()
		if option, ex := config[OptionString]; ex {
			if opt, ok := option.([]Option); ok {
				metricOptions[key] = opt
			}
		}
	}
	return metricOptions
}

func GetMetricTags() map[string][]string {
	metricTags := make(map[string][]string)
	for key, collector := range Collectors {
		coll := collector()
		metricTags[key] = coll.Tags()
	}
	return metricTags
}
