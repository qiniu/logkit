package metric

import (
	"sort"

	. "github.com/qiniu/logkit/utils/models"
)

func GetMetricTypeKey() map[string]interface{} {
	typeKey := make(map[string]interface{})
	for key, collector := range Collectors {
		coll := collector()
		config := coll.Config()
		if attributes, ex := config[AttributesString]; ex {
			typeKey[key] = attributes
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

func GetMetricOptions() map[string]interface{} {
	metricOptions := make(map[string]interface{})
	for key, collector := range Collectors {
		coll := collector()
		config := coll.Config()
		if option, ex := config[OptionString]; ex {
			metricOptions[key] = option
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
