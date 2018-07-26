package router

import (
	. "github.com/qiniu/logkit/utils/models"
)

func GetRouterOption() []Option {
	mTypeNames := make([]interface{}, len(MatchTypeRegistry))
	for name := range MatchTypeRegistry {
		mTypeNames = append(mTypeNames, name)
	}
	return []Option{
		{
			KeyName:      RouterKeyName,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "attr1",
			DefaultNoUse: true,
			Description:  "作为路由标准的字段名称(router_key_name)",
		},
		{
			KeyName:       RouterMatchType,
			ChooseOnly:    true,
			ChooseOptions: mTypeNames,
			Default:       "",
			DefaultNoUse:  true,
			Description:   "选择值匹配方式(router_match_type)",
		},
		{
			KeyName:      RouterDefaultIndex,
			ChooseOnly:   false,
			Default:      "0",
			Required:     true,
			DefaultNoUse: true,
			Description:  "默认选择的 sender (router_default_sender)",
		},
	}
}

func GetRouterMatchTypeUsage() KeyValueSlice {
	mTypeUsage := make(KeyValueSlice, 0)
	for name, mType := range MatchTypeRegistry {
		mTypeFunc := mType()
		mTypeUsage = append(mTypeUsage, KeyValue{
			Key:   name,
			Value: mTypeFunc.usage(),
		})
	}
	return mTypeUsage
}
