package common

import (
	. "github.com/qiniu/logkit/utils/models"
)

func ConvertDatas(ins []map[string]interface{}) []Data {
	var datas []Data
	for _, v := range ins {
		datas = append(datas, Data(v))
	}
	return datas
}
func ConvertDatasBack(ins []Data) []map[string]interface{} {
	var datas []map[string]interface{}
	for _, v := range ins {
		datas = append(datas, map[string]interface{}(v))
	}
	return datas
}
