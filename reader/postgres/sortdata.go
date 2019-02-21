package postgres

import (
	"sort"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
)

var json = jsoniter.Config{SortMapKeys: true}.Froze()

type jsonIndex struct {
	Json string
	Idx  int
}

type ByJson []jsonIndex

func (a ByJson) Len() int           { return len(a) }
func (a ByJson) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByJson) Less(i, j int) bool { return a[i].Json < a[j].Json }

//if Marshal failed return all data
func SortByJson(datas []readInfo) ([]readInfo, bool) {
	if len(datas) < 1 {
		return datas, true
	}
	byjson := make(ByJson, len(datas))
	for idx, v := range datas {
		jst, err := json.Marshal(v.data)
		if err != nil {
			log.Error("can't marshal json for sort", err)
			return datas, true
		}
		byjson[idx] = jsonIndex{
			Idx:  idx,
			Json: string(jst),
		}
		datas[idx].json = string(jst)
	}

	sort.Sort(byjson)
	newdata := make([]readInfo, len(datas))
	for idx, v := range byjson {
		newdata[idx] = datas[v.Idx]
	}
	return newdata, false
}

//GetJson return true, if bejson success
func GetJson(datas []readInfo) ([]readInfo, bool) {
	if len(datas) < 1 {
		return datas, false
	}
	for idx, v := range datas {
		jst, err := json.Marshal(v.data)
		if err != nil {
			log.Error("can't marshal json for sort", err)
			return datas, false
		}
		datas[idx].json = string(jst)
	}
	return datas, true
}
