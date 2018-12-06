package sql

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
func sortByJson(datas []readInfo) ([]readInfo, bool) {
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
