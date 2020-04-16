package mysql

import (
	"reflect"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/reader/sql"
)

func (r *MysqlReader) updateStartTime(offsetKeyIndex int, scanArgs []interface{}) bool {
	if r.timestampKeyInt {
		timeOffset, ok := GetTimeIntFromArgs(offsetKeyIndex, scanArgs)
		if ok && timeOffset > r.startTimeInt {
			r.startTimeInt = timeOffset
			r.timestampMux.Lock()
			r.timeCacheMap = nil
			r.timestampMux.Unlock()
			return true
		}
		return false
	}
	timeData, ok := GetTimeFromArgs(offsetKeyIndex, scanArgs)
	if ok && timeData.After(r.startTime) {
		r.startTime = timeData
		r.timestampMux.Lock()
		r.timeCacheMap = nil
		r.timestampMux.Unlock()
		return true
	}
	return false
}

func GetTimeStrFromArgs(offsetKeyIndex int, scanArgs []interface{}) (string, bool) {
	if offsetKeyIndex < 0 || offsetKeyIndex > len(scanArgs) {
		return "", false
	}
	v := scanArgs[offsetKeyIndex]
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		log.Error("scanArgs not a pointer")
		return "", false
	}
	if dpv.IsNil() {
		log.Error("scanArgs is a nil pointer")
		return "", false
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Interface:
		data := dv.Interface()
		switch data.(type) {
		case []byte:
			ret, _ := data.([]byte)
			return string(ret), true
		default:
			log.Errorf("updateStartTimeStr failed as %v(%T) is not []byte", data, data)
		}
	default:
		log.Errorf("updateStartTimeStr is not Interface but %v", dv.Kind())
	}
	return "", false
}

//用于更新时间戳，已经同样时间戳上那个数据点
func (r *MysqlReader) updateTimeCntFromData(v ReadInfo) {
	if r.timestampKeyInt {
		timeData, ok := GetTimeIntFromData(v.Data, r.timestampKey)
		if !ok {
			return
		}
		if timeData > r.startTimeInt {
			r.startTimeInt = timeData
			r.timestampMux.Lock()
			r.timeCacheMap = map[string]string{v.Json: "1"}
			r.timestampMux.Unlock()
		} else if timeData == r.startTimeInt {
			r.timestampMux.Lock()
			if r.timeCacheMap == nil {
				r.timeCacheMap = make(map[string]string)
			}
			r.timeCacheMap[v.Json] = "1"
			r.timestampMux.Unlock()
		}
		return
	}

	timeData, ok := GetTimeFromData(v.Data, r.timestampKey)
	if !ok {
		return
	}
	if timeData.After(r.startTime) {
		r.startTime = timeData
		r.timestampMux.Lock()
		r.timeCacheMap = map[string]string{v.Json: "1"}
		r.timestampMux.Unlock()
	} else if timeData.Equal(r.startTime) {
		r.timestampMux.Lock()
		if r.timeCacheMap == nil {
			r.timeCacheMap = make(map[string]string)
		}
		r.timeCacheMap[v.Json] = "1"
		r.timestampMux.Unlock()
	}
	return
}

func (r *MysqlReader) trimExistData(datas []ReadInfo) []ReadInfo {
	if len(r.timestampKey) <= 0 || len(datas) < 1 {
		return datas
	}
	datas, success := GetJson(datas)
	if !success {
		return datas
	}
	newdatas := make([]ReadInfo, 0, len(datas))
	for _, v := range datas {
		var compare int
		var exist bool
		if r.timestampKeyInt {
			compare, exist = CompareWithStartTimeInt(v.Data, r.timestampKey, r.startTimeInt)
		} else {
			compare, exist = CompareWithStartTime(v.Data, r.timestampKey, r.startTime)
		}
		if !exist {
			//如果出现了数据中没有时间的，实际上已经不合法了，那就获取
			newdatas = append(newdatas, v)
			continue
		}
		if compare == 1 {
			//找到的数据都是比当前时间还要新的，选取
			newdatas = append(newdatas, v)
			continue
		}
		if compare == 0 {
			r.timestampMux.RLock()
			//判断map去重
			if _, ok := r.timeCacheMap[v.Json]; !ok {
				newdatas = append(newdatas, v)
			}
			r.timestampMux.RUnlock()
		}
	}
	return newdatas
}
