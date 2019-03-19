package mysql

import (
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
