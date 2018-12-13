package sql

import (
	"reflect"
	"time"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/utils/models"
)

func (r *Reader) getTimeFromData(data Data) (time.Time, bool) {
	if len(r.timestampKey) <= 0 {
		return time.Time{}, false
	}
	dt, ok := data[r.timestampKey]
	if !ok {
		return time.Time{}, false
	}
	tm, ok := dt.(time.Time)
	return tm, ok
}

func (r *Reader) getTimeIntFromData(data Data) (int64, bool) {
	if len(r.timestampKey) <= 0 {
		return 0, false
	}
	dt, ok := data[r.timestampKey]
	if !ok {
		return 0, false
	}
	tm, ok := dt.(int64)
	return tm, ok
}

func (r *Reader) getTimeFromArgs(offsetKeyIndex int, scanArgs []interface{}) (time.Time, bool) {
	if offsetKeyIndex < 0 || offsetKeyIndex > len(scanArgs) {
		return time.Time{}, false
	}
	v := scanArgs[offsetKeyIndex]
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		log.Error("scanArgs not a pointer")
		return time.Time{}, false
	}
	if dpv.IsNil() {
		log.Error("scanArgs is a nil pointer")
		return time.Time{}, false
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Interface:
		data := dv.Interface()
		switch timeData := data.(type) {
		case time.Time:
			return timeData, true
		case *time.Time:
			return *timeData, true
		default:
			log.Errorf("updateStartTime failed as %v(%T) is not time.Time", data, data)
		}
	default:
		log.Errorf("updateStartTime is not Interface but %v", dv.Kind())
	}
	return time.Time{}, false
}

func (r *Reader) getTimeIntFromArgs(offsetKeyIndex int, scanArgs []interface{}) (int64, bool) {
	if offsetKeyIndex < 0 || offsetKeyIndex > len(scanArgs) {
		return 0, false
	}
	timeOffset, err := convertLong(scanArgs[offsetKeyIndex])
	if err != nil {
		log.Error("getTimeIntFromArgs err ", err)
		return 0, false
	}
	return timeOffset, true
}

func (r *Reader) updateStartTime(offsetKeyIndex int, scanArgs []interface{}) bool {
	if r.timestampKeyInt {
		timeOffset, ok := r.getTimeIntFromArgs(offsetKeyIndex, scanArgs)
		if ok && timeOffset > r.startTimeInt {
			r.startTimeInt = timeOffset
			r.timestampmux.Lock()
			r.trimecachemap = nil
			r.timestampmux.Unlock()
			return true
		}
		return false
	}
	timeData, ok := r.getTimeFromArgs(offsetKeyIndex, scanArgs)
	if ok && timeData.After(r.startTime) {
		r.startTime = timeData
		r.timestampmux.Lock()
		r.trimecachemap = nil
		r.timestampmux.Unlock()
		return true
	}
	return false
}

//用于更新时间戳，已经同样时间戳上那个数据点
func (r *Reader) updateTimeCntFromData(v readInfo) {
	if r.timestampKeyInt {
		timeData, ok := r.getTimeIntFromData(v.data)
		if !ok {
			return
		}
		if timeData > r.startTimeInt {
			r.startTimeInt = timeData
			r.timestampmux.Lock()
			r.trimecachemap = map[string]string{v.json: "1"}
			r.timestampmux.Unlock()
		} else if timeData == r.startTimeInt {
			r.timestampmux.Lock()
			if r.trimecachemap == nil {
				r.trimecachemap = make(map[string]string)
			}
			r.trimecachemap[v.json] = "1"
			r.timestampmux.Unlock()
		}
		return
	}
	timeData, ok := r.getTimeFromData(v.data)
	if !ok {
		return
	}
	if timeData.After(r.startTime) {
		r.startTime = timeData
		r.timestampmux.Lock()
		r.trimecachemap = map[string]string{v.json: "1"}
		r.timestampmux.Unlock()
	} else if timeData.Equal(r.startTime) {
		r.timestampmux.Lock()
		if r.trimecachemap == nil {
			r.trimecachemap = make(map[string]string)
		}
		r.trimecachemap[v.json] = "1"
		r.timestampmux.Unlock()
	}
}

//-1 代表不存在; 1 代表更大; 0 代表相等
func (r *Reader) compareWithStartTime(data Data) (int, bool) {
	timeData, ok := r.getTimeFromData(data)
	if !ok {
		//如果出现了数据中没有时间的，实际上已经不合法了，那就获取，宁愿重复不愿遗漏
		return 1, false
	}
	if timeData.After(r.startTime) {
		return 1, true
	}
	return 0, true
}

//-1 代表不存在; 1 代表更大; 0 代表相等
func (r *Reader) compareWithStartTimeInt(data Data) (int, bool) {
	timeData, ok := r.getTimeIntFromData(data)
	if !ok {
		//如果出现了数据中没有时间的，实际上已经不合法了，那就获取，宁愿重复不愿遗漏
		return 1, false
	}
	if timeData > r.startTimeInt {
		return 1, true
	}
	return 0, true
}

func (r *Reader) trimeExistData(datas []readInfo) []readInfo {
	if len(r.timestampKey) <= 0 || len(datas) < 1 {
		return datas
	}
	datas, success := getJson(datas)
	if !success {
		return datas
	}
	newdatas := make([]readInfo, 0, len(datas))
	for _, v := range datas {
		var compare int
		var exist bool
		if r.timestampKeyInt {
			compare, exist = r.compareWithStartTimeInt(v.data)
		} else {
			compare, exist = r.compareWithStartTime(v.data)
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
			r.timestampmux.RLock()
			//判断map去重
			if _, ok := r.trimecachemap[v.json]; !ok {
				newdatas = append(newdatas, v)
			}
			r.timestampmux.RUnlock()
		}
	}
	return newdatas
}
