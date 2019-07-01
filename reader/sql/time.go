package sql

import (
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils/models"
)

//这个query只有一行
func QueryNumber(tsql string, db *sql.DB) (int64, error) {
	rows, err := db.Query(tsql)
	if err != nil {
		log.Error(err)
		return 0, err
	}
	defer rows.Close()
	var scanArgs = []interface{}{new(interface{})}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Error(err)
			return 0, err
		}
		if len(scanArgs) < 1 {
			return 0, errors.New("no data found")
		}

		number, err := ConvertLong(scanArgs[0])
		if err != nil {
			log.Error(err)
		}
		return number, nil
	}
	return 0, errors.New("no data found")
}

func GetTimeIntFromArgs(offsetKeyIndex int, scanArgs []interface{}) (int64, bool) {
	if offsetKeyIndex < 0 || offsetKeyIndex > len(scanArgs) {
		return 0, false
	}
	timeOffset, err := ConvertLong(scanArgs[offsetKeyIndex])
	if err != nil {
		log.Error("getTimeIntFromArgs err ", err)
		return 0, false
	}
	return timeOffset, true
}

func GetTimeFromArgs(offsetKeyIndex int, scanArgs []interface{}) (time.Time, bool) {
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
		case []byte:
			ret, _ := data.([]byte)
			timeDataResult, err := times.StrToTimeLocation(string(ret), time.Local)
			if err != nil {
				log.Errorf("updateStartTime failed as %v(%T) is not time.Time", data, data)
			} else {
				return timeDataResult, true
			}
		default:
			log.Errorf("updateStartTime failed as %v(%T) is not time.Time", data, data)
		}
	default:
		log.Errorf("updateStartTime is not Interface but %v", dv.Kind())
	}
	return time.Time{}, false
}

func GetTimeIntFromData(data models.Data, timestampKey string) (int64, bool) {
	if len(timestampKey) <= 0 {
		return 0, false
	}
	dt, ok := data[timestampKey]
	if !ok {
		return 0, false
	}
	tm, ok := dt.(int64)
	return tm, ok
}

func GetTimeStrFromData(data models.Data, timestampKey string) (string, bool) {
	if len(timestampKey) <= 0 {
		return "", false
	}
	dt, ok := data[timestampKey]
	if !ok {
		return "", false
	}
	tmStr, ok := dt.(string)
	return tmStr, ok
}

func GetTimeFromData(data models.Data, timestampKey string) (time.Time, bool) {
	if len(timestampKey) <= 0 {
		return time.Time{}, false
	}
	dt, ok := data[timestampKey]
	if !ok {
		return time.Time{}, false
	}
	tm, ok := dt.(time.Time)
	return tm, ok
}
