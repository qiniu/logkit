package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils/models"
)

func ConvertLong(v interface{}) (int64, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return 0, errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return 0, errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return dv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(dv.Uint()), nil
	case reflect.String:
		return strconv.ParseInt(dv.String(), 10, 64)
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(int64); ok {
			return ret, nil
		}
		if ret, ok := idv.(int); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint64); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(string); ok {
			return strconv.ParseInt(ret, 10, 64)
		}
		if ret, ok := idv.(int8); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(int16); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(int32); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint8); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint16); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint32); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.([]byte); ok {
			if len(ret) == 8 {
				return int64(binary.BigEndian.Uint64(ret)), nil
			} else {
				return strconv.ParseInt(string(ret), 10, 64)
			}
		}
		if idv == nil {
			return 0, nil
		}
		log.Errorf("sql reader convertLong for type %v is not supported", reflect.TypeOf(idv))
	}
	return 0, fmt.Errorf("%v type can not convert to int", dv.Kind())
}

func ConvertFloat(v interface{}) (float64, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return 0, errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return 0, errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(dv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(dv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return dv.Float(), nil
	case reflect.String:
		return strconv.ParseFloat(dv.String(), 64)
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(float64); ok {
			return ret, nil
		}
		if ret, ok := idv.(float32); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int64); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint64); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(string); ok {
			return strconv.ParseFloat(ret, 64)
		}
		if ret, ok := idv.(int8); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int16); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int32); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint8); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint16); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint32); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.([]byte); ok {
			return strconv.ParseFloat(string(ret), 64)
		}
		if idv == nil {
			return 0, nil
		}
		log.Errorf("sql reader convertFloat for type %v is not supported", reflect.TypeOf(idv))
	}
	return 0, fmt.Errorf("%v type can not convert to int", dv.Kind())
}

func ConvertDate(v interface{}) (time.Time, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return time.Time{}, errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return time.Time{}, errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(time.Time); ok {
			return ret, nil
		}
		if ret, ok := idv.(*time.Time); ok {
			return *ret, nil
		}
		if idv == nil {
			return time.Time{}, nil
		}

		if ret, ok := idv.([]byte); ok {
			return times.StrToTimeLocation(string(ret), time.Local)
		}
		log.Errorf("sql reader convertDate for type %v is not supported", reflect.TypeOf(idv))
	}
	return time.Time{}, fmt.Errorf("%v type can not convert to string", dv.Kind())
}

func ConvertString(v interface{}) (string, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return "", errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return "", errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.Itoa(int(dv.Int())), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.Itoa(int(dv.Uint())), nil
	case reflect.String:
		return dv.String(), nil
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(int64); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(int); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint64); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(string); ok {
			return ret, nil
		}
		if ret, ok := idv.(int8); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(int16); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(int32); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint8); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint16); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint32); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.([]byte); ok {
			return string(ret), nil
		}
		//Postgres的时间类型为time.Time
		if ret, ok := idv.(time.Time); ok {
			return ret.Format(time.RFC3339), nil
		}
		if idv == nil {
			return "", nil
		}
		log.Errorf("sql reader convertString for type %v is not supported", reflect.TypeOf(idv))
	}
	return "", fmt.Errorf("%v type can not convert to string", dv.Kind())
}

func ConvertBool(v interface{}) (bool, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return false, errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return false, errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(bool); ok {
			return bool(ret), nil
		}
		if idv == nil {
			return false, nil
		}
		log.Errorf("sql reader convertBool for type %v is not supported", reflect.TypeOf(idv))
	}
	return false, fmt.Errorf("%v type can not convert to Bool", dv.Kind())
}

func ConvertScanArgs(data models.Data, scanArg interface{}, column, runnerName, name string,
	nochiced bool, schemas map[string]string) (int64, error) {
	var (
		bytes int64
		err   error
	)
	vtype, ok := schemas[column]
	if !ok {
		vtype = "unknown"
	}
	switch vtype {
	case "long":
		val, serr := ConvertLong(scanArg)
		if serr != nil {
			err = fmt.Errorf("runner[%v] %v convertLong for %v (%v) error %v, this key will be ignored", runnerName, name, column, scanArg, serr)
			log.Error(err)
		} else {
			data[column] = val
			bytes = 8
		}
	case "float":
		val, serr := ConvertFloat(scanArg)
		if serr != nil {
			err = fmt.Errorf("runner[%v] %v convertFloat for %v (%v) error %v, this key will be ignored", runnerName, name, column, scanArg, serr)
			log.Error(err)
		} else {
			data[column] = val
			bytes = 8
		}
	case "string":
		val, serr := ConvertString(scanArg)
		if serr != nil {
			err = fmt.Errorf("runner[%v] %v convertString for %v (%v) error %v, this key will be ignored", runnerName, name, column, scanArg, serr)
			log.Error(err)
		} else {
			data[column] = val
			bytes = int64(len(val))
		}
	case "bool":
		val, serr := ConvertBool(scanArg)
		if serr != nil {
			err = fmt.Errorf("runner[%v] %v convertBool for %v (%v) error %v, this key will be ignored", runnerName, name, column, scanArg, serr)
			log.Error(err)
		} else {
			data[column] = val
			bytes = 4
		}
	case "date":
		val, serr := ConvertDate(scanArg)
		if serr != nil {
			err = fmt.Errorf("runner[%v] %v convertDate for %v (%v) error %v, this key will be ignored", runnerName, name, column, scanArg, serr)
			log.Error(err)
		} else {
			data[column] = val
			bytes = 20
		}
	default:
		dealed := false
		if !nochiced {
			dealed = true
			switch d := scanArg.(type) {
			case *string:
				data[column] = *d
				bytes = int64(len(*d))
			case *[]byte:
				data[column] = string(*d)
				bytes = int64(len(*d))
			case *bool:
				data[column] = *d
				bytes = 1
			case int64:
				data[column] = d
				bytes = 8
			case *int64:
				data[column] = *d
				bytes = 8
			case float64:
				data[column] = d
				bytes = 8
			case *float64:
				data[column] = *d
				bytes = 8
			case uint64:
				data[column] = d
				bytes = 8
			case *uint64:
				data[column] = *d
				bytes = 8
			case *interface{}:
				dealed = false
			default:
				dealed = false
			}
		}
		if !dealed {
			val, serr := ConvertString(scanArg)
			if serr != nil {
				err = fmt.Errorf("runner[%v] %v convertString for %v (%v) error %v, this key will be ignored", runnerName, name, column, scanArg, serr)
				log.Error(err)
			} else {
				data[column] = val
				bytes = int64(len(val))
			}
		}
	}
	return bytes, err
}
