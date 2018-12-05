package transforms

import (
	"errors"
	"fmt"
	"sort"

	. "github.com/qiniu/logkit/utils/models"
)

const (
	General = iota
	GetErr
	SetErr
)

func GetTransformerUsages() KeyValueSlice {
	var ModeUsages KeyValueSlice
	for _, v := range Transformers {
		cr := v()
		ModeUsages = append(ModeUsages, KeyValue{
			Key:     cr.Type(),
			Value:   cr.Description(),
			SortKey: cr.Type(),
		})
	}
	sort.Stable(ModeUsages)
	return ModeUsages
}

func GetTransformerOptions() map[string][]Option {
	ModeKeyOptions := make(map[string][]Option)
	for _, v := range Transformers {
		cr := v()
		ModeKeyOptions[cr.Type()] = cr.ConfigOptions()
	}
	return ModeKeyOptions
}

func SetStatsInfo(err error, stats StatsInfo, errNum, dataLen int64, transformType string) (StatsInfo, error) {
	var fmtErr error
	if err != nil {
		fmtErr = fmt.Errorf("find total %v erorrs in transform %v, last error info is %v", errNum, transformType, err)
		stats.LastError = fmtErr.Error()
	}
	stats.Errors += errNum
	stats.Success += dataLen - errNum
	return stats, fmtErr
}

func SetError(errNum int, currentErr error, errType int, key string) (int, error) {
	errNum++
	switch errType {
	case GetErr:
		return errNum, errors.New("transform key " + key + " not exist in data")
	case SetErr:
		return errNum, errors.New("value of " + key + " is not the type of map[string]interface{}")
	default:
		return errNum, currentErr
	}
}
