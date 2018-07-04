package transforms

import (
	"fmt"

	. "github.com/qiniu/logkit/utils/models"
)

const (
	General = iota
	GetErr
	SetErr

	NotExistErr = "transform key %v not exist in data"
	TypeErr     = "value of %v is not the type of map[string]interface{}"
)

func GetTransformerUsages() []KeyValue {
	var ModeUsages []KeyValue
	for _, v := range Transformers {
		cr := v()
		ModeUsages = append(ModeUsages, KeyValue{
			Key:   cr.Type(),
			Value: cr.Description(),
		})
	}
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
		return errNum, fmt.Errorf(NotExistErr, key)
	case SetErr:
		return errNum, fmt.Errorf(TypeErr, key)
	default:
		return errNum, currentErr
	}
}
