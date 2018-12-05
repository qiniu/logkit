package apps

import (
	"errors"
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const KeyRedis = "key"

var (
	_ transforms.StatsTransformer = &Redis{}
	_ transforms.Transformer      = &Redis{}
	_ transforms.Initializer      = &Redis{}

	OptionRedisKey = Option{
		KeyName:      KeyRedis,
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "按redis日志格式进行解析的键(" + KeyRedis + ")",
		ToolTip:      "对该字段的值按redis的日志格式进行解析",
		Type:         transforms.TransformTypeString,
	}
)

type Redis struct {
	Key string `json:"key"`

	keys       []string
	stats      StatsInfo
	numRoutine int
}

func (r *Redis) Init() error {
	r.keys = GetKeys(r.Key)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	r.numRoutine = numRoutine
	return nil
}

func (r *Redis) Description() string {
	return `对于日志数据中的每条记录，按照redis日志的格式进行解析。`
}

func (r *Redis) SampleConfig() string {
	return `{
       "type":"redis",
       "key":"myParseKey",
    }`
}

func (r *Redis) ConfigOptions() []Option {
	return []Option{
		OptionRedisKey,
	}
}

func (r *Redis) Type() string {
	return "redis"
}

func (r *Redis) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("redis transformer not support rawTransform")
}

func (r *Redis) Stage() string {
	return transforms.StageAfterParser
}

func (r *Redis) Stats() StatsInfo {
	return r.stats
}

func (r *Redis) SetStats(err string) StatsInfo {
	r.stats.LastError = err
	return r.stats
}

func (r *Redis) Transform(datas []Data) ([]Data, error) {
	if len(r.keys) == 0 {
		r.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int
		numRoutine  = r.numRoutine

		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go r.transform(dataPipeline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipeline <- transforms.TransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipeline)
	}()

	var transformResultSlice = make(transforms.TransformResultSlice, dataLen)
	for resultInfo := range resultChan {
		transformResultSlice[resultInfo.Index] = resultInfo
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	r.stats, fmtErr = transforms.SetStatsInfo(err, r.stats, int64(errNum), int64(dataLen), r.Type())
	return datas, fmtErr
}

func parserWithV2(raw string) (data Data, err error) {
	endPid := strings.IndexByte(raw, ']')
	if endPid <= 0 {
		return nil, errors.New("can't find ']' in record [" + raw + "],this record skipped")
	}
	pid := raw[1:endPid]
	if endPid+1 >= len(raw) {
		return nil, errors.New("can't find date fieldname in record [" + raw + "],this record skipped")
	}
	restStr := strings.TrimSpace(raw[endPid+1:])
	rest := strings.SplitN(restStr, " ", 5)
	if len(rest) != 5 {
		return nil, errors.New("rest string [" + restStr + "] splitted by blank length is not equal 5")
	}
	logLevel := getLogLevel(rest[3])
	return Data{
		"pid_redis":       pid,
		"timestamp_redis": strings.Join(rest[0:3], " "),
		"loglevel_redis":  logLevel,
		"message_redis":   rest[4],
	}, nil
}

func parserWithV3(raw string) (data Data, err error) {
	colon := strings.IndexByte(raw, ':')
	pid := raw[:colon]
	if colon+1 >= len(raw) {
		return nil, errors.New("can't find role fieldname in record [" + raw + "],this record skipped")
	}
	restStr := strings.TrimSpace(raw[colon+1:])
	rest := strings.SplitN(restStr, " ", 6)
	if len(rest) != 6 {
		return nil, errors.New("rest string [" + restStr + "] splitted by blank length is not equal 6")
	}
	role := getRole(rest[0])
	logLevel := getLogLevel(rest[4])
	return Data{
		"pid_redis":       pid,
		"timestamp_redis": strings.Join(rest[1:4], " "),
		"loglevel_redis":  logLevel,
		"role_redis":      role,
		"message_redis":   rest[5],
	}, nil
}

func getLogLevel(sign string) (logLevel string) {
	switch sign {
	case ".":
		logLevel = "debug"
	case "-":
		logLevel = "verbose"
	case "*":
		logLevel = "notice"
	case "#":
		logLevel = "warning"
	default:
		logLevel = "unknown"
	}
	return logLevel
}

func getRole(sign string) (role string) {
	switch strings.ToUpper(sign) {
	case "X":
		role = "sentinel"
	case "C":
		role = "RDB/AOF writing child"
	case "S":
		role = "slave"
	case "M":
		role = "master"
	default:
		role = "unknown"
	}
	return role
}

func init() {
	transforms.Add("redis", func() transforms.Transformer {
		return &Redis{}
	})
}

func (r *Redis) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, r.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, r.Key)
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + r.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		if len(strVal) == 0 {
			errNum, err = transforms.SetError(errNum, errors.New("string value is empty,skip this record"), transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		if strVal[0] == '[' {
			newMap, err := parserWithV2(strings.TrimSpace(strVal))
			if err != nil {
				errNum, err = transforms.SetError(errNum, err, transforms.General, "")
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					Err:     err,
					ErrNum:  errNum,
				}
				continue
			}
			for k, v := range newMap {
				setErr := SetMapValue(transformInfo.CurData, v, false, k)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, r.Key)
				}
			}
		} else {
			newMap, err := parserWithV3(strings.TrimSpace(strVal))
			if err != nil {
				errNum, err = transforms.SetError(errNum, err, transforms.General, "")
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					Err:     err,
					ErrNum:  errNum,
				}
				continue
			}
			for k, v := range newMap {
				setErr := SetMapValue(transformInfo.CurData, v, false, k)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, r.Key)
				}
			}
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			Err:     err,
			ErrNum:  errNum,
		}
	}
	wg.Done()
}
