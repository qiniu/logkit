package apps

import (
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const KeyRedis = "key"

var (
	_ transforms.StatsTransformer = &Redis{}
	_ transforms.Transformer      = &Redis{}

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
	Key   string `json:"key"`
	stats StatsInfo
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
	var err, fmtErr error
	errNum := 0
	keys := GetKeys(r.Key)
	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, r.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", r.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		if len(strVal) == 0 {
			errNum, err = transforms.SetError(errNum, fmt.Errorf("string value is empty,skip this record"), transforms.General, "")
			continue
		}
		if strVal[0] == '[' {
			newMap, err := parserWithV2(strings.TrimSpace(strVal))
			if err != nil {
				errNum, err = transforms.SetError(errNum, err, transforms.General, "")
				continue
			}
			for k, v := range newMap {
				setErr := SetMapValue(datas[i], v, false, k)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, r.Key)
				}
			}
		} else {
			newMap, err := parserWithV3(strings.TrimSpace(strVal))
			if err != nil {
				errNum, err = transforms.SetError(errNum, err, transforms.General, "")
				continue
			}
			for k, v := range newMap {
				setErr := SetMapValue(datas[i], v, false, k)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, r.Key)
				}
			}
		}
	}
	r.stats, fmtErr = transforms.SetStatsInfo(err, r.stats, int64(errNum), int64(len(datas)), r.Type())
	return datas, fmtErr
}

func parserWithV2(raw string) (data Data, err error) {
	endPid := strings.IndexByte(raw, ']')
	if endPid <= 0 {
		return nil, fmt.Errorf("can't find ']' in record [%s],this record skipped", raw)
	}
	pid := raw[1:endPid]
	if endPid+1 >= len(raw) {
		return nil, fmt.Errorf("can't find date fieldname in record [%s],this record skipped", raw)
	}
	restStr := strings.TrimSpace(raw[endPid+1:])
	rest := strings.SplitN(restStr, " ", 5)
	if len(rest) != 5 {
		return nil, fmt.Errorf("rest string [%s] splitted by blank length is not equal 5", restStr)
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
		return nil, fmt.Errorf("can't find role fieldname in record [%s],this record skipped", raw)
	}
	restStr := strings.TrimSpace(raw[colon+1:])
	rest := strings.SplitN(restStr, " ", 6)
	if len(rest) != 6 {
		return nil, fmt.Errorf("rest string [%s] splitted by blank length is not equal 6", restStr)
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
