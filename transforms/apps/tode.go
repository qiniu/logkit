package apps

import (
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const KeyTode = "key"

var (
	_ transforms.StatsTransformer = &Redis{}
	_ transforms.Transformer      = &Redis{}

	OptionTodeKey = Option{
		KeyName:      KeyTode,
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "按tode日志格式进行解析的键(" + KeyTode + ")",
		ToolTip:      "对该字段的值按tode的日志格式进行解析",
		Type:         transforms.TransformTypeString,
	}
)

type Tode struct {
	Key   string `json:"key"`
	stats StatsInfo
}

func (t *Tode) Description() string {
	return `对于日志数据中的每条记录，按照tode日志的格式进行解析。`
}

func (t *Tode) SampleConfig() string {
	return `{
       "type":"tode",
       "key":"myParseKey",
    }`
}

func (t *Tode) ConfigOptions() []Option {
	return []Option{
		OptionTodeKey,
	}
}

func (t *Tode) Type() string {
	return "tode"
}

func (t *Tode) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("tode transformer not support rawTransform")
}

func (t *Tode) Stage() string {
	return transforms.StageAfterParser
}

func (t *Tode) Stats() StatsInfo {
	return t.stats
}

func (t *Tode) SetStats(err string) StatsInfo {
	t.stats.LastError = err
	return t.stats
}

func (t *Tode) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	keys := GetKeys(t.Key)
	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, t.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", t.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		if len(strVal) == 0 {
			errNum, err = transforms.SetError(errNum, fmt.Errorf("string value is empty,skip this record"), transforms.General, "")
			continue
		}
		newMap, err := parser(strings.TrimSpace(strVal))
		if err != nil {
			errNum, err = transforms.SetError(errNum, err, transforms.General, "")
			continue
		}
		for k, v := range newMap {
			setErr := SetMapValue(datas[i], v, false, k)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, t.Key)
			}
		}
	}
	t.stats, fmtErr = transforms.SetStatsInfo(err, t.stats, int64(errNum), int64(len(datas)), t.Type())
	return datas, fmtErr
}

func parser(raw string) (data Data, err error) {
	//get timestamp
	startIndex := strings.IndexByte(raw, '[')
	endIndex := strings.IndexByte(raw, ']')
	if startIndex+1 > endIndex {
		return nil, fmt.Errorf("'[' index is greater than ']' index in string [%s].It's error format", raw)
	}
	timestamp := raw[startIndex+1 : endIndex]
	var level, source, line string
	if endIndex+1 >= len(raw) {
		return nil, fmt.Errorf("log format is fault,failed to find log level in record [%s]", raw)
	}
	//get log level
	arr := strings.SplitN(raw[endIndex+1:], ":", 5)
	if len(arr) != 5 {
		return nil, fmt.Errorf("string [%s] splitted by ':' length is not equal 5", raw)
	}
	level = arr[0]

	//arr[1] is hostname,arr[2] is server name,arr[3] is pid
	//get source and line from arr[4]
	startIndex = strings.IndexByte(arr[4], '[')
	endIndex = strings.IndexByte(arr[4], ']')
	if startIndex+1 > endIndex {
		return nil, fmt.Errorf("'[' index is greater than ']' index in string [%s].It's error format", arr[4])
	}
	sourceAndLine := arr[4][startIndex+1 : endIndex]
	coll := strings.SplitN(sourceAndLine, ":", 2)
	if len(coll) != 2 {
		log.Warnf("failed to find source and line number in record [%s]", raw)
		source = "unknown"
		line = "unknown"
	} else {
		source = coll[0]
		line = coll[1]
	}
	data = Data{
		"timestamp_tode": timestamp,
		"level_tode":     level,
		"host_tode":      arr[1],
		"server_tode":    arr[2],
		"pid_tode":       arr[3],
		"source_tode":    source,
		"line_tode":      line,
	}
	//get message
	if endIndex+1 >= len(arr[4]) {
		log.Warnf("failed to find message in record [%s]", raw)
		data["message_tode"] = "unknown"
		return data, nil
	}
	index := strings.IndexByte(arr[4][endIndex+1:], ':')
	offset := endIndex + 1 + index + 1
	if index == -1 || offset >= len(arr[4]) {
		log.Warnf("log format is fault,failed to find message in record [%s]", raw)
		data["message_tode"] = "unknown"
		return data, nil
	}
	data["message_tode"] = strings.TrimSpace(arr[4][offset:])
	return data, nil
}

func init() {
	transforms.Add("tode", func() transforms.Transformer {
		return &Tode{}
	})
}
