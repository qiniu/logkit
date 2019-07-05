package linuxaudit

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	DefaultCheckKey = 5
	Addr            = "addr"
	Msg             = "msg"
	MsgTimestamp    = Msg + "_timestamp"
	MsgID           = Msg + "_id"
)

func processSpace(key, tmp string, data Data) {
	if key == "" {
		return
	}
	tmp = strings.TrimSpace(tmp)
	if tmp == "" {
		return
	}

	if strings.HasSuffix(tmp, ":") {
		tmp = strings.TrimSuffix(tmp, ":")
	}
	if key == Msg && strings.HasPrefix(tmp, "audit(") {
		if getTimestampID(tmp, data) {
			return
		}
	}
	tmp = strings.TrimSuffix(tmp, ":")
	SetData(key, tmp, data)

	return
}

func getTimestampID(tmp string, data Data) bool {
	tmp = strings.TrimPrefix(tmp, "audit(")
	tmp = strings.TrimSuffix(tmp, ")")
	arr := strings.SplitN(tmp, ":", 2)
	if len(arr) < 2 {
		return false
	}

	timestamp := strings.TrimSpace(arr[0])
	if timestamp != "" {
		if idx := strings.Index(arr[0], "."); idx != -1 {
			timestamp = arr[0][:idx] + arr[0][idx+1:]
		}
		tm, err := GetTime(timestamp)
		if err != nil {
			log.Errorf("parse msg timestamp: %s failed: %v", timestamp, err)
			data[MsgTimestamp] = strings.TrimSpace(arr[0])
		} else {
			data[MsgTimestamp] = FormatWithUserOption("", 0, tm)
		}
	}

	id := strings.TrimSpace(arr[1])
	if id == "" {
		return true
	}
	data[MsgID] = id
	return true
}

func SetData(key string, value interface{}, data Data) {
	if key == "" || value == nil {
		return
	}
	if val, ok := value.(string); ok && val == "" {
		return
	}

	_, ok := data[key]
	if !ok {
		data[key] = value
		return
	}

	i := 1
	finalKey := key + "_" + strconv.Itoa(i)
	for ; i <= DefaultCheckKey; i++ {
		_, ok = data[finalKey]
		if !ok {
			data[finalKey] = value
			return
		}
		finalKey = key + "_" + strconv.Itoa(i)
	}
	data[key] = value
}

func Parse(line string) (Data, error) {
	var (
		data   = make(Data)
		tmp    string
		key    string
		prefix bool
	)

	for idx, c := range line {
		if c == '\'' {
			prefix = !prefix
			processSubLine(tmp, line[idx+1:], key, data)
			tmp, key = "", ""
		}

		if prefix || c == '"' {
			continue
		}

		if c == '=' {
			key = tmp
			tmp = ""
			continue
		}

		if unicode.IsSpace(c) {
			processSpace(key, tmp, data)
			tmp, key = "", ""
			continue
		}

		tmp += string(c)
	}

	if key != "" {
		if strings.HasSuffix(tmp, ":") {
			tmp = strings.TrimSuffix(tmp, ":")
		}
		SetData(key, tmp, data)
	}
	return data, nil
}

func processSubLine(tmp, tmpLine, key string, data Data) {
	if key == "" {
		return
	}

	if tmp != "" {
		tmp = strings.TrimSuffix(tmp, ":")
		SetData(key, tmp, data)
		return
	}

	suffix := strings.Index(tmpLine, "'")
	if suffix != -1 {
		tmpData, err := Parse(tmpLine[:suffix+1])
		if err != nil {
			log.Errorf("parse line: %s failed: %v", tmpLine[:suffix+1], err)
			return
		}
		if len(tmpData) == 0 {
			return
		}
		value := convertDataToMap(tmpData)
		SetData(key, value, data)
		if val, ok := data[Msg]; ok {
			setAddr(data, val)
		}
	}
	return
}

func convertDataToMap(data Data) map[string]interface{} {
	if len(data) == 0 {
		return make(map[string]interface{})
	}

	res := make(map[string]interface{})
	for k, v := range data {
		res[k] = v
	}
	return res
}

func setAddr(data Data, val interface{}) {
	valMap, ok := val.(map[string]interface{})
	if !ok {
		return
	}

	addr, ok := valMap[Addr]
	if !ok {
		return
	}

	addrStr, ok := addr.(string)
	if !ok {
		return
	}

	if addrStr == "?" {
		return
	}

	data[Addr] = addrStr
}
