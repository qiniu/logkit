package kafkarest

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	KEY_SRC_IP   = "source_ip"
	KEY_METHOD   = "method"
	KEY_TOPIC    = "topic"
	KEY_CODE     = "code"
	KEY_RESP_LEN = "resp_len"
	KEY_DURATION = "duration"
	KEY_LOG_TIME = "log_time"
	KEY_ERROR    = "error"
	KEY_WARN     = "warn"
	EMPTY_STRING = ""
)

func init() {
	parser.RegisterConstructor(parser.TypeKafkaRest, NewParser)
}

type Parser struct {
	name                 string
	labels               []parser.Label
	disableRecordErrData bool
}

func (krp *Parser) Name() string {
	return krp.name
}

func (krp *Parser) Type() string {
	return parser.TypeKafkaRest
}

func (krp *Parser) Parse(lines []string) ([]Data, error) {
	datas := []Data{}
	for _, line := range lines {
		line = strings.Replace(line, "\n", " ", -1)
		line = strings.Replace(line, "\t", "\\t", -1)
		line = strings.Trim(line, " ")
		fields := strings.Split(line, " ")
		if len(fields) >= 3 {
			if len(fields) == 16 && fields[2] == "INFO" {
				datas = append(datas, krp.parseRequestLog(fields))
			} else if (len(fields) > 0 && fields[2] == "ERROR") || (len(fields) > 0 && fields[2] == "WARN") {
				datas = append(datas, krp.parseAbnormalLog(fields))
			}
		} else {
			if !krp.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				datas = append(datas, errData)
			}
		}
	}
	return datas, nil
}

func (krp *Parser) parseRequestLog(fields []string) Data {
	d := Data{}
	d[KEY_SRC_IP] = krp.ParseIp(fields)
	d[KEY_TOPIC] = krp.ParseTopic(fields)
	d[KEY_METHOD] = krp.ParseMethod(fields)
	d[KEY_CODE] = krp.ParseCode(fields)
	d[KEY_RESP_LEN] = krp.ParseRespCL(fields)
	d[KEY_DURATION] = krp.ParseDuration(fields)
	d[KEY_LOG_TIME] = krp.ParseLogTime(fields)
	for _, label := range krp.labels {
		d[label.Name] = label.Value
	}
	return d
}

func (krp *Parser) parseAbnormalLog(fields []string) Data {
	d := Data{}
	d[KEY_LOG_TIME] = krp.ParseLogTime(fields)
	if fields[2] == "ERROR" {
		d[KEY_ERROR] = 1
	} else if fields[2] == "WARN" {
		d[KEY_WARN] = 1
	}
	for _, label := range krp.labels {
		d[label.Name] = label.Value
	}
	return d
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	nameMap := map[string]struct{}{
		KEY_SRC_IP:   struct{}{},
		KEY_METHOD:   struct{}{},
		KEY_TOPIC:    struct{}{},
		KEY_CODE:     struct{}{},
		KEY_RESP_LEN: struct{}{},
		KEY_DURATION: struct{}{},
		KEY_LOG_TIME: struct{}{},
	}
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
	}, nil
}

func (krp *Parser) ParseIp(fields []string) string {
	if len(fields) < 1 {
		return EMPTY_STRING
	}
	return fields[3]
}

func (krp *Parser) ParseMethod(fields []string) string {
	if len(fields) < 1 {
		return EMPTY_STRING
	}
	str := fields[8]
	return strings.TrimPrefix(str, "\"")
}

func (krp *Parser) ParseTopic(fields []string) string {
	if len(fields) < 1 {
		return EMPTY_STRING
	}
	str := fields[9]
	topic_fields := strings.Split(str, `/`)
	if len(topic_fields) > 2 {
		str = topic_fields[2]
	} else {
		str = EMPTY_STRING
	}
	return str

}

func (krp *Parser) ParseCode(fields []string) int {
	if len(fields) < 1 {
		return 0
	}
	str := fields[11]
	code, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return code
}

func (krp *Parser) ParseDuration(fields []string) int {
	if len(fields) < 1 {
		return 0
	}
	str := fields[14]
	duration, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return duration
}

func (krp *Parser) ParseRespCL(fields []string) int {
	if len(fields) < 1 {
		return 0
	}
	str := fields[12]
	respcl, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return respcl
}

func (krp *Parser) ParseLogTime(fields []string) int64 {
	if len(fields) < 1 {
		return 0
	}
	str := fmt.Sprintf("%s %s", fields[0], fields[1])
	str = strings.Trim(str, "[")
	str = strings.Trim(str, "]")
	_, zoneValue := times.GetTimeZone()
	ymdhms := str[:len(str)-4] + zoneValue
	precesion_str := str[20:len(str)]
	precesion_int, err := strconv.ParseInt(precesion_str, 10, 64)
	if err != nil {
		log.Errorf("KafaRestlogParser parse time err %v", err)
		return 0
	}
	t, err := time.Parse("2006-01-02 15:04:05 -0700", ymdhms)
	ts := t.Unix()*1000 + precesion_int
	if err != nil {
		log.Error(err)
		return 0
	}
	return ts
}
