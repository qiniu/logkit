package kafkarest

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
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
	parser.RegisterConstructor(TypeKafkaRest, NewParser)
}

type Parser struct {
	name                 string
	labels               []GrokLabel
	disableRecordErrData bool
	keepRawData          bool

	numRoutine int
}

func (krp *Parser) Name() string {
	return krp.name
}

func (krp *Parser) Type() string {
	return TypeKafkaRest
}

func (krp *Parser) Parse(lines []string) ([]Data, error) {
	var (
		lineLen    = len(lines)
		datas      = make([]Data, lineLen)
		se         = &StatsError{}
		numRoutine = krp.numRoutine

		sendChan   = make(chan parser.ParseInfo)
		resultChan = make(chan parser.ParseResult)
		wg         = new(sync.WaitGroup)
	)

	if lineLen < numRoutine {
		numRoutine = lineLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go parser.ParseLine(sendChan, resultChan, wg, false, krp.parse)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, line := range lines {
			line = strings.Replace(line, "\n", " ", -1)
			line = strings.Replace(line, "\t", "\\t", -1)
			line = strings.Trim(line, " ")
			sendChan <- parser.ParseInfo{
				Line:  line,
				Index: idx,
			}
		}
		close(sendChan)
	}()

	var parseResultSlice = make(parser.ParseResultSlice, lineLen)
	for resultInfo := range resultChan {
		parseResultSlice[resultInfo.Index] = resultInfo
	}

	se.DatasourceSkipIndex = make([]int, lineLen)
	datasourceIndex := 0
	dataIndex := 0
	for _, parseResult := range parseResultSlice {
		if parseResult.Err != nil {
			se.AddErrors()
			se.LastError = parseResult.Err.Error()
			errData := make(Data)
			if !krp.disableRecordErrData {
				errData[KeyPandoraStash] = parseResult.Line
			} else if !krp.keepRawData {
				se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
				datasourceIndex++
			}
			if krp.keepRawData {
				errData[KeyRawData] = parseResult.Line
			}
			if !krp.disableRecordErrData || krp.keepRawData {
				datas[dataIndex] = errData
				dataIndex++
			}
			continue
		}

		if len(parseResult.Data) < 1 { //数据为空时不发送
			se.AddSuccess()
			continue
		}

		se.AddSuccess()
		if krp.keepRawData {
			parseResult.Data[KeyRawData] = parseResult.Line
		}
		datas[dataIndex] = parseResult.Data
		dataIndex++
	}

	se.DatasourceSkipIndex = se.DatasourceSkipIndex[:datasourceIndex]
	datas = datas[:dataIndex]
	if se.Errors == 0 && len(se.DatasourceSkipIndex) == 0 {
		return datas, nil
	}
	return datas, se
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
		if _, ok := d[label.Name]; ok {
			continue
		}
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
		if _, ok := d[label.Name]; ok {
			continue
		}
		d[label.Name] = label.Value
	}
	return d
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)
	nameMap := map[string]struct{}{
		KEY_SRC_IP:   {},
		KEY_METHOD:   {},
		KEY_TOPIC:    {},
		KEY_CODE:     {},
		KEY_RESP_LEN: {},
		KEY_DURATION: {},
		KEY_LOG_TIME: {},
	}
	labels := GetGrokLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}

	return &Parser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
		keepRawData:          keepRawData,
		numRoutine:           numRoutine,
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
	return strings.TrimPrefix(fields[8], "\"")
}

func (krp *Parser) ParseTopic(fields []string) string {
	if len(fields) < 1 {
		return EMPTY_STRING
	}
	topicFields := strings.Split(fields[9], `/`)
	if len(topicFields) > 2 {
		return topicFields[2]
	}
	return EMPTY_STRING
}

func (krp *Parser) ParseCode(fields []string) int {
	if len(fields) < 1 {
		return 0
	}
	code, err := strconv.Atoi(fields[11])
	if err != nil {
		return 0
	}
	return code
}

func (krp *Parser) ParseDuration(fields []string) int {
	if len(fields) < 1 {
		return 0
	}
	duration, err := strconv.Atoi(fields[14])
	if err != nil {
		return 0
	}
	return duration
}

func (krp *Parser) ParseRespCL(fields []string) int {
	if len(fields) < 1 {
		return 0
	}
	respCl, err := strconv.Atoi(fields[12])
	if err != nil {
		return 0
	}
	return respCl
}

func (krp *Parser) ParseLogTime(fields []string) int64 {
	if len(fields) < 1 {
		return 0
	}
	str := fmt.Sprintf("%s %s", fields[0], fields[1])
	str = strings.Trim(str, "[")
	str = strings.Trim(str, "]")
	_, zoneValue := times.GetTimeZone()
	zoneStr := str[:len(str)-4] + zoneValue
	precessionInt, err := strconv.ParseInt(str[20:], 10, 64)
	if err != nil {
		log.Errorf("KafaRestlogParser parse time err %v", err)
		return 0
	}
	t, err := time.Parse("2006-01-02 15:04:05 -0700", zoneStr)
	ts := t.Unix()*1000 + precessionInt
	if err != nil {
		log.Error(err)
		return 0
	}
	return ts
}

func (krp *Parser) parse(line string) (data Data, err error) {
	fields := strings.Split(line, " ")
	if len(fields) < 3 {
		return nil, fmt.Errorf("kafka parser need data fields at least 3, acutal get %d", len(fields))
	}

	if len(fields) == 16 && fields[2] == "INFO" {
		return krp.parseRequestLog(fields), nil
	}
	if (len(fields) > 0 && fields[2] == "ERROR") || (len(fields) > 0 && fields[2] == "WARN") {
		return krp.parseAbnormalLog(fields), nil
	}
	return nil, nil
}
