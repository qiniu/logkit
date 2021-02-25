package syslog

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/parse/syslog"
)

func init() {
	parser.RegisterConstructor(TypeSyslog, NewParser)
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	rfctype, _ := c.GetStringOr(KeyRFCType, "automic")
	parseYear, _ := c.GetBoolOr(KeyRFC3164ParseYear, false)
	maxline, _ := c.GetIntOr(KeySyslogMaxline, 100)

	nameMap := make(map[string]struct{})
	labels := GetGrokLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)
	facilityDetail, _ := c.GetBoolOr(KeyFacilityDetail, false)
	severityDetail, _ := c.GetBoolOr(KeySeverityDetail, false)

	timeZoneOffsetRaw, _ := c.GetStringOr(KeyTimeZoneOffset, "")
	timeZoneOffset := ParseTimeZoneOffset(timeZoneOffsetRaw)

	format := syslog.GetFormat(rfctype, parseYear)
	rfctype = strings.ToLower(rfctype)
	var needModefyTime = false
	if rfctype == "rfc3164" || rfctype == "automic" {
		needModefyTime = true
	}
	buff := bytes.NewBuffer([]byte{})
	return &SyslogParser{
		name:                 name,
		labels:               labels,
		buff:                 buff,
		format:               format,
		disableRecordErrData: disableRecordErrData,
		maxline:              maxline,
		curline:              0,
		keepRawData:          keepRawData,
		facilityDetail:       facilityDetail,
		severityDetail:       severityDetail,
		timeZoneOffset:       timeZoneOffset,
		needModefyTime:       needModefyTime,
		parseYear:            parseYear,
		numRoutine:           1, // 不支持多线程
	}, nil
}

type SyslogParser struct {
	name                 string
	labels               []GrokLabel
	buff                 *bytes.Buffer
	format               syslog.Format
	maxline              int
	curline              int
	disableRecordErrData bool
	keepRawData          bool
	timeZoneOffset       int
	needModefyTime       bool
	parseYear            bool
	facilityDetail       bool
	severityDetail       bool

	numRoutine int
}

func (p *SyslogParser) Name() string {
	return p.name
}

func (p *SyslogParser) Type() string {
	return TypeSyslog
}

func (p *SyslogParser) Parse(lines []string) ([]Data, error) {
	var (
		lineLen = len(lines)
		datas   = make([]Data, lineLen)
		se      = &StatsError{}

		numRoutine = p.numRoutine
		sendChan   = make(chan parser.ParseInfo)
		resultChan = make(chan parser.ParseResult)
		wg         = new(sync.WaitGroup)
	)

	if lineLen < numRoutine {
		numRoutine = lineLen
	}

	if p.buff.Len() == 0 && len(lines) == 1 && lines[0] == PandoraParseFlushSignal {
		return []Data{}, nil
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go parser.ParseLine(sendChan, resultChan, wg, true, p.parse)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, line := range lines {
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
		if len(parseResult.Line) == 0 {
			se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
			datasourceIndex++
			continue
		}

		if p.facilityDetail {
			if v, ok := parseResult.Data[Facility]; ok {
				code, _ := v.(uint8)
				parseResult.Data[Facility] = syslog.MessageFacilities[int(code)]
			}
		}
		if p.severityDetail {
			if v, ok := parseResult.Data[Severity]; ok {
				code, _ := v.(uint8)
				parseResult.Data[Severity] = syslog.MessageSeverities[int(code)]
			}
		}
		if parseResult.Err != nil {
			se.AddErrors()
			se.LastError = parseResult.Err.Error()
			if p.disableRecordErrData && !p.keepRawData {
				se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
				datasourceIndex++
			} else {
				datas[dataIndex] = parseResult.Data
				dataIndex++
			}
			continue
		}
		if len(parseResult.Data) < 1 { //数据为空时不发送
			se.AddSuccess()
			continue
		}
		for _, label := range p.labels {
			parseResult.Data[label.Name] = label.Value
		}
		se.AddSuccess()
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

func (p *SyslogParser) parse(line string) (data Data, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("syslog parse data error\npanic: %v\nstack: %s", rec, debug.Stack())
		}
	}()
	if p.buff.Len() > 0 {
		if line == PandoraParseFlushSignal {
			return p.Flush()
		}
	}

	if line != PandoraParseFlushSignal {
		_, writeErr := p.buff.Write([]byte(line))
		if writeErr != nil {
			log.Errorf("line write to buff failed: %v", writeErr)
			if !p.disableRecordErrData || p.keepRawData {
				data = make(Data)
			}
			if !p.disableRecordErrData {
				data[KeyPandoraStash] = string(p.buff.Bytes())
			}
			if p.keepRawData {
				data[KeyRawData] = string(p.buff.Bytes())
			}
			return data, writeErr
		}
		p.curline++
		if p.curline >= p.maxline || p.format.IsNewLine([]byte(line)) {
			data, err = p.Flush()
		}
	}
	return data, err
}

func (p *SyslogParser) Flush() (data Data, err error) {
	line := p.buff.Bytes()
	data = make(Data)
	defer func() {
		if rec := recover(); rec != nil {
			if !p.disableRecordErrData {
				data[KeyPandoraStash] = string(line)
			}
			if p.keepRawData {
				data[KeyRawData] = string(line)
			}
			err = fmt.Errorf("parse syslog failed when flush data, error: %v, line: %v", rec, string(line))
			log.Errorf("parse syslog failed when flush data\nline: %v\npanic: %v\nstack: %s", string(line), rec, debug.Stack())
		}
	}()
	sparser := p.format.GetParser(line)
	msg, err := sparser.Parse(line)
	if err == nil || (sparser.HasBestEffort() && len(msg) != 0) {
		data = Data(msg)
		if sparser.NeedModifyTime() && p.needModefyTime {
			dataTime, ok := data["timestamp"].(time.Time)
			if ok {
				data["timestamp"] = dataTime.Add(time.Duration(p.timeZoneOffset) * time.Hour)
			}
		}
		err = nil
	} else {
		if p.curline == p.maxline {
			err = fmt.Errorf("syslog meet max line %v, try to parse err %v, check if this is standard rfc3164/rfc5424 syslog", p.maxline, err)
		}
		if !p.disableRecordErrData || p.keepRawData {
			data = make(Data)
		}
		if !p.disableRecordErrData {
			data[KeyPandoraStash] = string(line)
		}
	}
	if p.keepRawData {
		data[KeyRawData] = string(line)
	}
	p.curline = 0
	p.buff.Reset()
	return data, err
}
