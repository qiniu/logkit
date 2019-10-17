package syslog

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

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
	maxline, _ := c.GetIntOr(KeySyslogMaxline, 100)

	nameMap := make(map[string]struct{})
	labels := GetGrokLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)

	timeZoneOffsetRaw, _ := c.GetStringOr(KeyTimeZoneOffset, "")
	timeZoneOffset := ParseTimeZoneOffset(timeZoneOffsetRaw)

	format := syslog.GetFormt(rfctype)
	rfctype = strings.ToLower(rfctype)
	var needModefyTime = false
	if rfctype == "rfc3164" || rfctype == "automic" {
		needModefyTime = true
	}
	buff := bytes.NewBuffer([]byte{})
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	return &SyslogParser{
		name:                 name,
		labels:               labels,
		buff:                 buff,
		format:               format,
		disableRecordErrData: disableRecordErrData,
		maxline:              maxline,
		curline:              0,
		keepRawData:          keepRawData,
		timeZoneOffset:       timeZoneOffset,
		needModefyTime:       needModefyTime,
		numRoutine:           numRoutine,
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

		if parseResult.Err != nil {
			se.AddErrors()
			se.LastError = parseResult.Err.Error()
			if p.disableRecordErrData && !p.keepRawData {
				se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
				datasourceIndex++
			}
			if parseResult.Data != nil {
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
		if p.keepRawData {
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

func (p *SyslogParser) parse(line string) (data Data, err error) {
	if p.buff.Len() > 0 {
		if line == PandoraParseFlushSignal {
			return p.Flush()
		}

		if p.curline >= p.maxline || p.format.IsNewLine([]byte(line)) {
			data, err = p.Flush()
			if err != nil {
				return data, err
			}
		} else {
			p.curline++
		}
	}

	if line != PandoraParseFlushSignal {
		_, err = p.buff.Write([]byte(line))
		if err != nil {
			if !p.disableRecordErrData || p.keepRawData {
				data = make(Data)
			}
			if !p.disableRecordErrData {
				data[KeyPandoraStash] = string(p.buff.Bytes())
			}
			if p.keepRawData {
				data[KeyRawData] = string(p.buff.Bytes())
			}
		}
		return data, err
	}
	return data, nil
}

func (p *SyslogParser) Flush() (data Data, err error) {
	sparser := p.format.GetParser(p.buff.Bytes())
	err = sparser.Parse()
	if err == nil || err.Error() == "No structured data" {
		data = Data(sparser.Dump())
		if sparser.NeedModifyTime() && p.needModefyTime {
			data["timestamp"] = data["timestamp"].(time.Time).Add(time.Duration(p.timeZoneOffset) * time.Hour)
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
			data[KeyPandoraStash] = string(p.buff.Bytes())
		}
		if p.keepRawData {
			data[KeyRawData] = string(p.buff.Bytes())
		}
	}
	p.curline = 0
	p.buff.Reset()
	return data, err
}
