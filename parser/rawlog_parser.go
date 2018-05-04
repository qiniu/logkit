package parser

import (
	"strings"
	"time"

	"errors"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	KeyRaw       = "raw"
	KeyTimestamp = "timestamp"
)

func NewRawlogParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	withtimestamp, _ := c.GetBoolOr(KeyTimestamp, true)
	nameMap := make(map[string]struct{})
	labels := GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)

	return &RawlogParser{
		name:                 name,
		labels:               labels,
		withTimeStamp:        withtimestamp,
		disableRecordErrData: disableRecordErrData,
	}, nil
}

type RawlogParser struct {
	name                 string
	labels               []Label
	withTimeStamp        bool
	disableRecordErrData bool
}

func (p *RawlogParser) Name() string {
	return p.name
}

func (p *RawlogParser) Type() string {
	return TypeRaw
}

func (p *RawlogParser) Parse(lines []string) ([]Data, error) {
	emptyErr := errors.New("empty line error")
	se := &StatsError{}
	datas := []Data{}
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
			se.AddErrors()
			se.ErrorDetail = emptyErr
			se.ErrorIndex = append(se.ErrorIndex, idx)
			if !p.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				datas = append(datas, errData)
			}
			continue
		}
		d := Data{}
		d[KeyRaw] = line
		if p.withTimeStamp {
			d[KeyTimestamp] = time.Now().Format(time.RFC3339Nano)
		}
		for _, label := range p.labels {
			d[label.Name] = label.Value
		}
		datas = append(datas, d)
		se.AddSuccess()
	}
	return datas, se
}
