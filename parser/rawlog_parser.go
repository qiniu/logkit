package parser

import (
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
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

	return &RawlogParser{
		name:          name,
		labels:        labels,
		withTimeStamp: withtimestamp,
	}, nil
}

type RawlogParser struct {
	name          string
	labels        []Label
	withTimeStamp bool
}

func (p *RawlogParser) Name() string {
	return p.name
}

func (p *RawlogParser) Type() string {
	return TypeRaw
}

func (p *RawlogParser) Parse(lines []string) ([]sender.Data, error) {

	se := &utils.StatsError{}
	datas := []sender.Data{}
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
			se.AddErrors()
			se.ErrorIndex = append(se.ErrorIndex, idx)
			continue
		}
		d := sender.Data{}
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
