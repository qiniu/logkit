package parser

import (
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
)

const (
	KeyRaw       = "raw"
	KeyTimestamp = "timestamp"
)

func NewRawlogParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	nameMap := make(map[string]struct{})
	labels := GetLabels(labelList, nameMap)

	return &RawlogParser{
		name:   name,
		labels: labels,
	}, nil
}

type RawlogParser struct {
	name   string
	labels []Label
}

func (p *RawlogParser) Name() string {
	return p.name
}

func (p *RawlogParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
			continue
		}
		d := sender.Data{}
		d[KeyRaw] = line
		d[KeyTimestamp] = time.Now().Format(time.RFC3339Nano)
		for _, label := range p.labels {
			d[label.Name] = label.Value
		}
		datas = append(datas, d)
	}
	return datas, nil
}
