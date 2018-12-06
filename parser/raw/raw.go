package raw

import (
	"time"

	"strings"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(TypeRaw, NewParser)
}

type Parser struct {
	name                 string
	labels               []GrokLabel
	withTimeStamp        bool
	disableRecordErrData bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	withtimestamp, _ := c.GetBoolOr(KeyTimestamp, true)
	nameMap := make(map[string]struct{})
	labels := GetGrokLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		withTimeStamp:        withtimestamp,
		disableRecordErrData: disableRecordErrData,
	}, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeRaw
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	se := &StatsError{}
	datas := make([]Data, 0, len(lines))
	for _, line := range lines {
		//raw就是原样全copy到Raw字段
		if len(strings.TrimSpace(line)) < 1 {
			continue
		}
		data := Data{KeyRaw: line}
		if p.withTimeStamp {
			data[KeyTimestamp] = time.Now().Format(time.RFC3339Nano)
		}
		for _, label := range p.labels {
			data[label.Name] = label.Value
		}
		se.AddSuccess()
		datas = append(datas, data)
	}
	if se.Errors == 0 {
		return datas, nil
	}

	return datas, se
}
