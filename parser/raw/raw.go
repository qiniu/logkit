package raw

import (
	"strings"
	"time"

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
	labels               []parser.Label
	withTimeStamp        bool
	disableRecordErrData bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	withtimestamp, _ := c.GetBoolOr(KeyTimestamp, true)
	nameMap := make(map[string]struct{})
	labels := parser.GetLabels(labelList, nameMap)

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
	datas := []Data{}
	for idx, line := range lines {
		//raw格式的不应该trime空格，只需要判断剔除掉全空就好了
		if len(strings.TrimSpace(line)) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
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

	if se.Errors == 0 {
		return datas, nil
	}
	return datas, se
}
