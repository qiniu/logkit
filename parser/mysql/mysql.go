package mysql

import (
	"strings"

	"github.com/Preetam/mysqllog"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(parser.TypeMySQL, NewParser)
}

type Parser struct {
	name                 string
	ps                   *mysqllog.Parser
	labels               []parser.Label
	disableRecordErrData bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})

	nameMap := make(map[string]struct{})
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
		ps:                   &mysqllog.Parser{},
	}, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return parser.TypeMySQL
}

func (p *Parser) parse(line string) (d Data, err error) {
	if line == parser.PandoraParseFlushSignal {
		return p.Flush()
	}
	event := p.ps.ConsumeLine(line)
	if event == nil {
		return
	}
	d = make(Data, len(event)+len(p.labels))
	for k, v := range event {
		d[k] = v
	}
	for _, l := range p.labels {
		d[l.Name] = l.Value
	}
	return d, nil
}
func (p *Parser) Parse(lines []string) ([]Data, error) {
	var datas []Data
	se := &StatsError{}
	for idx, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		d, err := p.parse(line)
		if err != nil {
			se.AddErrors()
			se.ErrorDetail = err
			if !p.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				datas = append(datas, errData)
			} else {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			}
			continue
		}
		if d == nil || len(d) < 1 {
			continue
		}
		se.AddSuccess()
		datas = append(datas, d)
	}
	return datas, se
}

func (p *Parser) Flush() (data Data, err error) {
	data = Data(p.ps.Flush())
	return
}
