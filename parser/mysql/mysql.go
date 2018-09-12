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
	keepRawData          bool
	rawDatas             []string
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})

	nameMap := make(map[string]struct{})
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)
	keepRawData, _ := c.GetBoolOr(parser.KeyKeepRawData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
		ps:                   &mysqllog.Parser{},
		keepRawData:          keepRawData,
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
	if p.keepRawData {
		p.rawDatas = append(p.rawDatas, line)
	}
	event := p.ps.ConsumeLine(line)
	if event == nil {
		return
	}
	d = make(Data, len(event)+len(p.labels)+1)
	for k, v := range event {
		d[k] = v
	}
	for _, l := range p.labels {
		d[l.Name] = l.Value
	}
	if p.keepRawData {
		d[parser.KeyRawData] = strings.Join(p.rawDatas, "\n")
		p.rawDatas = p.rawDatas[:0:0]
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
			errData := make(Data)
			if !p.disableRecordErrData {
				errData[KeyPandoraStash] = line
			} else if !p.keepRawData {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			}
			if p.keepRawData {
				errData[parser.KeyRawData] = line
			}
			if !p.disableRecordErrData || p.keepRawData {
				datas = append(datas, errData)
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
