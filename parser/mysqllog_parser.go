package parser

import (
	"regexp"

	"github.com/Preetam/mysqllog"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	CompliedPatterns = make(map[string]*regexp.Regexp)
	for k, v := range HeaderPattern {
		c, _ := regexp.Compile(v)
		CompliedPatterns[k] = c
	}
}

type MysqllogParser struct {
	name                 string
	ps                   *mysqllog.Parser
	labels               []Label
	disableRecordErrData bool
}

func NewMysqllogParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})

	nameMap := make(map[string]struct{})
	labels := GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)

	return &MysqllogParser{
		name:                 name,
		labels:               labels,
		disableRecordErrData: disableRecordErrData,
		ps:                   &mysqllog.Parser{},
	}, nil
}

func (p *MysqllogParser) Name() string {
	return p.name
}

func (p *MysqllogParser) Type() string {
	return TypeMysqlLog
}

func (p *MysqllogParser) parse(line string) (d Data, err error) {
	if line == PandoraParseFlushSignal {
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
func (p *MysqllogParser) Parse(lines []string) ([]Data, error) {
	var datas []Data
	se := &utils.StatsError{}
	for idx, line := range lines {
		d, err := p.parse(line)
		if err != nil {
			se.AddErrors()
			se.ErrorIndex = append(se.ErrorIndex, idx)
			se.ErrorDetail = err
			if !p.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
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

func (p *MysqllogParser) Flush() (data Data, err error) {
	data = Data(p.ps.Flush())
	return
}
