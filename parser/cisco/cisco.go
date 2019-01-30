package cisco

import (
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/grok"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	parser.RegisterConstructor(TypeCisco, NewParser)
}

const CiscoPatterns = "%{CISCO_TAGGED_SYSLOG},%{CISCOFW104001},%{CISCOFW104002},%{CISCOFW104003},%{CISCOFW104004}," +
	"%{CISCOFW105003},%{CISCOFW105004},%{CISCOFW105005},%{CISCOFW105008},%{CISCOFW105009}," +
	"%{CISCOFW106001},%{CISCOFW106006_106007_106010},%{CISCOFW106014},%{CISCOFW106015},%{CISCOFW106021},%{CISCOFW106023}," +
	"%{CISCOFW106100_2_3},%{CISCOFW106100}," +
	"%{CISCOFW304001},%{CISCOFW110002}," +
	"%{CISCOFW302010},%{CISCOFW302013_302014_302015_302016},%{CISCOFW302020_302021},%{CISCOFW305011}," +
	"%{CISCOFW313001_313004_313008},%{CISCOFW313005},%{CISCOFW321001}," +
	"%{CISCOFW402117},%{CISCOFW402119},%{CISCOFW419001},%{CISCOFW419002}," +
	"%{CISCOFW500004},%{CISCOFW602303_602304}," +
	"%{CISCOFW710001_710002_710003_710005_710006},%{CISCOFW713172},%{CISCOFW733100}"

type Parser struct {
	name       string
	grokParser parser.Parser
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	c[KeyGrokPatterns] = CiscoPatterns
	grokParser, err := grok.NewParser(c)
	if err != nil {
		return nil, err
	}

	return &Parser{
		grokParser: grokParser,
		name:       name,
	}, nil
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	return p.grokParser.Parse(lines)
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeCisco
}
