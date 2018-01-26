package parser

import (
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

func NewEmptyParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	return &EmptyParser{
		name: name,
	}, nil
}

type EmptyParser struct {
	name string
}

func (p *EmptyParser) Name() string {
	return p.name
}

func (p *EmptyParser) Parse(lines []string) (datas []Data, err error) {
	return
}
