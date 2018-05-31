package samples

import (
	"strings"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
)

// 一个自定义parser的示例，将日志放到data中的log字段中
type CustomParser struct {
	// parser 的名字
	name string
	// 每行截断最大字符数
	maxLen int
}

func NewMyParser(c conf.MapConf) (parser.Parser, error) {
	// 获取parser配置中的name项，默认myparser
	name, _ := c.GetStringOr("name", "myparsername")
	// 获取parser配置中的max_len选项，默认1000
	maxLen, _ := c.GetIntOr("max_len", 1000)
	p := &CustomParser{
		name:   name,
		maxLen: maxLen,
	}
	return p, nil
}

func (p *CustomParser) Name() string {
	return p.name
}

func (p *CustomParser) Parse(lines []string) (datas []Data, err error) {
	for _, l := range lines {
		d := Data{}
		line := strings.TrimSpace(l)
		if len(line) > p.maxLen {
			line = line[:p.maxLen]
		}
		d["log"] = line
		datas = append(datas, d)
	}
	return datas, nil
}
