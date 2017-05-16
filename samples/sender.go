package samples

import (
	"fmt"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
)

// CustomSender 仅作为示例，什么都不做，只是把数据打印出来而已
type CustomSender struct {
	name   string
	prefix string
}

func NewMySender(c conf.MapConf) (sender.Sender, error) {
	name, _ := c.GetStringOr("name", "my_sender_name")
	prefix, _ := c.GetStringOr("prefix", "")
	return &CustomSender{
		name:   name,
		prefix: prefix,
	}, nil
}

func (c *CustomSender) Name() string {
	return c.name
}

func (c *CustomSender) Send(datas []sender.Data) error {
	for _, d := range datas {
		var line string
		for k, v := range d {
			line += fmt.Sprintf("%v=%v ", k, v)
		}
		log.Info(c.prefix, line)
	}
	return nil
}
func (c *CustomSender) Close() error {
	return nil
}
