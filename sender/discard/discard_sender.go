package discard

import (
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

// Discard sender doing nothing

type DiscardSender struct {
	name  string
	count int
}

// NewDiscardSender 仅用于日志清理
func NewDiscardSender(c conf.MapConf) (sender.Sender, error) {
	name, _ := c.GetStringOr(sender.KeyName, "discardSender")
	s := &DiscardSender{
		name:  name,
		count: 0,
	}
	return s, nil
}

//Name function will return the name as string
func (s *DiscardSender) Name() string {
	return s.name
}

func (s *DiscardSender) Send(d []Data) error {
	s.count++
	return nil
}

func (s *DiscardSender) Close() error {
	return nil
}
func (s *DiscardSender) SendCount() int {
	return s.count
}

func init() {
	sender.Add(sender.TypeDiscard, NewDiscardSender)
}
