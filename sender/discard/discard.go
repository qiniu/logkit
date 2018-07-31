package discard

import (
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

var _ sender.SkipDeepCopySender = &Sender{}

// discard sender doing nothing
type Sender struct {
	name  string
	count int
}

func init() {
	sender.RegisterConstructor(sender.TypeDiscard, NewSender)
}

// discard sender 仅用于日志清理
func NewSender(c conf.MapConf) (sender.Sender, error) {
	name, _ := c.GetStringOr(sender.KeyName, "discardSender")
	s := &Sender{
		name:  name,
		count: 0,
	}
	return s, nil
}

//Name function will return the name as string
func (s *Sender) Name() string {
	return s.name
}

func (s *Sender) Send(d []Data) error {
	s.count++
	return nil
}

func (s *Sender) Close() error {
	return nil
}
func (s *Sender) SendCount() int {
	return s.count
}

func (_ *Sender) SkipDeepCopy() bool { return true }
