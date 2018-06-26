package mock

import (
	"sync"

	"github.com/json-iterator/go"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

// mock sender is used for debug
type Sender struct {
	name  string
	Datas []Data
	count int
	mux   sync.Mutex
}

func init() {
	sender.RegisterConstructor(sender.TypeMock, NewSender)
}

// NewMockSender 测试用sender
func NewSender(c conf.MapConf) (sender.Sender, error) {
	name, _ := c.GetStringOr(sender.KeyName, "mockSender")
	ms := &Sender{
		name:  name,
		count: 0,
		mux:   sync.Mutex{},
	}
	return ms, nil
}

//Name function will return the name and datas recieved as string
func (mock *Sender) Name() string {
	mock.mux.Lock()
	defer mock.mux.Unlock()
	raw, err := jsoniter.Marshal(mock.Datas)
	if err != nil {
		raw = []byte(err.Error())
	}
	return mock.name + " " + string(raw)
}

func (mock *Sender) Send(d []Data) error {
	mock.mux.Lock()
	defer mock.mux.Unlock()
	mock.Datas = append(mock.Datas, d...)
	mock.count++
	return nil
}

func (mock *Sender) Close() error {
	return nil
}
func (mock *Sender) SendCount() int {
	mock.mux.Lock()
	defer mock.mux.Unlock()
	return mock.count
}
