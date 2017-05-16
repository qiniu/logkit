package sender

import (
	"encoding/json"

	"github.com/qiniu/logkit/conf"
)

// mock sender is used for debug

type MockSender struct {
	name  string
	datas []Data
	count int
}

// NewMockSender 测试用sender
func NewMockSender(c conf.MapConf) (Sender, error) {
	name, _ := c.GetStringOr(KeyName, "mockSender")
	ms := &MockSender{
		name:  name,
		count: 0,
	}
	return ms, nil
}

//Name function will return the name and datas recieved as string
func (mock *MockSender) Name() string {
	raw, err := json.Marshal(mock.datas)
	if err != nil {
		raw = []byte(err.Error())
	}
	return mock.name + " " + string(raw)
}

func (mock *MockSender) Send(d []Data) error {
	mock.datas = append(mock.datas, d...)
	mock.count++
	return nil
}

func (mock *MockSender) Close() error {
	return nil
}
func (mock *MockSender) SendCount() int {
	return mock.count
}
