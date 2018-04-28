package mock

import (
	"sync"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender/common"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
)

// mock sender is used for debug

type MockSender struct {
	name  string
	Datas []Data
	count int
	mux   sync.Mutex
}

// NewMockSender 测试用sender
func NewMockSender(c conf.MapConf) (common.Sender, error) {
	name, _ := c.GetStringOr(KeyName, "mockSender")
	ms := &MockSender{
		name:  name,
		count: 0,
		mux:   sync.Mutex{},
	}
	return ms, nil
}

//Name function will return the name and datas recieved as string
func (mock *MockSender) Name() string {
	mock.mux.Lock()
	defer mock.mux.Unlock()
	raw, err := jsoniter.Marshal(mock.Datas)
	if err != nil {
		raw = []byte(err.Error())
	}
	return mock.name + " " + string(raw)
}

func (mock *MockSender) Send(d []Data) error {
	mock.mux.Lock()
	defer mock.mux.Unlock()
	mock.Datas = append(mock.Datas, d...)
	mock.count++
	return nil
}

func (mock *MockSender) Close() error {
	return nil
}
func (mock *MockSender) SendCount() int {
	mock.mux.Lock()
	defer mock.mux.Unlock()
	return mock.count
}
