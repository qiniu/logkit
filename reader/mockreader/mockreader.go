package mockreader

import (
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.Reader     = &mockReader{}
	_ reader.DataReader = &mockReader{}
)

var TypeMockReader = "mockreader"

//mock一个reader用于测试readdata()
type mockReader struct {
	invalidCnt int
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (ret reader.Reader, err error) {
	return &mockReader{
		invalidCnt: 0,
	}, nil
}

func (m *mockReader) Name() string {
	return ""
}

func (m *mockReader) SetMode(mode string, v interface{}) error {
	return nil
}

func (m *mockReader) Source() string {
	return ""
}

func (m *mockReader) ReadLine() (string, error) {
	return "", nil
}

func (m *mockReader) SyncMeta() {

}

func (m *mockReader) Close() error {
	return nil
}

func (m *mockReader) ReadData() (Data, int64, error) {
	if m.invalidCnt < 10 {
		m.invalidCnt++
		return nil, int64(0), nil
	}
	m.invalidCnt++
	return Data{
		"logkit": "logkit",
	}, int64(1), nil
}

func init() {
	reader.RegisterConstructor(TypeMockReader, NewReader)
}
