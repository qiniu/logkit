package mgr

import (
	"net/http"
	"os"

	"github.com/json-iterator/go"
	"github.com/qiniu/logkit/reader"
	"github.com/stretchr/testify/assert"
)

type respReaderRet struct {
	Code string `json:"code"`
	Data string `json:"data"`
}

// Rest 测试 端口容易冲突导致混淆，61xx
func readerAPITest(p *testParam) {
	t := p.t
	rs := p.rs

	var got1 respModeUsages
	url := "http://127.0.0.1" + rs.address + "/logkit/reader/usages"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got1); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, reader.ModeUsages, got1.Data)

	var got2 respModeKeyOptions
	url = "http://127.0.0.1" + rs.address + "/logkit/reader/options"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got2); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, reader.ModeKeyOptions, got2.Data)

	var got3 respReaderRet
	readerConfig := `{
		"log_path":"./readerAPITest/logdir",
		"meta_path":"./readerAPITest1/meta_req_csv",
		"mode":"dir",
		"read_from":"oldest",
		"ignore_hidden":"true"
	}`
	logfile := "./readerAPITest/logdir/log1"
	logdir := "./readerAPITest/logdir"
	defer func() {
		os.RemoveAll("./readerAPITest")
		os.RemoveAll("./readerAPITest1")
	}()

	if err := os.MkdirAll(logdir, 0777); err != nil {
		t.Error(err)
	}
	err = createFile(logfile, 20000000)
	if err != nil {
		t.Error(err)
	}

	url = "http://127.0.0.1" + rs.address + "/logkit/reader/read"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte(readerConfig))
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got3); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	expect := "abc\n"
	assert.Equal(t, expect, got3.Data)

	// Test reader/check with date transformer
	var got4 respDataMessage
	url = "http://127.0.0.1" + rs.address + "/logkit/reader/check"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte(readerConfig))
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got4); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, "", got4.Message)

	var got5 respModeUsages
	url = "http://127.0.0.1" + rs.address + "/logkit/reader/tooltips"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got5); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, reader.ModeToolTips, got5.Data)
}
