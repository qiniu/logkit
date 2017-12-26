package mgr

import (
	"encoding/json"
	"net/http"

	"github.com/qiniu/logkit/reader"
	"github.com/stretchr/testify/assert"
)

// Rest 测试 端口容易冲突导致混淆，61xx
func readerAPITest(p *testParam) {
	t := p.t
	rs := p.rs

	var got1 respModeUsages
	url := "http://127.0.0.1" + rs.address + "/logkit/reader/usages"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = json.Unmarshal(respBody, &got1); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, reader.ModeUsages, got1.Data)

	var got2 respModeKeyOptions
	url = "http://127.0.0.1" + rs.address + "/logkit/reader/options"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = json.Unmarshal(respBody, &got2); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, reader.ModeKeyOptions, got2.Data)
}
