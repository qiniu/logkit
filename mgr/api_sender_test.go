package mgr

import (
	"net/http"

	"github.com/qiniu/logkit/sender/registry"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

// Rest 测试 端口容易冲突导致混淆，66xx
func senderAPITest(p *testParam) {
	t := p.t
	rs := p.rs

	var got1 respModeUsages
	url := "http://127.0.0.1" + rs.address + "/logkit/sender/usages"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got1); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, registry.ModeUsages, got1.Data)

	var got2 respModeKeyOptions
	url = "http://127.0.0.1" + rs.address + "/logkit/sender/options"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got2); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, registry.ModeKeyOptions, got2.Data)

	// Test sender/send with sender config
	var got3 respDataMessage
	senderConfig := `{
		"sampleLog": "[{\"a\": 1}]",
		"senders": [{
			"name":        "mock_sender",
			"sender_type": "mock"
		}]
	}`
	url = "http://127.0.0.1" + rs.address + "/logkit/sender/check"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte(senderConfig))
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got3); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, "", got3.Message)

	// Test sender/send with sender config
	var got4 respDataMessage
	url = "http://127.0.0.1" + rs.address + "/logkit/sender/send"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte(senderConfig))
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got4); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, "", got4.Message)
}
