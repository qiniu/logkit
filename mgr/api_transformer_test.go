package mgr

import (
	"encoding/json"
	"net/http"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	_ "github.com/qiniu/logkit/transforms/date"
	"github.com/stretchr/testify/assert"
)

type respTransformerRet struct {
	Code string        `json:"code"`
	Data []sender.Data `json:"data"`
}

// Rest 测试 端口容易冲突导致混淆，63xx
func transformerAPITest(p *testParam) {
	t := p.t
	rs := p.rs

	var got1 respModeUsages
	url := "http://127.0.0.1" + rs.address + "/logkit/transformer/usages"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = json.Unmarshal(respBody, &got1); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, len(transforms.Transformers), len(got1.Data))

	var got2 respModeKeyOptions
	url = "http://127.0.0.1" + rs.address + "/logkit/transformer/options"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = json.Unmarshal(respBody, &got2); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, len(transforms.Transformers), len(got2.Data))

	// Test transformer/transform with date transformer
	var got3 respTransformerRet
	var dateTransformerConfig = `{
		"type":"date",
		"key":"ts",
		"offset":-1,
		"time_layout_before":"",
		"time_layout_after":"2006-01-02T15:04:05Z07:00",
		"sampleLog":"{\"ts\":\"2006-01-02 15:04:05.997\"}"
    }`
	url = "http://127.0.0.1" + rs.address + "/logkit/transformer/transform"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte(dateTransformerConfig))
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = json.Unmarshal(respBody, &got3); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	exp := []sender.Data{{"ts": "2006-01-02T14:04:05Z"}}
	assert.Equal(t, exp, got3.Data)
}