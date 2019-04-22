package mgr

import (
	"net/http"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	conf2 "github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

type respParserRet struct {
	Code string       `json:"code"`
	Data PostParseRet `json:"data"`
}

// Rest 测试 端口容易冲突导致混淆，62xx
func parserParseTest(p *testParam) {
	t := p.t
	rs := p.rs

	// raw
	rawConf := conf2.MapConf{}
	rawConf[KeySampleLog] = SampleLogs[TypeRaw]
	rawConf[KeyParserType] = TypeRaw
	rawpst, err := jsoniter.Marshal(rawConf)
	assert.NoError(t, err)
	url := "http://127.0.0.1" + rs.address + "/logkit/parser/parse"
	respCode, respBody, err := makeRequest(url, http.MethodPost, rawpst)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)

	var got1 respParserRet
	err = jsoniter.Unmarshal(respBody, &got1)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, 1, len(got1.Data.SamplePoints))

	// json
	var got2 respParserRet
	jsonConf := conf2.MapConf{}
	jsonConf[KeySampleLog] = SampleLogs[TypeJSON]
	jsonConf[KeyParserType] = TypeJSON
	rawpst, err = jsoniter.Marshal(jsonConf)
	assert.NoError(t, err)
	url = "http://127.0.0.1" + rs.address + "/logkit/parser/parse"
	respCode, respBody, err = makeRequest(url, http.MethodPost, rawpst)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	err = jsoniter.Unmarshal(respBody, &got2)
	if err != nil {
		t.Error(err)
	}
	exp2 := Data{
		"a": "b",
		"c": 1.0,
		"d": 1.1,
	}
	assert.Equal(t, exp2, got2.Data.SamplePoints[0])

	// grok
	grokConf := conf2.MapConf{}
	var got3 respParserRet
	grokConf[KeySampleLog] = SampleLogs[TypeGrok]
	grokConf[KeyParserType] = TypeGrok
	grokConf[KeyGrokPatterns] = "%{COMMON_LOG_FORMAT}"
	rawpst, err = jsoniter.Marshal(grokConf)
	assert.NoError(t, err)
	url = "http://127.0.0.1" + rs.address + "/logkit/parser/parse"
	respCode, respBody, err = makeRequest(url, http.MethodPost, rawpst)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	err = jsoniter.Unmarshal(respBody, &got3)
	if err != nil {
		t.Error(err)
	}

	exp3 := Data{
		"ts":           "2000-10-10T13:55:36-07:00",
		"verb":         "GET",
		"http_version": 1.0,
		"request":      "/apache_pb.gif",
		"ident":        "user-identifier",
		"resp_bytes":   2326.0,
		"resp_code":    "200",
		"auth":         "frank", "client_ip": "127.0.0.1"}

	assert.Equal(t, exp3, got3.Data.SamplePoints[0])

	// raw
	rawConf = conf2.MapConf{}
	rawConf[KeySampleLog] = SampleLogs[TypeRaw]
	rawConf[KeyParserType] = TypeRaw
	rawpst, err = jsoniter.Marshal(rawConf)
	assert.NoError(t, err)
	url = "http://127.0.0.1" + rs.address + "/logkit/parser/check"
	respCode, respBody, err = makeRequest(url, http.MethodPost, rawpst)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
}

func parserAPITest(p *testParam) {
	t := p.t
	rs := p.rs

	var got1 respModeUsages
	url := "http://127.0.0.1" + rs.address + "/logkit/parser/usages"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got1); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, ModeUsages, got1.Data)

	var got2 respModeKeyOptions
	url = "http://127.0.0.1" + rs.address + "/logkit/parser/options"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got2); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, ModeKeyOptions, got2.Data)

	var got3 respSampleLogs
	url = "http://127.0.0.1" + rs.address + "/logkit/parser/samplelogs"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got3); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, SampleLogs, got3.Data)

	var got4 respModeUsages
	url = "http://127.0.0.1" + rs.address + "/logkit/parser/tooltips"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got4); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, ModeToolTips, got4.Data)
}
