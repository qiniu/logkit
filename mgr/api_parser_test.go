package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"bytes"

	"github.com/labstack/echo"
	conf2 "github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

func TestParserParse(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + DEFAULT_LOGKIT_REST_DIR
	defer os.RemoveAll(confdir)

	var conf ManagerConfig
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
	}()

	// raw
	rawConf := conf2.MapConf{}
	rawConf[KeySampleLog] = parser.SampleLogs[parser.TypeRaw]
	rawConf[parser.KeyParserType] = parser.TypeRaw
	rawpst, err := json.Marshal(rawConf)
	assert.NoError(t, err)
	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/parser/parse", TESTContentApplictionJson, bytes.NewReader(rawpst))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, 200, resp.StatusCode, string(content))
	var got1 PostParseRet
	err = json.Unmarshal(content, &got1)
	assert.NoError(t, err, string(content))
	assert.Equal(t, 4, len(got1.SamplePoints))

	// json
	var got2 PostParseRet
	jsonConf := conf2.MapConf{}
	jsonConf[KeySampleLog] = parser.SampleLogs[parser.TypeJson]
	jsonConf[parser.KeyParserType] = parser.TypeJson
	rawpst, err = json.Marshal(jsonConf)
	assert.NoError(t, err)
	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/parser/parse", TESTContentApplictionJson, bytes.NewReader(rawpst))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, 200, resp.StatusCode, string(content))
	err = json.Unmarshal(content, &got2)
	if err != nil {
		t.Error(err)
	}
	exp2 := sender.Data{
		"a": "b",
		"c": 1.0,
		"d": 1.1,
	}
	assert.Equal(t, exp2, got2.SamplePoints[0])

	// grok
	grokConf := conf2.MapConf{}
	var got3 PostParseRet
	grokConf[KeySampleLog] = parser.SampleLogs[parser.TypeGrok]
	grokConf[parser.KeyParserType] = parser.TypeGrok
	grokConf[parser.KeyGrokPatterns] = "%{COMMON_LOG_FORMAT}"
	rawpst, err = json.Marshal(grokConf)
	assert.NoError(t, err)
	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/parser/parse", TESTContentApplictionJson, bytes.NewReader(rawpst))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, 200, resp.StatusCode, string(content))
	err = json.Unmarshal(content, &got3)
	if err != nil {
		t.Error(err)
	}

	exp3 := sender.Data{
		"ts":           "2000-10-10T13:55:36-07:00",
		"verb":         "GET",
		"http_version": 1.0,
		"request":      "/apache_pb.gif",
		"ident":        "user-identifier",
		"resp_bytes":   2326.0,
		"resp_code":    "200",
		"auth":         "frank", "client_ip": "127.0.0.1"}

	assert.Equal(t, exp3, got3.SamplePoints[0])
}

func TestParserAPI(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + DEFAULT_LOGKIT_REST_DIR
	defer os.RemoveAll(confdir)

	var conf ManagerConfig
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
	}()

	var got1 []utils.KeyValue

	resp, err := http.Get("http://127.0.0.1" + rs.address + "/logkit/parser/usages")
	if err != nil {
		t.Error(err)
	}
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &got1)
	if err != nil {
		fmt.Println(string(content))
		t.Error(err)
	}
	assert.Equal(t, parser.ModeUsages, got1)

	var got2 map[string]map[string]utils.Option
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/parser/options")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &got2)
	if err != nil {
		fmt.Println(string(content))
		t.Error(err)
	}
	assert.Equal(t, parser.ModeKeyOptions, got2)

	var got3 map[string]string
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/parser/samplelogs")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &got3)
	if err != nil {
		fmt.Println(string(content))
		t.Error(err)
	}
	assert.Equal(t, parser.SampleLogs, got3)
}
