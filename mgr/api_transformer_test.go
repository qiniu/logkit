package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
	"bytes"
	_ "github.com/qiniu/logkit/transforms/date"
)

// Rest 测试 端口容易冲突导致混淆，63xx
func TestTransformerAPI(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + DEFAULT_LOGKIT_REST_DIR
	defer os.RemoveAll(confdir)

	var conf ManagerConfig
	conf.BindHost = ":6301"
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

	resp, err := http.Get("http://127.0.0.1" + rs.address + "/logkit/transformer/usages")
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
	assert.Equal(t, len(transforms.Transformers), len(got1))

	var got2 map[string][]utils.Option
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/transformer/options")
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
	assert.Equal(t, len(transforms.Transformers), len(got2))

	// Test transformer/transform with date transformer
	var got3 []map[string]string
	var dateTransformerConfig = `{
		"type":"date",
		"key":"ts",
		"offset":-1,
		"time_layout_before":"",
		"time_layout_after":"2006-01-02T15:04:05Z07:00",
		"sampleLog":"{\"ts\":\"2006-01-02 15:04:05.997\"}"
    }`
	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/transformer/transform", "application/json", bytes.NewReader([]byte(dateTransformerConfig)))
	content, _ = ioutil.ReadAll(resp.Body)
	if err = json.Unmarshal(content, &got3); err != nil {
		t.Error(err)
	}
	exp := []map[string]string{{"ts":"2006-01-02T14:04:05Z"}}
	assert.Equal(t, exp, got3)
}
