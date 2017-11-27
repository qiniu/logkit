package mgr

import (
	"fmt"
	"os"
	"testing"

	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

type respMetricUsage struct {
	Code string         `json:"code"`
	Data []utils.Option `json:"data"`
}

type respMetricKeys struct {
	Code string                      `json:"code"`
	Data map[string][]utils.KeyValue `json:"data"`
}

type respMetricOptions struct {
	Code string                    `json:"code"`
	Data map[string][]utils.Option `json:"data"`
}

func TestMetricAPI(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + DEFAULT_LOGKIT_REST_DIR
	defer os.RemoveAll(confdir)

	var conf ManagerConfig
	conf.BindHost = ":6261"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
	}()

	var got1 respMetricUsage

	resp, err := http.Get("http://127.0.0.1" + rs.address + "/logkit/metric/usages")
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
	assert.Equal(t, metric.GetMetricUsages(), got1.Data)

	var got2 respMetricOptions
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/metric/options")
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
	assert.Equal(t, metric.GetMetricOptions(), got2.Data)

	var got3 respMetricKeys
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/metric/keys")
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
	assert.Equal(t, metric.GetMetricTypeKey(), got3.Data)
}
