package mgr

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/metric/system"
	"github.com/stretchr/testify/assert"
	"net/http"
)

func TestMetricRun(t *testing.T) {
	var testMetricConf = `{
		"name":"test1.csv",
		"batch_len": 1,
		"batch_size": 20,
		"batch_interval": 60,
		"batch_try_times": 3,
		"collect_interval": 10,
		"metric": [
			{
				"type": "disk"
			}
		],
		"senders":[{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": "./TestMetricRun/filesenderdata"
		}]
	}`
	dir := "TestMetricRun"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestMetricRun error mkdir %v %v", dir, err)
	}
	defer func() {
		defer os.RemoveAll("meta")
		defer os.RemoveAll(dir)
	}()
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + "/" + dir
	logconfs := dir + "/confs"
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("TestMetricRun error mkdir %v %v", logconfs, err)
	}
	err = ioutil.WriteFile(logconfs+"/test1.conf", []byte(testMetricConf), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":8346"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		dir + "/confs",
	}
	err = m.Watch(confs)
	if err != nil {
		t.Error(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
	}()
	time.Sleep(5 * time.Second)

	// 必须要引入 system 以便执行其中的 init
	log.Println(system.TypeMetricCpu)

	resp, err := http.Get("http://localhost:8346/logkit/status")
	assert.NoError(t, err)
	allStData, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus := make(map[string]RunnerStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err)
	if err != nil {
		t.Error(err)
	}
	log.Println(allStatus)
	//exp := map[string]RunnerStatus{
	//	"test1.csv": {
	//		Name:             "test1.csv",
	//		Logpath:          "",
	//		ReadDataCount:    4,
	//		ReadDataSize:     29,
	//		ReadSpeedTrend:   SpeedUp,
	//		ReadSpeedTrendKb: SpeedStable,
	//		Lag: RunnerLag{
	//			Size:  0,
	//			Files: 0,
	//		},
	//		SenderStats: map[string]utils.StatsInfo{
	//			"file_sender": {
	//				Errors:  0,
	//				Success: 4,
	//				Trend:   SpeedUp,
	//			},
	//		},
	//	},
	//}

	//assert.Equal(t, exp, rss, out.String())
}
