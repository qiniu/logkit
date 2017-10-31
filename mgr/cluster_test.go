package mgr

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"path/filepath"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

func TestClusterApi(t *testing.T) {
	dir := "TestClusterApi"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestClusterApi error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	var master_conf, slave_conf ManagerConfig
	master_conf.RestDir = confdir
	master_conf.BindHost = ":6349" //master
	master_conf.Cluster.Enable = true
	master_conf.Cluster.IsMaster = true

	master, err := NewManager(master_conf)
	assert.NoError(t, err)
	master_rs := NewRestService(master, echo.New())

	slave_conf.RestDir = confdir
	slave_conf.BindHost = ":6350" //slave
	slave_conf.Cluster.Enable = true
	slave_conf.Cluster.IsMaster = false
	slave_conf.Cluster.MasterUrl = []string{master_rs.cluster.myaddress}

	slave, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs := NewRestService(slave, echo.New())

	defer func() {
		slave_rs.Stop()
		master_rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	err = Register([]string{master_rs.cluster.myaddress}, slave_rs.cluster.myaddress, "test")
	assert.NoError(t, err)
	master_rs.cluster.slaves[0].LastTouch = time.Time{}
	assert.Equal(t, []Slave{{Url: slave_rs.cluster.myaddress, Tag: "test", Status: StatusOK}}, master_rs.cluster.slaves)
	req := TagReq{Tag: "test_changed"}
	marshaled, err := json.Marshal(req)
	assert.NoError(t, err)
	resp, err := http.Post(slave_rs.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, []Slave{{Url: slave_rs.cluster.myaddress, Tag: "test_changed", Status: StatusOK, LastTouch: master_rs.cluster.slaves[0].LastTouch}}, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6351"
	slave2, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs2 := NewRestService(slave2, echo.New())
	defer slave_rs2.Stop()
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs2.cluster.myaddress, "test")
	assert.NoError(t, err)

	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/slaves?tag=test_changed")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	var slaves []Slave
	err = json.Unmarshal(content, &slaves)
	assert.NoError(t, err)
	for i := range slaves {
		slaves[i].LastTouch = time.Time{}
	}
	assert.Equal(t, []Slave{{Url: slave_rs.cluster.myaddress, Tag: "test_changed", Status: StatusOK}}, slaves, string(content))
	resp.Body.Close()

	var testClusterApiConf = `{
    "name":"test1",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestClusterApi/logdir",
        "meta_path":"./TestClusterApi/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "csv",
		"csv_schema":   "logtype string, xx long",
		"csv_splitter": " "
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./TestClusterApi/filesenderdata"
    }]
}`

	if err := os.Mkdir(logpath, 0755); err != nil {
		assert.NoError(t, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		assert.NoError(t, err)
	}
	log1 := `hello 123
	xx 1`
	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		assert.NoError(t, err)
	}
	time.Sleep(1 * time.Second)

	_, err = http.Post(slave_rs.cluster.myaddress+"/logkit/configs/"+"test1", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test_changed")
	assert.NoError(t, err)
	allstdata, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allstatus := make(map[string]ClusterStatus)
	err = json.Unmarshal(allstdata, &allstatus)
	assert.NoError(t, err, string(allstdata))
	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}

	sts := map[string]RunnerStatus{
		"test1": {
			Name:             "test1",
			Logpath:          rp,
			ReadDataCount:    2,
			ReadDataSize:     15,
			ReadSpeedTrend:   SpeedUp,
			ReadSpeedTrendKb: SpeedUp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  0,
				Success: 2,
				Trend:   SpeedUp,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 2,
					Trend:   SpeedUp,
				},
			},
			Tag: "test_changed",
			Url: slave_rs.cluster.myaddress,
		},
	}

	gotstatus := make(map[string]ClusterStatus)
	gotstatus[slave_rs.cluster.myaddress] = ClusterStatus{
		Status: sts,
		Tag:    "test_changed",
	}

	rs, ok := allstatus[slave_rs.cluster.myaddress]
	assert.Equal(t, true, ok, string(allstdata))
	v, ok := rs.Status["test1"]
	assert.Equal(t, true, ok, rs.Status)
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs := v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rs.Status["test1"] = v
	allstatus[slave_rs.cluster.myaddress] = rs

	assert.Equal(t, gotstatus, allstatus)
}
