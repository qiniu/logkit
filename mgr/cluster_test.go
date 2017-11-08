package mgr

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestClusterUpdate(t *testing.T) {
	dir := "TestClusterUpdate"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestClusterUpdate error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	var master_conf, slave_conf ManagerConfig
	master_conf.RestDir = confdir
	master_conf.BindHost = ":6352" //master
	master_conf.Cluster.Enable = true
	master_conf.Cluster.IsMaster = true

	master, err := NewManager(master_conf)
	assert.NoError(t, err)
	master_rs := NewRestService(master, echo.New())

	slaves := make([]Slave, 0)

	slave_conf.RestDir = confdir
	slave_conf.BindHost = ":6353" //slave
	slave_conf.Cluster.Enable = true
	slave_conf.Cluster.IsMaster = false
	slave_conf.Cluster.MasterUrl = []string{master_rs.cluster.myaddress}
	slave, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs := NewRestService(slave, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs.cluster.myaddress, "test")
	assert.NoError(t, err)
	req := TagReq{Tag: "test"}
	marshaled, err := json.Marshal(req)
	assert.NoError(t, err)
	resp, err := http.Post(slave_rs.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[0].LastTouch})
	content, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6354" // slave2
	slave2, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs2 := NewRestService(slave2, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs2.cluster.myaddress, "test")
	assert.NoError(t, err)
	resp, err = http.Post(slave_rs2.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs2.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[1].LastTouch})
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6355" // slave3
	slave3, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs3 := NewRestService(slave3, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs3.cluster.myaddress, "test_change")
	assert.NoError(t, err)
	req = TagReq{Tag: "test_change"}
	marshaled, err = json.Marshal(req)
	resp, err = http.Post(slave_rs3.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, []Slave{{Url: slave_rs3.cluster.myaddress, Tag: "test_change", Status: StatusOK, LastTouch: master_rs.cluster.slaves[2].LastTouch}}, []Slave{master_rs.cluster.slaves[2]}, string(content))
	resp.Body.Close()

	defer func() {
		slave_rs.Stop()
		master_rs.Stop()
		slave_rs2.Stop()
		slave_rs3.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	var testClusterApiConf = `{
    "name":"test2",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestClusterUpdate/logdir",
        "meta_path":"./TestClusterUpdate/meta_mock_csv",
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
		"file_send_path": "./TestClusterUpdate/filesenderdata"
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

	// 测试增加 runner
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test2?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	resp, err = http.Get(master_rs.cluster.myaddress + "/logkit/cluster/status?tag=test")
	assert.NoError(t, err)
	allStData, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus := make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}

	sts := map[string]RunnerStatus{
		"test2": {
			Name:             "test2",
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
			Tag: "test",
			Url: slave_rs.cluster.myaddress,
		},
	}

	sts2 := map[string]RunnerStatus{
		"test2": {
			Name:             "test2",
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
			Tag: "test",
			Url: slave_rs2.cluster.myaddress,
		},
	}

	gotStatus := make(map[string]ClusterStatus)
	gotStatus[slave_rs.cluster.myaddress] = ClusterStatus{
		Status: sts,
		Tag:    "test",
	}
	gotStatus[slave_rs2.cluster.myaddress] = ClusterStatus{
		Status: sts2,
		Tag:    "test",
	}

	rs, ok := allStatus[slave_rs.cluster.myaddress]
	assert.Equal(t, true, ok, string(allStData))
	v, ok := rs.Status["test2"]
	assert.Equal(t, true, ok, rs.Status)
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs := v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rs.Status["test2"] = v
	allStatus[slave_rs.cluster.myaddress] = rs

	rs2, ok := allStatus[slave_rs2.cluster.myaddress]
	assert.Equal(t, true, ok, string(allStData))
	v2, ok := rs2.Status["test2"]
	assert.Equal(t, true, ok, rs.Status)
	v2.Elaspedtime = 0
	v2.ReadSpeed = 0
	v2.ReadSpeedKB = 0
	v2.ParserStats.Speed = 0
	fs2 := v.SenderStats["file_sender"]
	fs2.Speed = 0
	v2.SenderStats["file_sender"] = fs2
	rs2.Status["test2"] = v2
	allStatus[slave_rs2.cluster.myaddress] = rs2
	assert.Equal(t, len(gotStatus), len(allStatus))
	for key, val := range allStatus {
		ex, ok := gotStatus[key]
		assert.Equal(t, ex, val)
		assert.Equal(t, true, ok)
	}

	// 测试 update runner
	var testClusterApiConf1 = `{
    "name":"test2",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestClusterUpdate/logdir/log1",
        "meta_path":"./TestClusterUpdate/meta_mock_csv",
        "mode":"file",
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
		"file_send_path": "./TestClusterUpdate/filesenderdata"
    }]
}`
	request, err := http.NewRequest(http.MethodPut, master_rs.cluster.myaddress+"/logkit/cluster/configs/test2?tag=test", bytes.NewReader([]byte(testClusterApiConf1)))
	request.Header.Set(ContentType, ApplicationJson)
	resp, err = http.DefaultClient.Do(request)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(5 * time.Second)

	// 上述配置文件改变了 logpath, 所以下面验证 status 中的 logpath 是否正确
	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test")
	assert.NoError(t, err)
	allStData, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus = make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	logPath := allStatus[slave_rs.cluster.myaddress].Status["test2"].Logpath
	assert.Equal(t, confdir+"/logdir/log1", logPath)
	logPath = allStatus[slave_rs2.cluster.myaddress].Status["test2"].Logpath
	assert.Equal(t, confdir+"/logdir/log1", logPath)
}

func TestClusterStartStop(t *testing.T) {
	dir := "TestClusterStartStop"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestClusterStartStop error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	var master_conf, slave_conf ManagerConfig
	master_conf.RestDir = confdir
	master_conf.BindHost = ":6356" //master
	master_conf.Cluster.Enable = true
	master_conf.Cluster.IsMaster = true

	master, err := NewManager(master_conf)
	assert.NoError(t, err)
	master_rs := NewRestService(master, echo.New())

	slaves := make([]Slave, 0)

	slave_conf.RestDir = confdir
	slave_conf.BindHost = ":6357" //slave
	slave_conf.Cluster.Enable = true
	slave_conf.Cluster.IsMaster = false
	slave_conf.Cluster.MasterUrl = []string{master_rs.cluster.myaddress}
	slave, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs := NewRestService(slave, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs.cluster.myaddress, "test")
	assert.NoError(t, err)
	req := TagReq{Tag: "test"}
	marshaled, err := json.Marshal(req)
	assert.NoError(t, err)
	resp, err := http.Post(slave_rs.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[0].LastTouch})
	content, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6358" // slave2
	slave2, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs2 := NewRestService(slave2, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs2.cluster.myaddress, "test")
	assert.NoError(t, err)
	resp, err = http.Post(slave_rs2.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs2.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[1].LastTouch})
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6359" // slave3
	slave3, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs3 := NewRestService(slave3, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs3.cluster.myaddress, "test_change")
	assert.NoError(t, err)
	req = TagReq{Tag: "test_change"}
	marshaled, err = json.Marshal(req)
	resp, err = http.Post(slave_rs3.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, []Slave{{Url: slave_rs3.cluster.myaddress, Tag: "test_change", Status: StatusOK, LastTouch: master_rs.cluster.slaves[2].LastTouch}}, []Slave{master_rs.cluster.slaves[2]}, string(content))
	resp.Body.Close()

	defer func() {
		slave_rs.Stop()
		master_rs.Stop()
		slave_rs2.Stop()
		slave_rs3.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	var testClusterApiConf = `{
    "name":"test3",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestClusterStartStop/logdir",
        "meta_path":"./TestClusterStartStop/meta_mock_csv",
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
		"file_send_path": "./TestClusterStartStop/filesenderdata"
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

	// 增加 runner
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test3?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test3?tag=test_change", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	// 测试 stop runner
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test3/stop?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	// 停止后，tag == 'test' 的 status 返回为空
	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test")
	assert.NoError(t, err)
	allStData, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus := make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	assert.Equal(t, map[string]ClusterStatus{
		slave_rs.cluster.myaddress:  ClusterStatus{Status: map[string]RunnerStatus{}, Tag: "test"},
		slave_rs2.cluster.myaddress: ClusterStatus{Status: map[string]RunnerStatus{}, Tag: "test"},
	}, allStatus)

	// 再次停止会有错误
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test3/stop?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.NotEqual(t, resp.StatusCode, http.StatusOK)

	// tag == 'test_changed' 的 status 依然存在
	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test_change")
	assert.NoError(t, err)
	allStData, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus = make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	rs3_CS, ok := allStatus[slave_rs3.cluster.myaddress]
	assert.Equal(t, true, ok)
	_, ok = rs3_CS.Status["test3"]
	assert.Equal(t, true, ok)

	// 测试 runner 启动
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test3/start?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	// status 状态恢复
	resp, err = http.Get(slave_rs2.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test")
	assert.NoError(t, err)
	allStData, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus = make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}

	readDataSize := allStatus[slave_rs.cluster.myaddress].Status["test3"].ReadDataSize
	readDataSize2 := allStatus[slave_rs2.cluster.myaddress].Status["test3"].ReadDataSize
	sts := map[string]RunnerStatus{
		"test3": {
			Name:             "test3",
			Logpath:          rp,
			ReadDataCount:    2,
			ReadDataSize:     readDataSize,
			ReadSpeedTrend:   SpeedStable,
			ReadSpeedTrendKb: SpeedStable,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  0,
				Success: 2,
				Trend:   SpeedStable,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 2,
					Trend:   SpeedStable,
				},
			},
			Tag: "test",
			Url: slave_rs.cluster.myaddress,
		},
	}

	sts2 := map[string]RunnerStatus{
		"test3": {
			Name:             "test3",
			Logpath:          rp,
			ReadDataCount:    2,
			ReadDataSize:     readDataSize2,
			ReadSpeedTrend:   SpeedStable,
			ReadSpeedTrendKb: SpeedStable,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  0,
				Success: 2,
				Trend:   SpeedStable,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 2,
					Trend:   SpeedStable,
				},
			},
			Tag: "test",
			Url: slave_rs2.cluster.myaddress,
		},
	}

	gotStatus := make(map[string]ClusterStatus)
	gotStatus[slave_rs.cluster.myaddress] = ClusterStatus{
		Status: sts,
		Tag:    "test",
	}
	gotStatus[slave_rs2.cluster.myaddress] = ClusterStatus{
		Status: sts2,
		Tag:    "test",
	}

	rs, ok := allStatus[slave_rs.cluster.myaddress]
	assert.Equal(t, true, ok, string(allStData))
	v, ok := rs.Status["test3"]
	assert.Equal(t, true, ok, rs.Status)
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs := v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rs.Status["test3"] = v
	allStatus[slave_rs.cluster.myaddress] = rs

	rs2, ok := allStatus[slave_rs2.cluster.myaddress]
	assert.Equal(t, true, ok, string(allStData))
	v2, ok := rs2.Status["test3"]
	assert.Equal(t, true, ok, rs.Status)
	v2.Elaspedtime = 0
	v2.ReadSpeed = 0
	v2.ReadSpeedKB = 0
	v2.ParserStats.Speed = 0
	fs2 := v2.SenderStats["file_sender"]
	fs2.Speed = 0
	v2.SenderStats["file_sender"] = fs2
	rs2.Status["test3"] = v2
	allStatus[slave_rs2.cluster.myaddress] = rs2
	assert.Equal(t, len(gotStatus), len(allStatus))
	for key, val := range allStatus {
		ex, ok := gotStatus[key]
		assert.Equal(t, ex, val)
		assert.Equal(t, true, ok)
	}
	// 再次开启，会报错
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test3/start?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.NotEqual(t, resp.StatusCode, http.StatusOK)
}

func TestClusterResetDelete(t *testing.T) {
	dir := "TestClusterResetDelete"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestClusterResetDelete mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	filesenderdata := dir + "/filesenderdata"
	var master_conf, slave_conf ManagerConfig
	master_conf.RestDir = confdir
	master_conf.BindHost = ":6360" //master
	master_conf.Cluster.Enable = true
	master_conf.Cluster.IsMaster = true

	master, err := NewManager(master_conf)
	assert.NoError(t, err)
	master_rs := NewRestService(master, echo.New())

	slaves := make([]Slave, 0)

	slave_conf.RestDir = confdir
	slave_conf.BindHost = ":6361" //slave
	slave_conf.Cluster.Enable = true
	slave_conf.Cluster.IsMaster = false
	slave_conf.Cluster.MasterUrl = []string{master_rs.cluster.myaddress}
	slave, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs := NewRestService(slave, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs.cluster.myaddress, "test")
	assert.NoError(t, err)
	req := TagReq{Tag: "test"}
	marshaled, err := json.Marshal(req)
	assert.NoError(t, err)
	resp, err := http.Post(slave_rs.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[0].LastTouch})
	content, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6362" // slave2
	slave2, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs2 := NewRestService(slave2, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs2.cluster.myaddress, "test")
	assert.NoError(t, err)
	resp, err = http.Post(slave_rs2.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs2.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[1].LastTouch})
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6363" // slave3
	slave3, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs3 := NewRestService(slave3, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs3.cluster.myaddress, "test_change")
	assert.NoError(t, err)
	req = TagReq{Tag: "test_change"}
	marshaled, err = json.Marshal(req)
	resp, err = http.Post(slave_rs3.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, []Slave{{Url: slave_rs3.cluster.myaddress, Tag: "test_change", Status: StatusOK, LastTouch: master_rs.cluster.slaves[2].LastTouch}}, []Slave{master_rs.cluster.slaves[2]}, string(content))
	resp.Body.Close()

	defer func() {
		slave_rs.Stop()
		master_rs.Stop()
		slave_rs2.Stop()
		slave_rs3.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	var testClusterApiConf = `{
    "name":"test4",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestClusterResetDelete/logdir",
        "meta_path":"./TestClusterResetDelete/meta_mock_csv",
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
		"file_send_path": "./TestClusterResetDelete/filesenderdata"
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

	// 增加 runner
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test4?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)

	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test4?tag=test_change", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(10 * time.Second)

	// 读取日志发送目的文件，记录日志条数
	dataLine := 0
	f, err := os.Open(filesenderdata)
	assert.NoError(t, err)
	br := bufio.NewReader(f)
	for {
		_, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		dataLine++
	}
	f.Close()

	// reset tag == 'test' 的 slave
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test4/reset?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(10 * time.Second)

	// 重置之后, 日志发送目的文件中的日志条数应该增加
	dataLine1 := 0
	f, err = os.Open(filesenderdata)
	assert.NoError(t, err)
	br = bufio.NewReader(f)
	for {
		_, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		dataLine1++
	}
	f.Close()
	assert.Equal(t, dataLine1, dataLine+4)

	// 测试删除 runner
	request, err := http.NewRequest(http.MethodDelete, master_rs.cluster.myaddress+"/logkit/cluster/configs/test4?tag=test", nil)
	assert.NoError(t, err)
	resp, err = http.DefaultClient.Do(request)
	// 此处会有错误返回, 原因是两个 slave 的 conf 放在同一个地方，导致第二个slave删除配置文件的时候返回文件不存在的错误，所以此处不再判断 response code
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)

	// 删除后，status 返回为空
	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test")
	assert.NoError(t, err)
	allStData, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus := make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	assert.Equal(t, map[string]ClusterStatus{
		slave_rs.cluster.myaddress:  ClusterStatus{Status: map[string]RunnerStatus{}, Tag: "test"},
		slave_rs2.cluster.myaddress: ClusterStatus{Status: map[string]RunnerStatus{}, Tag: "test"},
	}, allStatus)

	// 再次删除会有错误
	resp, err = http.DefaultClient.Do(request)
	assert.NotEqual(t, resp.StatusCode, http.StatusOK)

	// tag == 'test_changed' 的 status 依然存在
	resp, err = http.Get(slave_rs.cluster.MasterUrl[0] + "/logkit/cluster/status?tag=test_change")
	assert.NoError(t, err)
	allStData, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allStatus = make(map[string]ClusterStatus)
	err = json.Unmarshal(allStData, &allStatus)
	assert.NoError(t, err, string(allStData))
	rs3_CS, ok := allStatus[slave_rs3.cluster.myaddress]
	assert.Equal(t, true, ok)
	_, ok = rs3_CS.Status["test4"]
	assert.Equal(t, true, ok)
}

func TestSalveConfigs(t *testing.T) {
	dir := "TestSalveConfigs"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestSlaveConfigs mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	var master_conf, slave_conf ManagerConfig
	master_conf.RestDir = confdir
	master_conf.BindHost = ":6364" //master
	master_conf.Cluster.Enable = true
	master_conf.Cluster.IsMaster = true

	master, err := NewManager(master_conf)
	assert.NoError(t, err)
	master_rs := NewRestService(master, echo.New())

	slaves := make([]Slave, 0)

	slave_conf.RestDir = confdir
	slave_conf.BindHost = ":6365" //slave
	slave_conf.Cluster.Enable = true
	slave_conf.Cluster.IsMaster = false
	slave_conf.Cluster.MasterUrl = []string{master_rs.cluster.myaddress}
	slave, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs := NewRestService(slave, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs.cluster.myaddress, "test")
	assert.NoError(t, err)
	req := TagReq{Tag: "test"}
	marshaled, err := json.Marshal(req)
	assert.NoError(t, err)
	resp, err := http.Post(slave_rs.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[0].LastTouch})
	content, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6366" // slave2
	slave2, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs2 := NewRestService(slave2, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs2.cluster.myaddress, "test")
	assert.NoError(t, err)
	resp, err = http.Post(slave_rs2.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	slaves = append(slaves, Slave{Url: slave_rs2.cluster.myaddress, Tag: "test", Status: StatusOK, LastTouch: master_rs.cluster.slaves[1].LastTouch})
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, slaves, master_rs.cluster.slaves, string(content))
	resp.Body.Close()

	slave_conf.BindHost = ":6367" // slave3
	slave3, err := NewManager(slave_conf)
	assert.NoError(t, err)
	slave_rs3 := NewRestService(slave3, echo.New())
	err = Register([]string{master_rs.cluster.myaddress}, slave_rs3.cluster.myaddress, "test_change")
	assert.NoError(t, err)
	req = TagReq{Tag: "test_change"}
	marshaled, err = json.Marshal(req)
	resp, err = http.Post(slave_rs3.cluster.myaddress+"/logkit/cluster/tag", TESTContentApplictionJson, bytes.NewReader(marshaled))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, []Slave{{Url: slave_rs3.cluster.myaddress, Tag: "test_change", Status: StatusOK, LastTouch: master_rs.cluster.slaves[2].LastTouch}}, []Slave{master_rs.cluster.slaves[2]}, string(content))
	resp.Body.Close()

	defer func() {
		slave_rs.Stop()
		master_rs.Stop()
		slave_rs2.Stop()
		slave_rs3.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	var testClusterApiConf = `{
    "name":"test5",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestSalveConfigs/logdir",
        "meta_path":"./TestSalveConfigs/meta_mock_csv",
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
		"file_send_path": "./TestSalveConfigs/filesenderdata"
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

	// 增加 runner
	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test5?tag=test", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)

	resp, err = http.Post(master_rs.cluster.myaddress+"/logkit/cluster/configs/test5?tag=test_change", TESTContentApplictionJson, bytes.NewReader([]byte(testClusterApiConf)))
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	assert.NoError(t, err)
	time.Sleep(5 * time.Second)

	// 测试获取 slave configs
	resp, err = http.Get(master_rs.cluster.myaddress + "/logkit/cluster/configs?tag=test")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	con, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allConfigs := make(map[string]SlaveConfig)
	err = json.Unmarshal(con, &allConfigs)
	assert.NoError(t, err, string(con))

	rc := RunnerConfig{}
	err = json.Unmarshal([]byte(testClusterApiConf), &rc)
	assert.NoError(t, err)
	// 此处无法完全匹配 rc 与 val
	for _, val := range allConfigs {
		assert.Equal(t, "test", val.Tag)
		assert.Equal(t, rc.RunnerName, val.Configs[confdir+"/test5.conf"].RunnerName)
		assert.Equal(t, rc.MaxBatchLen, val.Configs[confdir+"/test5.conf"].MaxBatchLen)
		assert.Equal(t, rc.MaxBatchInteval, val.Configs[confdir+"/test5.conf"].MaxBatchInteval)
	}
	assert.Equal(t, 2, len(allConfigs))

	// tag 为空
	resp, err = http.Get(master_rs.cluster.myaddress + "/logkit/cluster/configs")
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)
	con, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	allConfigs = make(map[string]SlaveConfig)
	err = json.Unmarshal(con, &allConfigs)
	assert.NoError(t, err, string(con))
	assert.Equal(t, 3, len(allConfigs))
}
