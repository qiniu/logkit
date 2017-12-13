package mgr

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"net"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

var TESTContentApplictionJson = "application/json"

type respModeUsages struct {
	Code string           `json:"code"`
	Data []utils.KeyValue `json:"data"`
}

type respModeKeyOptions struct {
	Code string                    `json:"code"`
	Data map[string][]utils.Option `json:"data"`
}

type respSampleLogs struct {
	Code string            `json:"code"`
	Data map[string]string `json:"data"`
}

type respErrorCode struct {
	Code string            `json:"code"`
	Data map[string]string `json:"data"`
}

func Test_generateStatsShell(t *testing.T) {
	err := generateStatsShell(":4001", "/logkit")
	if err != nil {
		t.Errorf("Test_generateStatsShell fail %v", err)
	}
	_, err = os.Stat(StatsShell)
	if err != nil {
		t.Error(StatsShell + " not found")
	}
	os.Remove(StatsShell)
}

var testRestConf = `{
    "name":"test1.csv",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_Rest/logdir",
        "meta_path":"./Test_Rest/meta_mock_csv",
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
		"file_send_path": "./Test_Rest/filesenderdata"
    }]
}`

func Test_RestGetStatus(t *testing.T) {
	dir := "Test_Rest"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logconfs, err)
	}
	log1 := `hello 123
	xx 1
	`
	log2 := `h 456
	x 789`
	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}
	err = ioutil.WriteFile(logconfs+"/test1.conf", []byte(testRestConf), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6346"
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
	cmd := exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	rss := make(map[string]RunnerStatus)
	var respRss respRunnerStatus
	err = json.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, out.String())
	rss = respRss.Data
	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}
	exp := map[string]RunnerStatus{
		"test1.csv": {
			Name:             "test1.csv",
			Logpath:          rp,
			ReadDataCount:    4,
			ReadDataSize:     29,
			ReadSpeedTrend:   SpeedUp,
			ReadSpeedTrendKb: SpeedUp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  0,
				Success: 4,
				Trend:   SpeedUp,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 4,
					Trend:   SpeedUp,
				},
			},
			RunningStatus: RunnerRunning,
		},
	}

	v := rss["test1.csv"]
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	for key, val := range v.SenderStats {
		val.Speed = 0
		v.SenderStats[key] = val
	}
	rss["test1.csv"] = v
	assert.Equal(t, exp, rss, out.String())
}

func Test_RestCRUD(t *testing.T) {
	dir := "Test_RestCRUD"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logpath1 := dir + "/logdir1"
	if err := os.Mkdir(logpath1, 0755); err != nil {
		log.Fatalf("%v error mkdir %v %v", dir, logpath1, err)
	}
	logpath2 := dir + "/logdir2"
	if err := os.Mkdir(logpath2, 0755); err != nil {
		log.Fatalf("%v error mkdir %v %v", dir, logpath2, err)
	}
	testRestCRUD1 := `{
    "name":"testRestCRUD1",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RestCRUD/logdir1",
        "meta_path":"./Test_RestCRUD/meta_mock_csv",
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
		"file_send_path": "./Test_RestCRUD/filesenderdata1"
    }]
}`

	testRestCRUD2 := `{
    "name":"testRestCRUD2",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RestCRUD/logdir2",
        "meta_path":"./Test_RestCRUD/meta_mock_csv",
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
		"file_send_path": "./Test_RestCRUD/filesenderdata2"
    }]
}`

	testRestCRUD3_Up2 := `{
    "name":"testRestCRUD2",
    "batch_len": 10,
    "batch_size": 10,
    "batch_interval": 10,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RestCRUD/logdir2",
        "meta_path":"./Test_RestCRUD/meta_mock_csv",
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
		"file_send_path": "./Test_RestCRUD/filesenderdata2"
    }]
}`

	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/Test_RestCRUD"
	defer os.RemoveAll(confdir)

	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6345"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()
	assert.Equal(t, rs.address, conf.BindHost)

	// 开始POST 第一个
	t.Log("开始POST 第一个")
	var expconf1, got1 RunnerConfig
	var respGot1 respRunnerConfig
	err = json.Unmarshal([]byte(testRestCRUD1), &expconf1)
	assert.NoError(t, err)
	expconf1.ReaderConfig[utils.GlobalKeyName] = expconf1.RunnerName
	expconf1.ReaderConfig[reader.KeyRunnerName] = expconf1.RunnerName
	expconf1.ParserConf[parser.KeyRunnerName] = expconf1.RunnerName
	expconf1.IsInWebFolder = true
	for i := range expconf1.SenderConfig {
		expconf1.SenderConfig[i][sender.KeyRunnerName] = expconf1.RunnerName
	}

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"testRestCRUD1", TESTContentApplictionJson, bytes.NewReader([]byte(testRestCRUD1)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	// GET 第一个
	t.Log("开始GET 第一个")
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/" + "testRestCRUD1")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &respGot1)
	if err != nil {
		fmt.Println(string(content))
		t.Error(err)
	}
	got1 = respGot1.Data
	// POST的和GET做验证
	t.Log("POST的和GET做验证")
	got1.CreateTime = ""
	assert.Equal(t, expconf1, got1)
	assert.Equal(t, 1, len(m.runners))

	var expconf2, got2 RunnerConfig
	var respGot2 respRunnerConfig
	err = json.Unmarshal([]byte(testRestCRUD2), &expconf2)
	assert.NoError(t, err)

	expconf2.ReaderConfig[utils.GlobalKeyName] = expconf2.RunnerName
	expconf2.ReaderConfig[reader.KeyRunnerName] = expconf2.RunnerName
	expconf2.ParserConf[parser.KeyRunnerName] = expconf2.RunnerName
	expconf2.IsInWebFolder = true
	for i := range expconf2.SenderConfig {
		expconf2.SenderConfig[i][sender.KeyRunnerName] = expconf2.RunnerName
	}

	t.Log("GET 2")
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Error(string(content), resp.StatusCode)
	}

	// POST 第2个
	t.Log("Post 2")
	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/testRestCRUD2", TESTContentApplictionJson, bytes.NewReader([]byte(testRestCRUD2)))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &respGot2)
	assert.NoError(t, err)
	got2 = respGot2.Data
	got2.CreateTime = ""
	// 验证 第2个
	assert.Equal(t, expconf2, got2)

	// 验证 一共有2个在运行
	assert.Equal(t, 2, len(m.runners))

	t.Log("GET all")
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	explists := map[string]RunnerConfig{
		confdir + "/testRestCRUD1.conf": expconf1,
		confdir + "/testRestCRUD2.conf": expconf2,
	}
	var respGotLists respRunnerConfigs
	gotlists := make(map[string]RunnerConfig)
	err = json.Unmarshal(content, &respGotLists)
	assert.NoError(t, err)
	gotlists = respGotLists.Data
	for i, v := range gotlists {
		v.CreateTime = ""
		gotlists[i] = v
	}
	assert.Equal(t, explists, gotlists)

	// PUT testRestCRUD2
	req, err := http.NewRequest("PUT", "http://127.0.0.1"+rs.address+"/logkit/configs/testRestCRUD2", bytes.NewReader([]byte(testRestCRUD3_Up2)))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", TESTContentApplictionJson)
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	var gotUpdate RunnerConfig
	var respGotUpdate respRunnerConfig
	err = json.Unmarshal(content, &respGotUpdate)
	assert.NoError(t, err)
	gotUpdate = respGotUpdate.Data
	assert.Equal(t, 10, gotUpdate.MaxBatchLen)
	assert.Equal(t, 10, gotUpdate.MaxBatchSize)
	assert.Equal(t, 10, gotUpdate.MaxBatchInteval)

	// DELETE testRestCRUD2
	t.Log("delete 2")
	req, err = http.NewRequest("DELETE", "http://127.0.0.1"+rs.address+"/logkit/configs/testRestCRUD2", nil)
	assert.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}

	t.Log("get 2")
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Error(string(content), resp.StatusCode)
	}
	assert.Equal(t, 1, len(m.runners))

	//再次get对比
	t.Log("get all")
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs")
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	explists = map[string]RunnerConfig{
		confdir + "/testRestCRUD1.conf": expconf1,
	}
	respGotLists = respRunnerConfigs{}
	err = json.Unmarshal(content, &respGotLists)
	assert.NoError(t, err)
	gotlists = respGotLists.Data
	for i, v := range gotlists {
		v.CreateTime = ""
		gotlists[i] = v
	}
	assert.NoError(t, err)
	assert.Equal(t, explists, gotlists)
}

func Test_RunnerReset(t *testing.T) {
	var runnerResetConf = `{
    "name":"test1.csv",
    "batch_len": 1,
    "batch_size": 200,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RunnerReset/logdir",
        "meta_path":"./Test_RunnerReset/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./Test_RunnerReset/filesenderdata"
    }]
}`

	dir := "Test_RunnerReset"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_RunnerReset error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logconfs, err)
	}
	log1 := `{"a":1,"b":"2"}
	{"a":3,"b":"4"}
	`
	log2 := `{"a":5,"b":"6"}
	{"a":7,"b":"8"}
	`
	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}

	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}

	exp := map[string]RunnerStatus{
		"test1.csv": {
			Name:             "test1.csv",
			Logpath:          rp,
			ReadDataCount:    5,
			ReadDataSize:     68,
			ReadSpeedTrend:   SpeedUp,
			ReadSpeedTrendKb: SpeedUp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  1,
				Success: 4,
				Trend:   SpeedUp,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 4,
					Trend:   SpeedUp,
				},
			},
			RunningStatus: RunnerRunning,
		},
	}

	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6344"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		dir + "/confs",
	}
	err = m.Watch(confs)
	assert.NoError(t, err)
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test1.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerResetConf)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(10 * time.Second)

	cmd := exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss := respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, out.String())
	rss := respRss.Data

	v := rss["test1.csv"]
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs := v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rss["test1.csv"] = v
	assert.Equal(t, exp, rss, out.String())

	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test1.csv/reset", TESTContentApplictionJson, nil)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)
	out.Reset()
	cmd = exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss = respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, "OUTSTRING: "+out.String())
	rss = respRss.Data
	rp, err = filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}
	v = rss["test1.csv"]
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs = v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rss["test1.csv"] = v
	assert.Equal(t, exp, rss, out.String())
}

func Test_RunnerStart(t *testing.T) {
	var runnerStartConf = `{
    "name":"test2.csv",
    "batch_len": 1,
    "batch_size": 200,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RunnerStart/logdir",
        "meta_path":"./Test_RunnerStart/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./Test_RunnerStart/filesenderdata"
    }],
	"is_stopped": true
}`

	dir := "Test_RunnerStart"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_RunnerStart error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logconfs, err)
	}
	log1 := `{"a":1,"b":"2"}
	{"a":3,"b":"4"}
	`
	log2 := `{"a":5,"b":"6"}
	{"a":7,"b":"8"}
	`
	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}

	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}

	exp := map[string]RunnerStatus{
		"test2.csv": {
			Name:             "test2.csv",
			Logpath:          rp,
			ReadDataCount:    5,
			ReadDataSize:     68,
			ReadSpeedTrend:   SpeedUp,
			ReadSpeedTrendKb: SpeedUp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  1,
				Success: 4,
				Trend:   SpeedUp,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 4,
					Trend:   SpeedUp,
				},
			},
			RunningStatus: RunnerRunning,
		},
	}

	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6347"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		dir + "/confs",
	}
	err = m.Watch(confs)
	assert.NoError(t, err)
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test2.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerStartConf)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	cmd := exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss := respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, out.String())
	rss := respRss.Data
	assert.Equal(t, map[string]RunnerStatus{
		"test2.csv": RunnerStatus{
			Name:           "test2.csv",
			ReaderStats:    utils.StatsInfo{},
			ParserStats:    utils.StatsInfo{},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats:    make(map[string]utils.StatsInfo),
			RunningStatus:  RunnerStopped,
		},
	}, rss)

	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test2.csv/start", TESTContentApplictionJson, nil)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)
	out.Reset()
	cmd = exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss = respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, "OUTSTRING: "+out.String())
	rss = respRss.Data
	rp, err = filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}
	v := rss["test2.csv"]
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs := v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rss["test2.csv"] = v
	assert.Equal(t, exp, rss, out.String())

	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test2.csv/start", TESTContentApplictionJson, nil)
	assert.NoError(t, err)
	assert.NotEqual(t, resp.StatusCode, 200)
}

func Test_RunnerStop(t *testing.T) {
	var runnerStopConf = `{
    "name":"test3.csv",
    "batch_len": 1,
    "batch_size": 200,
    "batch_interval": 60,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RunnerStop/logdir",
        "meta_path":"./Test_RunnerStop/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./Test_RunnerStop/filesenderdata"
    }]
}`

	dir := "Test_RunnerStop"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_RunnerStop error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logconfs, err)
	}
	log1 := `{"a":1,"b":"2"}
	{"a":3,"b":"4"}
	`
	log2 := `{"a":5,"b":"6"}
	{"a":7,"b":"8"}
	`
	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}

	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}

	exp := map[string]RunnerStatus{
		"test3.csv": {
			Name:             "test3.csv",
			Logpath:          rp,
			ReadDataCount:    5,
			ReadDataSize:     68,
			ReadSpeedTrend:   SpeedUp,
			ReadSpeedTrendKb: SpeedUp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  1,
				Success: 4,
				Trend:   SpeedUp,
			},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 4,
					Trend:   SpeedUp,
				},
			},
			RunningStatus: RunnerRunning,
		},
	}

	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6348"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		dir + "/confs",
	}
	err = m.Watch(confs)
	assert.NoError(t, err)
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test3.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerStopConf)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	cmd := exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss := respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	rss := respRss.Data
	assert.NoError(t, err, out.String())

	v := rss["test3.csv"]
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs := v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rss["test3.csv"] = v
	assert.Equal(t, exp, rss, out.String())

	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test3.csv/stop", TESTContentApplictionJson, nil)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)
	out.Reset()
	cmd = exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss = respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	rss = respRss.Data
	assert.NoError(t, err, "OUTSTRING: "+out.String())
	assert.Equal(t, map[string]RunnerStatus{
		"test3.csv": RunnerStatus{
			Name:           "test3.csv",
			ReaderStats:    utils.StatsInfo{},
			ParserStats:    utils.StatsInfo{},
			TransformStats: make(map[string]utils.StatsInfo),
			SenderStats:    make(map[string]utils.StatsInfo),
			RunningStatus:  RunnerStopped,
		},
	}, rss)

	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test3.csv/reset", TESTContentApplictionJson, bytes.NewReader([]byte(runnerStopConf)))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	cmd = exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	out.Reset()
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss = respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	rss = respRss.Data
	assert.NoError(t, err, out.String())

	v = rss["test3.csv"]
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ParserStats.Speed = 0
	fs = v.SenderStats["file_sender"]
	fs.Speed = 0
	v.SenderStats["file_sender"] = fs
	rss["test3.csv"] = v
	assert.Equal(t, exp, rss, out.String())
}

func Test_RunnerDataIntegrity(t *testing.T) {
	var runnerStopConf = `{
    "name":"test4.csv",
    "batch_size": 1000,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./Test_RunnerData/logdir",
        "meta_path":"./Test_RunnerData/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./Test_RunnerData/filesenderdata"
    }]
}`

	dir := "Test_RunnerData"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_RunnerData error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	writeCnt := int64(0)
	dataLine := int64(100)
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	filesenderdata := dir + "/filesenderdata"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logconfs, err)
	}
	log1 := `{"a":1,"b":2}`
	file, err := os.OpenFile(filepath.Join(logpath, "log1"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalf("Test_Run error createfile %v %v", filepath.Join(logpath, "log1"), err)
	}
	w := bufio.NewWriter(file)
	for i := int64(0); i < dataLine; i++ {
		fmt.Fprintln(w, log1)
	}
	writeCnt++
	w.Flush()
	file.Close()
	time.Sleep(time.Second)

	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6343"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		dir + "/confs",
	}
	err = m.Watch(confs)
	assert.NoError(t, err)
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test4.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerStopConf)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	for i := 0; i < 3; i++ {
		resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test4.csv/stop", TESTContentApplictionJson, nil)
		assert.NoError(t, err)
		content, _ = ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Error(string(content))
		}
		time.Sleep(5 * time.Second)

		file, err := os.OpenFile(filepath.Join(logpath, "log1"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			log.Fatalf("Test_Run error createfile %v %v", filepath.Join(logpath, "log1"), err)
		}
		w := bufio.NewWriter(file)
		for i := int64(0); i < dataLine; i++ {
			fmt.Fprintln(w, log1)
		}
		writeCnt++
		w.Flush()
		file.Close()
		time.Sleep(time.Second)

		resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test4.csv/start", TESTContentApplictionJson, nil)
		assert.NoError(t, err)
		content, _ = ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Error(string(content))
		}
		time.Sleep(6 * time.Second)
	}
	var out bytes.Buffer
	out.Reset()
	cmd := exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss := respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	rss := respRss.Data
	var curLine int64 = 0
	f, err := os.Open(filesenderdata)
	assert.NoError(t, err)
	defer f.Close()
	br := bufio.NewReader(f)
	result := make([]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal result curLine = %v %v", curLine, err)
		}
		curLine += int64(len(result))
	}
	out.Reset()
	cmd = exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	respRss = respRunnerStatus{}
	err = json.Unmarshal([]byte(out.String()), &respRss)
	rss = respRss.Data
	assert.Equal(t, dataLine*writeCnt, curLine)
	assert.Equal(t, dataLine*writeCnt, rss["test4.csv"].ReadDataCount)
	assert.Equal(t, dataLine*writeCnt, rss["test4.csv"].ParserStats.Success)
	assert.Equal(t, dataLine*writeCnt, rss["test4.csv"].SenderStats["file_sender"].Success)
}

func TestParseUrl(t *testing.T) {
	host, port, err := net.SplitHostPort(":1234")
	assert.NoError(t, err)
	fmt.Println(host, port)
}

func TestGetMySlaveUrl(t *testing.T) {
	url, err := GetMySlaveUrl("127.0.0.1:1222", "https://")
	assert.NoError(t, err)
	assert.Equal(t, "https://127.0.0.1:1222", url)
	url, err = GetMySlaveUrl(":1222", "http://")
	assert.NoError(t, err)
	fmt.Println("TestGetMySlaveUrl your IP:", url)
}

func TestGetErrorCode(t *testing.T) {
	var conf ManagerConfig
	conf.BindHost = ":6700"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
	}()
	resp, err := http.Get("http://127.0.0.1" + conf.BindHost + "/logkit/errorcode")
	assert.NoError(t, err)
	respCodeMap := respErrorCode{}
	content, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(content, &respCodeMap)
	assert.NoError(t, err)
	codeMap := respCodeMap.Data
	assert.Equal(t, len(utils.ErrorCodeHumanize), len(codeMap))
	for key, val := range utils.ErrorCodeHumanize {
		cm, ok := codeMap[key]
		assert.Equal(t, true, ok)
		if ok {
			assert.Equal(t, val, cm)
		}
	}
}

func TestGetRunners(t *testing.T) {
	var runnerConf1 = `{
    "name":"test1.csv",
    "batch_size": 1000,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestGetRunners/logdir",
        "meta_path":"./TestGetRunners/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./TestGetRunners/filesenderdata"
    }]
}`
	var runnerConf2 = `{
    "name":"test2.csv",
    "batch_size": 1000,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestGetRunners/logdir",
        "meta_path":"./TestGetRunners/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./TestGetRunners/filesenderdata"
    }]
}`
	dir := "TestGetRunners"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_RunnerData error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logPath := dir + "/logdir"
	metaPath := dir + "/meta_mock_csv"
	logConfs := dir + "/confs"
	if err := os.Mkdir(logPath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logPath, err)
	}
	if err := os.Mkdir(metaPath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metaPath, err)
	}
	if err := os.Mkdir(logConfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logConfs, err)
	}
	var conf ManagerConfig
	conf.BindHost = ":6701"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
	}()
	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test1.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerConf1)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(1 * time.Second)
	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test2.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerConf2)))
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(1 * time.Second)
	resp, err = http.Get("http://127.0.0.1" + conf.BindHost + "/logkit/runners")
	assert.NoError(t, err)
	var respRunner respRunnersNameList
	content, _ = ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(content, &respRunner)
	assert.NoError(t, err)
	runnerNameList := respRunner.Data
	assert.Equal(t, 2, len(runnerNameList))
	res := make([]string, 0)
	if runnerNameList[0] == "test1.csv" {
		res = append(res, "test1.csv")
		res = append(res, "test2.csv")
	} else {
		res = append(res, "test2.csv")
		res = append(res, "test1.csv")
	}
	assert.Equal(t, res, runnerNameList)
}

func TestSenderRouter(t *testing.T) {
	var runnerConf = `{
    "name":"test10.csv",
    "batch_size": 1000,
    "batch_interval": 1,
    "batch_try_times": 3,
    "reader":{
        "log_path":"./TestSenderRouter/logdir/log1",
        "meta_path":"./TestSenderRouter/meta_mock_csv",
        "mode":"file",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "json"
    },
	"router": {
		"router_key_name": "a",
		"router_default_sender": 2,
		"router_match_type": "equal",
		"router_routes": {
			"a": 0,
			"123": 0,
			"b": 1
		}
	},
    "senders":[
		{
			"name":           "file_sender1",
			"sender_type":    "file",
			"file_send_path": "./TestSenderRouter/sender_file1"
		},
		{
			"name":           "file_sender2",
			"sender_type":    "file",
			"file_send_path": "./TestSenderRouter/sender_file2"
		},
		{
			"name":           "file_sender3",
			"sender_type":    "file",
			"file_send_path": "./TestSenderRouter/sender_file3"
		}
	]
}`

	dir := "TestSenderRouter"
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("TestSenderRouter error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confdir := pwd + "/" + dir
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	filesenderdata1 := dir + "/sender_file1"
	filesenderdata2 := dir + "/sender_file2"
	filesenderdata3 := dir + "/sender_file3"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("TestSenderRouter error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("TestSenderRouter error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("TestSenderRouter error mkdir %v %v", logconfs, err)
	}
	log1 := `{"a":1,"b":2}
{"a": "a", "b": 3}
{"a": "b", "b": 3}
{"a": "c", "b": 3}
{"a": "a", "b": 3}
{"a": "b", "b": 3}
{"a": "AAA", "b": 3}
{"a": 123.21, "b": 3}
{"a": 123, "b": 3}
{"a": "123", "b": 3}
{"a": "a", "b": 3}
{"a": "a", "b": 3}`
	file, err := os.OpenFile(filepath.Join(logpath, "log1"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.Fatalf("TestSenderRouter error createfile %v %v", filepath.Join(logpath, "log1"), err)
	}
	w := bufio.NewWriter(file)
	fmt.Fprintln(w, log1)
	w.Flush()
	file.Close()
	time.Sleep(time.Second)

	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6702"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, err)
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"test10.csv", TESTContentApplictionJson, bytes.NewReader([]byte(runnerConf)))
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	f1, err := os.Open(filesenderdata1)
	assert.NoError(t, err)
	defer f1.Close()
	br := bufio.NewReader(f1)
	result := make([]map[string]interface{}, 0)
	dataCnt := 0
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("TestSenderRouter error unmarshal result curLine = %v %v", dataCnt, err)
		}
		dataCnt += len(result)
	}
	assert.Equal(t, 6, dataCnt)

	f2, err := os.Open(filesenderdata2)
	assert.NoError(t, err)
	defer f2.Close()
	br = bufio.NewReader(f2)
	result = make([]map[string]interface{}, 0)
	dataCnt = 0
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("TestSenderRouter error unmarshal result curLine = %v %v", dataCnt, err)
		}
		dataCnt += len(result)
	}
	assert.Equal(t, 2, dataCnt)

	f3, err := os.Open(filesenderdata3)
	assert.NoError(t, err)
	defer f3.Close()
	br = bufio.NewReader(f3)
	result = make([]map[string]interface{}, 0)
	dataCnt = 0
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("TestSenderRouter error unmarshal result curLine = %v %v", dataCnt, err)
		}
		dataCnt += len(result)
	}
	assert.Equal(t, 4, dataCnt)
}
