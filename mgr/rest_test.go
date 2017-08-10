package mgr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

var TESTContentApplictionJson = "application/json"

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
    "batch_interval": 60,
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
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
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
	err := ioutil.WriteFile(logconfs+"/test1.conf", []byte(testRestConf), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	var conf ManagerConfig
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
	err = json.Unmarshal([]byte(out.String()), &rss)
	assert.NoError(t, err, out.String())
	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}
	exp := map[string]RunnerStatus{
		"test1.csv": {
			Name:    "test1.csv",
			Logpath: rp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  0,
				Success: 4,
			},
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": {
					Errors:  0,
					Success: 4,
				},
			},
		},
	}
	assert.Equal(t, exp, rss, out.String())

}

func Test_RestCRUD(t *testing.T) {
	dir := "Test_RestCRUD"
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

	// 开始POST 第一个
	var expconf1, got1 RunnerConfig
	err = json.Unmarshal([]byte(testRestCRUD1), &expconf1)
	if err != nil {
		t.Error(err)
	}
	expconf1.ReaderConfig[utils.GlobalKeyName] = expconf1.RunnerName
	expconf1.ReaderConfig[reader.KeyRunnerName] = expconf1.RunnerName
	expconf1.ParserConf[parser.KeyRunnerName] = expconf1.RunnerName
	expconf1.IsInWebFolder = true
	for i := range expconf1.SenderConfig {
		expconf1.SenderConfig[i][sender.KeyRunnerName] = expconf1.RunnerName
	}

	resp, err := http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/"+"testRestCRUD1", TESTContentApplictionJson, bytes.NewReader([]byte(testRestCRUD1)))
	if err != nil {
		t.Error(err)
	}
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	// GET 第一个
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/" + "testRestCRUD1")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &got1)
	if err != nil {
		fmt.Println(string(content))
		t.Error(err)
	}
	// POST的和GET做验证
	assert.Equal(t, expconf1, got1)
	assert.Equal(t, 1, len(m.runners))

	var expconf2, got2 RunnerConfig
	err = json.Unmarshal([]byte(testRestCRUD2), &expconf2)
	if err != nil {
		t.Error(err)
	}

	expconf2.ReaderConfig[utils.GlobalKeyName] = expconf2.RunnerName
	expconf2.ReaderConfig[reader.KeyRunnerName] = expconf2.RunnerName
	expconf2.ParserConf[parser.KeyRunnerName] = expconf2.RunnerName
	expconf2.IsInWebFolder = true
	for i := range expconf2.SenderConfig {
		expconf2.SenderConfig[i][sender.KeyRunnerName] = expconf2.RunnerName
	}

	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Error(string(content), resp.StatusCode)
	}

	// POST 第2个
	resp, err = http.Post("http://127.0.0.1"+rs.address+"/logkit/configs/testRestCRUD2", TESTContentApplictionJson, bytes.NewReader([]byte(testRestCRUD2)))
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	err = json.Unmarshal(content, &got2)
	if err != nil {
		t.Error(err)
	}
	// 验证 第2个
	assert.Equal(t, expconf2, got2)

	// 验证 一共有2个在运行
	assert.Equal(t, 2, len(m.runners))

	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	explists := map[string]RunnerConfig{
		confdir + "/testRestCRUD1.conf": expconf1,
		confdir + "/testRestCRUD2.conf": expconf2,
	}
	gotlists := make(map[string]RunnerConfig)
	err = json.Unmarshal(content, &gotlists)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, explists, gotlists)

	// DELETE testRestCRUD2
	req, err := http.NewRequest("DELETE", "http://127.0.0.1"+rs.address+"/logkit/configs/testRestCRUD2", nil)
	if err != nil {
		t.Error(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs/testRestCRUD2")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Error(string(content), resp.StatusCode)
	}
	assert.Equal(t, 1, len(m.runners))

	//再次get对比
	resp, err = http.Get("http://127.0.0.1" + rs.address + "/logkit/configs")
	if err != nil {
		t.Error(err)
	}
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	explists = map[string]RunnerConfig{
		confdir + "/testRestCRUD1.conf": expconf1,
	}
	gotlists = make(map[string]RunnerConfig)
	err = json.Unmarshal(content, &gotlists)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, explists, gotlists)
}
