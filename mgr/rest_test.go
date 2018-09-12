package mgr

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/router"
	. "github.com/qiniu/logkit/utils/models"
)

type respModeUsages struct {
	Code string        `json:"code"`
	Data KeyValueSlice `json:"data"`
}

type respModeKeyOptions struct {
	Code string              `json:"code"`
	Data map[string][]Option `json:"data"`
}

type respSampleLogs struct {
	Code string            `json:"code"`
	Data map[string]string `json:"data"`
}

type respErrorCode struct {
	Code string            `json:"code"`
	Data map[string]string `json:"data"`
}

type respDataMessage struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type testParam struct {
	rd string
	t  *testing.T
	rs *RestService
}

func getMockSenderRunnerConfig(name, logPath, metaPath, mode string) ([]byte, error) {
	runnerConf := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       name,
			MaxBatchLen:      5,
			MaxBatchSize:     200,
			CollectInterval:  1,
			MaxBatchInterval: 1,
			MaxBatchTryTimes: 3,
		},
		ReaderConfig: conf.MapConf{
			"log_path":      logPath,
			"meta_path":     metaPath,
			"mode":          mode,
			"read_from":     "oldest",
			"ignore_hidden": "true",
		},
		ParserConf: conf.MapConf{
			"type": "json",
			"name": "json_parser",
		},
		SendersConfig: []conf.MapConf{{
			"sender_type": "mock",
			"is_req_err":  "true",
		}},
	}
	return jsoniter.Marshal(runnerConf)
}

func getRunnerConfig(name, logPath, metaPath, mode, senderPath string) ([]byte, error) {
	runnerConf := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       name,
			MaxBatchLen:      1,
			MaxBatchSize:     200,
			CollectInterval:  1,
			MaxBatchInterval: 1,
			MaxBatchTryTimes: 3,
		},
		ReaderConfig: conf.MapConf{
			"log_path":      logPath,
			"meta_path":     metaPath,
			"mode":          mode,
			"read_from":     "oldest",
			"ignore_hidden": "true",
		},
		ParserConf: conf.MapConf{
			"type": "json",
			"name": "json_parser",
		},
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": senderPath,
		}},
	}
	return jsoniter.Marshal(runnerConf)
}

func getRunnerStatus(rn, lp, rs, sender, lastError string, rdc, rds, pe, ps, se, ss int64) map[string]RunnerStatus {
	unit := "bytes"
	if rs != RunnerRunning {
		unit = ""
	}
	return map[string]RunnerStatus{
		rn: {
			Name:             rn,
			Logpath:          lp,
			ReadDataCount:    rdc,
			ReadDataSize:     rds,
			RunningStatus:    rs,
			ReadSpeedTrend:   "",
			ReadSpeedTrendKb: "",
			Lag: LagInfo{
				Size:     0,
				SizeUnit: unit,
			},
			ReaderStats: StatsInfo{
				Success: rdc,
			},
			ParserStats: StatsInfo{
				Errors:  pe,
				Success: ps,
				Trend:   "",
			},
			TransformStats: make(map[string]StatsInfo),
			SenderStats: map[string]StatsInfo{
				sender: {
					Errors:    se,
					Success:   ss,
					Trend:     "",
					LastError: lastError,
				},
			},
		},
	}
}

func clearGotStatus(v *RunnerStatus) {
	if v == nil {
		return
	}
	v.Elaspedtime = 0
	v.ReadSpeed = 0
	v.ReadSpeedKB = 0
	v.ReadSpeedTrendKb = ""
	v.ReadSpeedTrend = ""
	v.ReaderStats.Trend = ""
	v.ReaderStats.Speed = 0

	v.ParserStats.Speed = 0
	v.ParserStats.Trend = ""
	for k, t := range v.TransformStats {
		t.Trend = ""
		t.Speed = 0
		v.TransformStats[k] = t
	}
	for k, s := range v.SenderStats {
		s.Trend = ""
		s.Speed = 0
		v.SenderStats[k] = s
	}
}

func mkTestDir(mkDir ...string) error {
	for _, d := range mkDir {
		if err := os.Mkdir(d, DefaultDirPerm); err != nil {
			return err
		}
	}
	return nil
}

func writeLogFile(logStr []string, logDir string) error {
	for i, l := range logStr {
		if err := ioutil.WriteFile(filepath.Join(logDir, "log"+strconv.Itoa(i+1)), []byte(l), 0666); err != nil {
			return err
		}
		time.Sleep(100 * time.Microsecond)
	}
	return nil
}

func makeRequest(url, method string, configBytes []byte) (respCode int, respBody []byte, err error) {
	config := bytes.NewReader(configBytes)
	req, err := http.NewRequest(method, url, config)
	if err != nil {
		return
	}
	req.Header.Set(ContentTypeHeader, ApplicationJson)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	respCode = resp.StatusCode
	respBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return
}

func TestWebAPI(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confDirName := "confs"
	dirName := "testWebAPI"
	rootDir := filepath.Join(pwd, dirName)
	confDir := filepath.Join(rootDir, confDirName)
	os.RemoveAll(rootDir)
	if err := mkTestDir(rootDir, confDir); err != nil {
		t.Fatalf("testWebAPI mkdir error %v", err)
	}
	var logkitConf ManagerConfig
	logkitConf.RestDir = confDir
	logkitConf.BindHost = ":6301"
	m, err := NewManager(logkitConf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	time.Sleep(2 * time.Second)
	c := make(chan string)
	defer func() {
		close(c)
		rs.Stop()
		os.RemoveAll(rootDir)
		os.Remove(StatsShell)
	}()

	funcMap := map[string]func(*testParam){
		"metricAPITest":      metricAPITest,
		"parserParseTest":    parserParseTest,
		"parserAPITest":      parserAPITest,
		"readerAPITest":      readerAPITest,
		"senderAPITest":      senderAPITest,
		"transformerAPITest": transformerAPITest,
	}

	for k, f := range funcMap {
		go func(k string, f func(*testParam), c chan string) {
			f(&testParam{rootDir, t, rs})
			c <- k
		}(k, f, c)
	}
	funcCnt := len(funcMap)
	for i := 0; i < funcCnt; i++ {
		<-c
	}
}

func TestWebRest(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	dirName := "testWebRest"
	confDirName := "confs"
	webConfDirName := "webConfs"
	rootDir := filepath.Join(pwd, dirName)
	confDir := filepath.Join(rootDir, confDirName)
	webConfDir := filepath.Join(rootDir, webConfDirName)
	os.RemoveAll(rootDir)
	if err := mkTestDir(rootDir, confDir, webConfDir); err != nil {
		t.Fatalf("TestWebRest mkdir error %v", err)
	}
	var logkitConf ManagerConfig
	logkitConf.BindHost = ":6302"
	logkitConf.RestDir = webConfDir
	m, err := NewManager(logkitConf)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Watch([]string{confDir}); err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	time.Sleep(2 * time.Second)
	defer func() {
		rs.Stop()
		os.RemoveAll(rootDir)
		os.Remove(StatsShell)
	}()

	funcMap := map[string]func(*testParam){
		"restGetFailedDataStatusTest": restGetFailedDataStatusTest,
		"restGetStatusTest":           restGetStatusTest,
		"runnerResetTest":             runnerResetTest,
		"restCRUDTest":                restCRUDTest,
		"runnerStopStartTest":         runnerStopStartTest,
		"runnerDataIntegrityTest":     runnerDataIntegrityTest,
		"getErrorCodeTest":            getErrorCodeTest,
		"getRunnersTest":              getRunnersTest,
		"senderRouterTest":            senderRouterTest,
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(funcMap))
	for k, f := range funcMap {
		go func(k string, f func(*testParam), group *sync.WaitGroup) {
			f(&testParam{rootDir, t, rs})
			group.Done()
		}(k, f, wg)
	}
	wg.Wait()
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

func restGetFailedDataStatusTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	runnerName := "restGetFailedDataStatusTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	time.Sleep(1 * time.Second)
	runnerConf, err := getMockSenderRunnerConfig(runnerName, logDir, metaDir, reader.ModeDir)
	if err != nil {
		t.Fatalf("get mock sender runner config failed, error is %v", err)
	}
	for k := range rs.mgr.watchers {
		if err = ioutil.WriteFile(k+"/"+runnerName+".conf", runnerConf, 0666); err != nil {
			t.Error(err)
		} else {
			break
		}
	}
	log1 := `{"a":1,"b":2,"c":"3","d":"4"}
{"a1":1,"b1":2,"c1":"3","d1":"4"}
{"a2":1,"b2":2,"c2":"3","d2":"4"}
{"a3":1,"b3":2,"c3":"3","d3":"4"}
{"a4":1,"b4":2,"c4":"3","d4":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	time.Sleep(20 * time.Second)
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
	err = jsoniter.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, out.String())
	rss = respRss.Data
	exp := getRunnerStatus(runnerName, logDir, RunnerRunning, "mockSender", "SendError: mock failed, failDatas size : 1", 5, 29*5, 0, 5, 1, 4)

	v, ex := rss[runnerName]
	assert.Equal(t, true, ex)
	clearGotStatus(&v)
	v.ReadDataSize = exp[runnerName].ReadDataSize
	v.HistoryErrors = nil
	rss[runnerName] = v
	assert.Equal(t, exp[runnerName], rss[runnerName], out.String())
}

// 测试 status/stats/confs watcher
func restGetStatusTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName := "sendData"
	runnerName := "restGetStatusTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	time.Sleep(1 * time.Second)
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, reader.ModeDir, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	for k := range rs.mgr.watchers {
		if err = ioutil.WriteFile(k+"/"+runnerName+".conf", runnerConf, 0666); err != nil {
			t.Error(err)
		} else {
			break
		}
	}
	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	time.Sleep(20 * time.Second)
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
	err = jsoniter.Unmarshal([]byte(out.String()), &respRss)
	assert.NoError(t, err, out.String())
	rss = respRss.Data
	exp := getRunnerStatus(runnerName, logDir, RunnerRunning, "file_sender", "", 1, 29, 0, 1, 0, 1)

	v, ex := rss[runnerName]
	assert.Equal(t, true, ex)
	clearGotStatus(&v)
	v.ReadDataSize = exp[runnerName].ReadDataSize
	rss[runnerName] = v
	assert.Equal(t, exp[runnerName], rss[runnerName], out.String())
}

func restCRUDTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData1"
	resvName2 := "sendData2"
	dir := "restCRUDTestDir"
	runnerName1 := "restCrud1"
	runnerName2 := "restCrud2"
	testDir := filepath.Join(rd, dir)
	logDir1 := filepath.Join(testDir, "logdir1")
	logDir2 := filepath.Join(testDir, "logdir2")
	logPath1 := filepath.Join(logDir1, "log1")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	resvPath2 := filepath.Join(resvDir, resvName2)
	if err := mkTestDir(testDir, logDir1, logDir2, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir1); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	if err := writeLogFile([]string{log1}, logDir2); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	mode := reader.ModeDir
	conf1, err := getRunnerConfig(runnerName1, logDir1, metaDir, mode, resvPath1)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	conf2, err := getRunnerConfig(runnerName2, logDir2, metaDir, mode, resvPath2)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	mode = reader.ModeFile
	conf1Upd, err := getRunnerConfig(runnerName1, logPath1, metaDir, mode, resvPath1)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 开始POST 第一个
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName1
	respCode, respBody, err := makeRequest(url, http.MethodPost, conf1)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	var expconf1, got1 RunnerConfig
	var respGot1 respRunnerConfig
	err = jsoniter.Unmarshal([]byte(conf1), &expconf1)
	assert.NoError(t, err)
	expconf1.ReaderConfig[GlobalKeyName] = expconf1.RunnerName
	expconf1.ReaderConfig[KeyRunnerName] = expconf1.RunnerName
	expconf1.ParserConf[KeyRunnerName] = expconf1.RunnerName
	expconf1.IsInWebFolder = true
	for i := range expconf1.SendersConfig {
		expconf1.SendersConfig[i][KeyRunnerName] = expconf1.RunnerName
	}

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName1
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	err = jsoniter.Unmarshal(respBody, &respGot1)
	if err != nil {
		t.Error(err, string(respBody))
	}

	// POST的和GET做验证
	got1 = respGot1.Data
	got1.CreateTime = ""
	assert.Equal(t, expconf1, got1)

	var expconf2, got2 RunnerConfig
	var respGot2 respRunnerConfig
	err = jsoniter.Unmarshal([]byte(conf2), &expconf2)
	assert.NoError(t, err)

	expconf2.ReaderConfig[GlobalKeyName] = expconf2.RunnerName
	expconf2.ReaderConfig[KeyRunnerName] = expconf2.RunnerName
	expconf2.ParserConf[KeyRunnerName] = expconf2.RunnerName
	expconf2.IsInWebFolder = true
	for i := range expconf2.SendersConfig {
		expconf2.SendersConfig[i][KeyRunnerName] = expconf2.RunnerName
	}

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName2
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusBadRequest, respCode)

	// POST 第2个
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName2
	respCode, respBody, err = makeRequest(url, http.MethodPost, conf2)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName2
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	err = jsoniter.Unmarshal(respBody, &respGot2)
	assert.NoError(t, err)
	got2 = respGot2.Data
	got2.CreateTime = ""
	// 验证 第2个
	assert.Equal(t, expconf2, got2)

	url = "http://127.0.0.1" + rs.address + "/logkit/configs"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respGotLists respRunnerConfigs
	err = jsoniter.Unmarshal(respBody, &respGotLists)
	assert.NoError(t, err)
	gotLists := make(map[string]RunnerConfig)
	gotLists = respGotLists.Data
	for i, v := range gotLists {
		v.CreateTime = ""
		gotLists[i] = v
	}
	explists := map[string]RunnerConfig{
		rs.mgr.RestDir + "/" + runnerName1 + ".conf": expconf1,
		rs.mgr.RestDir + "/" + runnerName2 + ".conf": expconf2,
	}
	st1Name := rs.mgr.RestDir + "/" + runnerName1 + ".conf"
	st2Name := rs.mgr.RestDir + "/" + runnerName2 + ".conf"
	assert.Equal(t, explists[st1Name], gotLists[st1Name])
	assert.Equal(t, explists[st2Name], gotLists[st2Name])

	// PUT runner1
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName1
	respCode, respBody, err = makeRequest(url, http.MethodPut, conf1Upd)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(5 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName1
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var gotUpdate RunnerConfig
	var respGotUpdate respRunnerConfig
	err = jsoniter.Unmarshal(respBody, &respGotUpdate)
	assert.NoError(t, err)
	gotUpdate = respGotUpdate.Data
	assert.Equal(t, mode, gotUpdate.ReaderConfig["mode"])
	assert.Equal(t, logPath1, gotUpdate.ReaderConfig["log_path"])

	// DELETE runner2
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName2
	respCode, respBody, err = makeRequest(url, http.MethodDelete, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// get runner2
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName2
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusBadRequest, respCode)
	err = jsoniter.Unmarshal(respBody, &respGot2)
	assert.NoError(t, err)
	got2 = respGot2.Data
	got2.CreateTime = ""

	//再次get对比
	url = "http://127.0.0.1" + rs.address + "/logkit/configs"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotLists = respRunnerConfigs{}
	err = jsoniter.Unmarshal(respBody, &respGotLists)
	assert.NoError(t, err)
	gotLists = respGotLists.Data
	_, ex := gotLists[rs.mgr.RestDir+"/"+runnerName1+".conf"]
	assert.Equal(t, true, ex)
	_, ex = gotLists[rs.mgr.RestDir+"/"+runnerName2+".conf"]
	assert.Equal(t, false, ex)
}

type respErrors struct {
	Code string                  `json:"code"`
	Data map[string]ErrorsResult `json:"data"`
}

type respError struct {
	Code string       `json:"code"`
	Data ErrorsResult `json:"data"`
}

func runnerResetTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName := "sendData"
	runnerName := "runnerReset"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	mode := reader.ModeDir
	resetConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, resetConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(6 * time.Second)

	exp := getRunnerStatus(runnerName, logDir, RunnerRunning, "file_sender", "", 1, 29, 0, 1, 0, 1)
	url = "http://127.0.0.1" + rs.address + "/logkit/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss := respRunnerStatus{}
	if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rss := respRss.Data
	v := rss[runnerName]
	clearGotStatus(&v)
	rss[runnerName] = v
	assert.Equal(t, exp[runnerName], rss[runnerName])

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/reset"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(10 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss = respRunnerStatus{}
	if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rss = respRss.Data

	v = rss[runnerName]
	clearGotStatus(&v)
	rss[runnerName] = v
	assert.Equal(t, exp[runnerName], rss[runnerName])

	expErrors := map[string]ErrorsResult{
		runnerName: {},
	}
	url = "http://127.0.0.1" + rs.address + "/logkit/errors"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respErrs := respErrors{}
	if err = jsoniter.Unmarshal(respBody, &respErrs); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rssData := respErrs.Data
	assert.Equal(t, expErrors[runnerName], rssData[runnerName])

	os.RemoveAll(logDir)
	os.Mkdir(logDir, os.ModePerm)
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/reset"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(5 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/errors"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respErrs = respErrors{}
	if err = jsoniter.Unmarshal(respBody, &respErrs); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rssData = respErrs.Data
	assert.NotZero(t, rssData[runnerName].ReadErrors)
	assert.Equal(t, 1, len(rssData[runnerName].ReadErrors))
	assert.True(t, rssData[runnerName].ReadErrors[0].Count >= 1)
	assert.Equal(t, "no more file exist to be read", rssData[runnerName].ReadErrors[0].Error)

	url = "http://127.0.0.1" + rs.address + "/logkit/errors/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respErr := respError{}
	if err = jsoniter.Unmarshal(respBody, &respErr); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rssErrData := respErr.Data
	assert.True(t, len(rssErrData.ReadErrors) != 0)
	assert.Equal(t, "no more file exist to be read", rssErrData.ReadErrors[0].Error)

	url = "http://127.0.0.1" + rs.address + "/logkit/errors/runnerNotFound"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusBadRequest, respCode)
}

func runnerStopStartTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName := "sendData"
	runnerName := "runnerStopStartTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	mode := reader.ModeDir
	startConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, startConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(10 * time.Second)

	exp := getRunnerStatus(runnerName, logDir, RunnerRunning, "file_sender", "", 1, 29, 0, 1, 0, 1)
	url = "http://127.0.0.1" + rs.address + "/logkit/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss := respRunnerStatus{}
	if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rss := respRss.Data
	v := rss[runnerName]
	clearGotStatus(&v)
	rss[runnerName] = v
	assert.Equal(t, exp[runnerName], rss[runnerName])

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	expStopped := getRunnerStatus(runnerName, "", RunnerStopped, "file_sender", "", 0, 0, 0, 0, 0, 0)
	url = "http://127.0.0.1" + rs.address + "/logkit/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss = respRunnerStatus{}
	if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rss = respRss.Data
	v = rss[runnerName]
	clearGotStatus(&v)
	rss[runnerName] = v
	st := expStopped[runnerName]
	delete(st.SenderStats, "file_sender")
	expStopped[runnerName] = st
	assert.Equal(t, expStopped[runnerName], rss[runnerName])

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/start"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(5 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss = respRunnerStatus{}
	if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rss = respRss.Data
	v = rss[runnerName]
	clearGotStatus(&v)
	rss[runnerName] = v
	exp = getRunnerStatus(runnerName, logDir, RunnerRunning, "file_sender", "", 1, 0, 0, 1, 0, 1)
	assert.Equal(t, exp[runnerName], rss[runnerName])

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/start"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusBadRequest, respCode)
}

func runnerDataIntegrityTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	writeCnt := int64(0)
	dataLine := int64(100)
	resvName := "sendData"
	runnerName := "runnerDataIntegrityTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	log1 := `{"a":1,"b":2}`
	file, err := os.OpenFile(filepath.Join(logDir, "log1"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, DefaultFilePerm)
	if err != nil {
		log.Fatalf("Test_Run error createfile %v %v", filepath.Join(logDir, "log1"), err)
	}
	w := bufio.NewWriter(file)
	for i := int64(0); i < dataLine; i++ {
		fmt.Fprintln(w, log1)
	}
	writeCnt++
	w.Flush()
	file.Close()
	time.Sleep(time.Second)

	mode := reader.ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	for i := 0; i < 3; i++ {
		url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
		respCode, respBody, err := makeRequest(url, http.MethodPost, []byte{})
		assert.NoError(t, err, string(respBody))
		assert.Equal(t, http.StatusOK, respCode)
		time.Sleep(2 * time.Second)

		file, err := os.OpenFile(filepath.Join(logDir, "log1"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, DefaultFilePerm)
		if err != nil {
			log.Fatalf("Test_Run error createfile %v %v", filepath.Join(logDir, "log1"), err)
		}
		w := bufio.NewWriter(file)
		for i := int64(0); i < dataLine; i++ {
			fmt.Fprintln(w, log1)
		}
		writeCnt++
		w.Flush()
		file.Close()
		time.Sleep(time.Second)

		url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/start"
		respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
		assert.NoError(t, err, string(respBody))
		assert.Equal(t, http.StatusOK, respCode)
		time.Sleep(3 * time.Second)
	}
	time.Sleep(10 * time.Second)
	url = "http://127.0.0.1" + rs.address + "/logkit/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss := respRunnerStatus{}
	if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
		t.Fatalf("status unmarshal failed error is %v, respBody is %v", err, string(respBody))
	}
	rss := respRss.Data
	v := rss[runnerName]
	clearGotStatus(&v)
	rss[runnerName] = v
	var curLine int64 = 0
	f, err := os.Open(resvPath)
	assert.NoError(t, err)
	defer f.Close()
	br := bufio.NewReader(f)
	result := make([]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal result curLine = %v %v", curLine, err)
		}
		curLine += int64(len(result))
	}
	assert.Equal(t, dataLine*writeCnt, curLine)
	assert.Equal(t, dataLine*writeCnt, rss[runnerName].ReadDataCount)
	assert.Equal(t, dataLine*writeCnt, rss[runnerName].ParserStats.Success)
	assert.Equal(t, dataLine*writeCnt, rss[runnerName].SenderStats["file_sender"].Success)
}

func TestParseUrl(t *testing.T) {
	host, port, err := net.SplitHostPort(":1234")
	assert.NoError(t, err, fmt.Sprintf("%v:%v", host, port))
}

func TestGetMySlaveUrl(t *testing.T) {
	url, err := GetMySlaveUrl("127.0.0.1:1222", "https://")
	assert.NoError(t, err)
	assert.Equal(t, "https://127.0.0.1:1222", url)
	url, err = GetMySlaveUrl(":1222", "http://")
	assert.NoError(t, err, url)
}

func getErrorCodeTest(p *testParam) {
	t := p.t
	rs := p.rs
	url := "http://127.0.0.1" + rs.address + "/logkit/errorcode"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, respCode)
	respCodeMap := respErrorCode{}
	err = jsoniter.Unmarshal(respBody, &respCodeMap)
	assert.NoError(t, err)
	codeMap := respCodeMap.Data
	assert.Equal(t, len(ErrorCodeHumanize), len(codeMap))
	for key, val := range ErrorCodeHumanize {
		cm, ok := codeMap[key]
		assert.Equal(t, true, ok)
		if ok {
			assert.Equal(t, val, cm)
		}
	}
}

func getRunnersTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName := "sendData"
	runnerName1 := "getRunnersTest1"
	runnerName2 := "getRunnersTest2"
	dir := "getRunnersTestDir"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	mode := reader.ModeDir
	runnerConf1, err := getRunnerConfig(runnerName1, logDir, metaDir, mode, resvPath)
	runnerConf2, err := getRunnerConfig(runnerName2, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName1
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf1)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(1 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName2
	respCode, respBody, err = makeRequest(url, http.MethodPost, runnerConf2)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(1 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/runners"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respRunner respRunnersNameList
	err = jsoniter.Unmarshal(respBody, &respRunner)
	assert.NoError(t, err)
	runnerNameList := respRunner.Data
	runnerExist := 0
	for _, rn := range runnerNameList {
		if rn == runnerName1 || rn == runnerName2 {
			runnerExist++
		}
	}
	assert.Equal(t, 2, runnerExist)

	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName1
	respCode, respBody, err = makeRequest(url, http.MethodDelete, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(1 * time.Second)

	url = "http://127.0.0.1" + rs.address + "/logkit/runners"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)

	respRunner = respRunnersNameList{}
	err = jsoniter.Unmarshal(respBody, &respRunner)
	assert.NoError(t, err)
	runnerNameList = respRunner.Data
	runnerExist = 0
	var gotRunnerName string
	for _, rn := range runnerNameList {
		if rn == runnerName1 || rn == runnerName2 {
			runnerExist++
			gotRunnerName = rn
		}
	}
	assert.Equal(t, 1, runnerExist)
	assert.Equal(t, runnerName2, gotRunnerName)
}

func senderRouterTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData1"
	resvName2 := "sendData2"
	resvName3 := "sendData3"
	runnerName := "senderRouter"
	dir := runnerName + "Test"
	testDir := filepath.Join(rd, dir)
	logDir := filepath.Join(testDir, "logdir")
	metaDir := filepath.Join(testDir, "meta")
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	resvPath2 := filepath.Join(resvDir, resvName2)
	resvPath3 := filepath.Join(resvDir, resvName3)
	if err := mkTestDir(testDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
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
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log data error %v", err)
	}
	mode := reader.ModeDir
	runnerConfBytes, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath1)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}
	runnerConf := RunnerConfig{}
	err = jsoniter.Unmarshal(runnerConfBytes, &runnerConf)
	assert.NoError(t, err)
	runnerConf.SendersConfig = []conf.MapConf{
		conf.MapConf{
			"name":           "file_sender1",
			"sender_type":    "file",
			"file_send_path": resvPath1,
		},
		conf.MapConf{
			"name":           "file_sender2",
			"sender_type":    "file",
			"file_send_path": resvPath2,
		},
		conf.MapConf{
			"name":           "file_sender3",
			"sender_type":    "file",
			"file_send_path": resvPath3,
		},
	}
	runnerConf.Router = router.RouterConfig{
		KeyName:      "a",
		DefaultIndex: 2,
		MatchType:    router.MTypeEqualName,
		Routes: map[string]int{
			"a":   0,
			"123": 0,
			"b":   1,
		},
	}

	runnerConfBytes, err = jsoniter.Marshal(runnerConf)
	assert.NoError(t, err)
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConfBytes)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(10 * time.Second)

	f1, err := os.Open(resvPath1)
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
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("TestSenderRouter error unmarshal result curLine = %v %v", dataCnt, err)
		}
		dataCnt += len(result)
	}
	assert.Equal(t, 6, dataCnt)

	f2, err := os.Open(resvPath2)
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
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("TestSenderRouter error unmarshal result curLine = %v %v", dataCnt, err)
		}
		dataCnt += len(result)
	}
	assert.Equal(t, 2, dataCnt)

	f3, err := os.Open(resvPath3)
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
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("TestSenderRouter error unmarshal result curLine = %v %v", dataCnt, err)
		}
		dataCnt += len(result)
	}
	assert.Equal(t, 4, dataCnt)
}

func TestConvertWebParserConfig(t *testing.T) {
	cf := conf.MapConf{
		parser.KeyCSVSplitter:        "\\t",
		parser.KeyGrokCustomPatterns: `JUUyJTgyJUFDJTIwJUU0JUJEJUEwJUU1JUE1JUJEJTIwJUMzJUE2JUMzJUI4JUMzJUE1JUMzJTg2JUMzJTk4JUMzJTg1`,
	}
	newcf := parser.ConvertWebParserConfig(cf)
	expcf := conf.MapConf{
		parser.KeyCSVSplitter:        "\t",
		parser.KeyGrokCustomPatterns: `€ 你好 æøåÆØÅ`,
	}
	assert.Equal(t, expcf, newcf)

	cf = conf.MapConf{
		parser.KeyGrokCustomPatterns: `TkVXREFUQSUyMCguKiU1Q24pJTJCJTBBTVlMT0clMjAlNUMlNUIlMjUlN0JEQVRBJTNBdGltZXN0YW1wJTNBZGF0ZSU3RCU1QyU1RCU1QyU1QiUyNSU3Qk5PVFNQQUNFJTNBdHJhbnNOdW1iZXIlM0Fsb25nJTdEJTVDJTVEJTIwTGV2ZWwlMjAlMjUlN0JOT1RTUEFDRSUzQWxldmVsJTNBbG9uZyU3RCUyMFBNVFNNU0dIREwlM0ElMjAlNUNuJUU1JTg5JThEJUU0JUI4JTgwJUU1JUIxJThBJUU3JTgyJUI5JUU1JThGJTkxJUU5JTgwJTgxJUU2JTk3JUI2JUU5JTk3JUI0JTVDJTVCJTI1JTdCREFUQSUzQXByZXRyYXNuVGltZSUzQWRhdGUlN0QlNUMlNUQlMkMlRTglQjAlODMlRTclOTQlQThEb05leHRNc2clRTYlOTclQjYlRTklOTclQjQlNUMlNUIlMjUlN0JEQVRBJTNBbmV4dFRyYW5UaW1lJTNBZGF0ZSU3RCU1QyU1RCU1Q24lRTYlOUMlQUMlRTUlOUMlQjAlRTklOTglOUYlRTUlODglOTclRTclQUUlQTElRTclOTAlODYlRTUlOTklQTglM0ElNUMlNUIlMjUlN0JOT1RTUEFDRSUzQXF1ZXVlTWFuZ2VyJTdEJTVDJTVEJTJDJUU2JTlDJUFDJUU1JTlDJUIwJUU5JTk4JTlGJUU1JTg4JTk3JTNBJTVDJTVCJTI1JTdCTk9UU1BBQ0UlM0Fsb2NhbFF1ZXVlJTdEJTVDJTVEJTVDbiVFOSVBNiU5NiVFNSU4NSU4OCVFNSU4RiU5MSVFOSU4MCU4MSVFOSU5OCU5RiVFNSU4OCU5NyVFNSU5MCU4RCUzQSU1QyU1QiUyNSU3Qk5PVFNQQUNFJTNBZmlyc3RRdWV1ZU5hbWUlN0QlNUMlNUQlMkMlRTUlQTQlODclRTYlQjMlQTglM0ElNUMlNUIlMjUlN0JEQVRBJTNBbm90ZSU3RCU1QyU1RCU1Q25VJUU1JUE0JUI0JUU0JUJGJUExJUU2JTgxJUFGJTNBJTVDJTVCJTI1JTdCREFUQSUzQXVoZWFkZXIlN0QlNUMlNUQlNUNuJUU2JThBJUE1JUU2JTk2JTg3JUU1JTg2JTg1JUU1JUFFJUI5JTNBJTVDbiU3QkglM0ElMjUlN0JOT1RTUEFDRSUzQWhjb2RlJTdEJTVDdCUyQiUyNSU3Qk5PVFNQQUNFJTNBaGNvZGUyJTdEJTVDdCUyQiUyNSU3Qk5PVFNQQUNFJTNBaGNvZGUzJTdEJTVDdCUyQiUyNSU3Qk5PVFNQQUNFJTNBaGNvZGU0JTdEJTVDdCUyQiU3RCU1Q24oJTdCUyUzQSUyNSU3QkRBVEElM0FzZGF0YSU3RCU3RCU1Q24pJTNGJTI1JTdCTkVXREFUQSUzQXhtbCU3RCU1Q24lNUNuJTVDbg==`,
	}
	newcf = parser.ConvertWebParserConfig(cf)
	expcf = conf.MapConf{
		parser.KeyGrokCustomPatterns: `NEWDATA (.*\n)+
MYLOG \[%{DATA:timestamp:date}\]\[%{NOTSPACE:transNumber:long}\] Level %{NOTSPACE:level:long} PMTSMSGHDL: \n前一届点发送时间\[%{DATA:pretrasnTime:date}\],调用DoNextMsg时间\[%{DATA:nextTranTime:date}\]\n本地队列管理器:\[%{NOTSPACE:queueManger}\],本地队列:\[%{NOTSPACE:localQueue}\]\n首先发送队列名:\[%{NOTSPACE:firstQueueName}\],备注:\[%{DATA:note}\]\nU头信息:\[%{DATA:uheader}\]\n报文内容:\n{H:%{NOTSPACE:hcode}\t+%{NOTSPACE:hcode2}\t+%{NOTSPACE:hcode3}\t+%{NOTSPACE:hcode4}\t+}\n({S:%{DATA:sdata}}\n)?%{NEWDATA:xml}\n\n\n`,
	}
	assert.Equal(t, expcf, newcf)

}
