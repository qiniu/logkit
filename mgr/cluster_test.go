package mgr

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/router"
)

type respSlave struct {
	Code string  `json:"code"`
	Data []Slave `json:"data"`
}

type respClusterStatus struct {
	Code string                   `json:"code"`
	Data map[string]ClusterStatus `json:"data"`
}

type respSlaveConfig struct {
	Code string                 `json:"code"`
	Data map[string]SlaveConfig `json:"data"`
}

const (
	logkitCount = 4
)

type testCluParam struct {
	rd string
	t  *testing.T
	rs [logkitCount]*RestService
}

func getClusterRunnerStatus(rn, lp, rs string, rdc, rds, pe, ps, se, ss int64, tag, url string) map[string]RunnerStatus {
	runnerStatus := getRunnerStatus(rn, lp, rs, "file_sender", "", rdc, rds, pe, ps, se, ss)
	for k, v := range runnerStatus {
		v.Tag = tag
		v.Url = url
		runnerStatus[k] = v
	}
	return runnerStatus
}

func TestClusterRest(t *testing.T) {
	t.Parallel()
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confDirName := "confs"
	rootDirName := "testClusterRest"
	testRoot := filepath.Join(pwd, rootDirName)
	os.RemoveAll(testRoot)
	if err := mkTestDir(testRoot); err != nil {
		t.Fatalf("TestClusterRest mkdir error %v", err)
	}

	port := 6201
	rs := [logkitCount]*RestService{}
	tag := [logkitCount]string{"test", "test", "test", "test_change"}
	logkitName := [logkitCount]string{"master", "slave1", "slave2", "slave3"}
	for i := 0; i < logkitCount; i++ {
		lName := logkitName[i]
		rootDir := filepath.Join(testRoot, lName)
		confDir := filepath.Join(rootDir, confDirName)
		if err := mkTestDir(rootDir, confDir); err != nil {
			t.Fatalf("TestClusterRest mkdir error %v", err)
		}
		var logkitConf ManagerConfig
		logkitConf.RestDir = confDir
		logkitConf.BindHost = "127.0.0.1:" + strconv.Itoa(port+i)
		logkitConf.Cluster.Enable = true
		if i == 0 {
			logkitConf.Cluster.IsMaster = true
		} else {
			logkitConf.Cluster.MasterUrl = []string{rs[0].address}
		}
		m, err := NewManager(logkitConf)
		if err != nil {
			t.Fatal(err)
		}
		r := NewRestService(m, echo.New())
		time.Sleep(100 * time.Millisecond)
		if i != 0 {
			r.cluster.Tag = tag[i]
			if err = Register(r.cluster.MasterUrl, r.cluster.Address, tag[i]); err != nil {
				t.Fatalf("register master failed error is %v", err)
			}
		}
		rs[i] = r
	}
	c := make(chan string)
	defer func() {
		close(c)
		os.Remove(StatsShell)
		os.RemoveAll(testRoot)
		for _, r := range rs {
			if r != nil {
				r.Stop()
			}
		}
	}()

	// 删除 slave
	clusterSlavesDeleteTest(&testCluParam{testRoot, t, rs})
	// 重新注册
	for i := 1; i < logkitCount; i++ {
		if err = Register(rs[i].cluster.MasterUrl, rs[i].cluster.Address, rs[i].cluster.Tag); err != nil {
			t.Fatalf("register master failed, error is %v", err)
		}
	}
	// 测试更改 tag
	changeTagsTest(&testCluParam{testRoot, t, rs})
	// 重新注册
	for i := 1; i < logkitCount; i++ {
		rs[i].cluster.Tag = tag[i]
		if err = Register(rs[i].cluster.MasterUrl, rs[i].cluster.Address, rs[i].cluster.Tag); err != nil {
			t.Fatalf("register master failed, error is %v", err)
		}
	}

	funcMap := map[string]func(*testCluParam){
		"clusterUpdateTest":       clusterUpdateTest,
		"clusterStartStopTest":    clusterStartStopTest,
		"clusterResetDeleteTest":  clusterResetDeleteTest,
		"clusterSalveConfigsTest": clusterSalveConfigsTest,
		"getSlavesRunnerTest":     getSlavesRunnerTest,
		"getSlaveConfigTest":      getSlaveConfigTest,
	}

	for k, f := range funcMap {
		go func(k string, f func(*testCluParam), c chan string) {
			f(&testCluParam{t: t, rs: rs, rd: testRoot})
			c <- k
		}(k, f, c)
	}
	funcCnt := len(funcMap)
	for i := 0; i < funcCnt; i++ {
		<-c
	}
}

func clusterUpdateTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	resvName := "sendData"
	runnerName := "clusterUpdateTest"
	dir := runnerName + "Dir"
	rootDir := filepath.Join(rd, dir)
	metaDir := filepath.Join(rootDir, "meta")
	logDir := filepath.Join(rootDir, "logdir")
	logPath := filepath.Join(logDir, "log1")
	resvDir := filepath.Join(rootDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(rootDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log string to file failed error is %v", err)
	}
	time.Sleep(time.Millisecond)
	mode := ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 测试增加 runner, tag = test
	url := rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 获取 status , tag = test
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respGotStatus respClusterStatus
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus := respGotStatus.Data

	sts1 := getClusterRunnerStatus(runnerName, logDir, RunnerRunning, 1, 29, 0, 1, 0, 1, rs[1].cluster.Tag, rs[1].cluster.Address)
	sts2 := getClusterRunnerStatus(runnerName, logDir, RunnerRunning, 1, 29, 0, 1, 0, 1, rs[2].cluster.Tag, rs[2].cluster.Address)

	expStatus := make(map[string]ClusterStatus)
	expStatus[rs[1].cluster.Address] = ClusterStatus{
		Status: sts1,
		Tag:    rs[1].cluster.Tag,
	}
	expStatus[rs[2].cluster.Address] = ClusterStatus{
		Status: sts2,
		Tag:    rs[2].cluster.Tag,
	}

	actStatus := make(map[string]ClusterStatus)
	gs, ok := gotStatus[rs[1].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok := gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[1].cluster.Address] = gs

	gs, ok = gotStatus[rs[2].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[2].cluster.Address] = gs
	assert.Equal(t, len(expStatus), len(actStatus))
	for key, ex := range expStatus {
		act, ok := actStatus[key]
		assert.Equal(t, true, ok)
		assert.Equal(t, ex, act)
	}

	// 测试 update runner
	mode = ModeFile
	updateConf, err := getRunnerConfig(runnerName, logPath, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 更新 runner, tag = test
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPut, updateConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 尝试更改一个不存在的 slave
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag + "&url=" + rs[3].cluster.Address
	respCode, respBody, err = makeRequest(url, http.MethodPut, updateConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusBadRequest, respCode)

	// 上述配置文件改变了 logPath, 所以下面验证 status 中的 logPath 是否正确
	// tag == "" && url == ""
	url = rs[0].cluster.Address + "/logkit/cluster/status"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus = respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus = respGotStatus.Data

	cluStatus, exist := gotStatus[rs[1].cluster.Address]
	assert.Equal(t, true, exist)
	runnerStatus, exist := cluStatus.Status[runnerName]
	assert.Equal(t, true, exist)
	assert.Equal(t, logPath, runnerStatus.Logpath)

	cluStatus, exist = gotStatus[rs[2].cluster.Address]
	assert.Equal(t, true, exist)
	runnerStatus, exist = cluStatus.Status[runnerName]
	assert.Equal(t, true, exist)
	assert.Equal(t, logPath, runnerStatus.Logpath)

	// 没有被更改的 slave 3 仍然是原来的 logPath
	cluStatus, exist = gotStatus[rs[3].cluster.Address]
	assert.Equal(t, true, exist)
	runnerStatus, exist = cluStatus.Status[runnerName]
	assert.Equal(t, true, exist)
	assert.Equal(t, logDir, runnerStatus.Logpath)
}

func clusterStartStopTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	resvName := "sendData"
	runnerName := "clusterStartStopTest"
	dir := runnerName + "Dir"
	rootDir := filepath.Join(rd, dir)
	metaDir := filepath.Join(rootDir, "meta")
	logDir := filepath.Join(rootDir, "logdir")
	resvDir := filepath.Join(rootDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(rootDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log string to file failed error is %v", err)
	}
	time.Sleep(time.Millisecond)
	mode := ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 测试增加 runner, tag = test
	url := rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 获取 status , tag = test
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respGotStatus respClusterStatus
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus := respGotStatus.Data

	sts1 := getClusterRunnerStatus(runnerName, logDir, RunnerRunning, 1, 29, 0, 1, 0, 1, rs[1].cluster.Tag, rs[1].cluster.Address)
	sts2 := getClusterRunnerStatus(runnerName, logDir, RunnerRunning, 1, 29, 0, 1, 0, 1, rs[2].cluster.Tag, rs[2].cluster.Address)
	sts3 := getClusterRunnerStatus(runnerName, logDir, RunnerRunning, 1, 29, 0, 1, 0, 1, rs[3].cluster.Tag, rs[3].cluster.Address)

	expStatus := make(map[string]ClusterStatus)
	expStatus[rs[1].cluster.Address] = ClusterStatus{
		Status: sts1,
		Tag:    rs[1].cluster.Tag,
	}
	expStatus[rs[2].cluster.Address] = ClusterStatus{
		Status: sts2,
		Tag:    rs[2].cluster.Tag,
	}

	actStatus := make(map[string]ClusterStatus)
	gs, ok := gotStatus[rs[1].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok := gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[1].cluster.Address] = gs

	gs, ok = gotStatus[rs[2].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[2].cluster.Address] = gs
	assert.Equal(t, len(expStatus), len(actStatus))
	for key, ex := range expStatus {
		act, ok := actStatus[key]
		assert.Equal(t, true, ok)

		assert.Equal(t, ex, act)
	}

	// 测试 stop runner tag=test
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "/stop?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	// 停止后，tag == 'test' 的 status runningStatus 为 stopped
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus = respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus = respGotStatus.Data

	stsStop1 := getClusterRunnerStatus(runnerName, "", RunnerStopped, 0, 0, 0, 0, 0, 0, rs[1].cluster.Tag, rs[1].cluster.Address)
	stsStop2 := getClusterRunnerStatus(runnerName, "", RunnerStopped, 0, 0, 0, 0, 0, 0, rs[2].cluster.Tag, rs[2].cluster.Address)
	stsStop3 := getClusterRunnerStatus(runnerName, "", RunnerStopped, 0, 0, 0, 0, 0, 0, rs[3].cluster.Tag, rs[3].cluster.Address)

	delete(stsStop1[runnerName].SenderStats, "file_sender")
	delete(stsStop2[runnerName].SenderStats, "file_sender")
	delete(stsStop3[runnerName].SenderStats, "file_sender")
	expStatus = make(map[string]ClusterStatus)
	expStatus[rs[1].cluster.Address] = ClusterStatus{
		Status: stsStop1,
		Tag:    rs[1].cluster.Tag,
	}
	expStatus[rs[2].cluster.Address] = ClusterStatus{
		Status: stsStop2,
		Tag:    rs[2].cluster.Tag,
	}

	actStatus = make(map[string]ClusterStatus)
	gs, ok = gotStatus[rs[1].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[1].cluster.Address] = gs

	gs, ok = gotStatus[rs[2].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[2].cluster.Address] = gs
	assert.Equal(t, len(expStatus), len(actStatus))
	for key, ex := range expStatus {
		act, ok := actStatus[key]
		assert.Equal(t, true, ok)
		assert.Equal(t, ex, act)
	}

	// 再次停止会有错误
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "/stop?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.NotEqual(t, http.StatusOK, respCode)

	// tag == 'test_changed' 的 status 依然正常
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[3].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus = respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus = respGotStatus.Data

	expStatus = make(map[string]ClusterStatus)
	expStatus[rs[3].cluster.Address] = ClusterStatus{
		Status: sts3,
		Tag:    rs[3].cluster.Tag,
	}

	actStatus = make(map[string]ClusterStatus)
	gs, ok = gotStatus[rs[3].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[3].cluster.Address] = gs

	assert.Equal(t, len(expStatus), len(actStatus))
	for key, ex := range expStatus {
		act, ok := actStatus[key]
		assert.Equal(t, true, ok)
		assert.Equal(t, ex, act)
	}

	// 测试 runner 启动
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "/start?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// tag == 'test' 的 status 恢复
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus = respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus = respGotStatus.Data

	expStatus = make(map[string]ClusterStatus)
	expStatus[rs[1].cluster.Address] = ClusterStatus{
		Status: sts1,
		Tag:    rs[1].cluster.Tag,
	}
	expStatus[rs[2].cluster.Address] = ClusterStatus{
		Status: sts2,
		Tag:    rs[2].cluster.Tag,
	}

	actStatus = make(map[string]ClusterStatus)
	gs, ok = gotStatus[rs[1].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	v.ReadDataSize = sts1[runnerName].ReadDataSize
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[1].cluster.Address] = gs

	gs, ok = gotStatus[rs[2].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	v.ReadDataSize = sts1[runnerName].ReadDataSize
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[2].cluster.Address] = gs
	assert.Equal(t, len(expStatus), len(actStatus))
	for key, ex := range expStatus {
		act, ok := actStatus[key]
		assert.Equal(t, true, ok)
		assert.Equal(t, ex, act)
	}
	// 再次开启，会报错
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "/start?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.NotEqual(t, http.StatusOK, respCode)

	// 测试 stop runner url
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "/stop?url=" + rs[3].cluster.Address
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	// tag == "test_change" status 状态为 stopped
	url = rs[0].cluster.Address + "/logkit/cluster/status?url=" + rs[3].cluster.Address
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus = respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus = respGotStatus.Data

	expStatus = make(map[string]ClusterStatus)
	expStatus[rs[3].cluster.Address] = ClusterStatus{
		Status: stsStop3,
		Tag:    rs[3].cluster.Tag,
	}

	actStatus = make(map[string]ClusterStatus)
	gs, ok = gotStatus[rs[3].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	v, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
	clearGotStatus(&v)
	gs.Status = make(map[string]RunnerStatus)
	gs.Status[runnerName] = v
	actStatus[rs[3].cluster.Address] = gs

	assert.Equal(t, len(expStatus), len(actStatus))
	for key, ex := range expStatus {
		act, ok := actStatus[key]
		assert.Equal(t, true, ok)
		assert.Equal(t, ex, act)
	}
}

func clusterResetDeleteTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	resvName := "sendData"
	runnerName := "clusterResetDeleteTest"
	dir := runnerName + "Dir"
	rootDir := filepath.Join(rd, dir)
	metaDir := filepath.Join(rootDir, "meta")
	logDir := filepath.Join(rootDir, "logdir")
	resvDir := filepath.Join(rootDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(rootDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log string to file failed error is %v", err)
	}
	time.Sleep(time.Millisecond)
	mode := ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 测试增加 runner, tag = test
	url := rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(10 * time.Second)

	// 读取日志发送目的文件，记录日志条数
	dataLine := 0
	f, err := os.Open(resvPath)
	assert.Nil(t, err)
	br := bufio.NewReader(f)
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		t.Log("line: ", string(line))
		dataLine++
	}
	f.Close()

	// reset tag == 'test' 的 slave
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "/reset?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(5 * time.Second)

	// 重置之后, 日志发送目的文件中的日志条数应该增加
	dataLine1 := 0
	f, err = os.Open(resvPath)
	assert.Nil(t, err)
	br = bufio.NewReader(f)
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		t.Log("line: ", string(line))
		dataLine1++
	}
	f.Close()
	assert.Equal(t, dataLine+2, dataLine1)

	// 测试删除 runner tag = test
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodDelete, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	// 删除后，status 返回为空
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus := respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus := respGotStatus.Data

	gs, ok := gotStatus[rs[1].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	_, ok = gs.Status[runnerName]
	assert.Equal(t, false, ok)

	gs, ok = gotStatus[rs[2].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	_, ok = gs.Status[runnerName]
	assert.Equal(t, false, ok)

	// 再次删除会有错误
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.NotEqual(t, http.StatusOK, respCode)

	// 获取 status , tag = test_change
	url = rs[0].cluster.Address + "/logkit/cluster/status?tag=" + rs[3].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotStatus = respClusterStatus{}
	err = jsoniter.Unmarshal(respBody, &respGotStatus)
	assert.NoError(t, err, string(respBody))
	gotStatus = respGotStatus.Data

	gs, ok = gotStatus[rs[3].cluster.Address]
	assert.Equal(t, true, ok, string(respBody))
	_, ok = gs.Status[runnerName]
	assert.Equal(t, true, ok)
}

func clusterSalveConfigsTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	resvName := "sendData"
	runnerName := "clusterSalveConfigsTest"
	dir := runnerName + "Dir"
	rootDir := filepath.Join(rd, dir)
	metaDir := filepath.Join(rootDir, "meta")
	logDir := filepath.Join(rootDir, "logdir")
	resvDir := filepath.Join(rootDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(rootDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log string to file failed error is %v", err)
	}
	time.Sleep(time.Millisecond)
	mode := ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 测试增加 runner, tag = test
	url := rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	// 测试获取 slave configs tag = test
	url = rs[0].cluster.Address + "/logkit/cluster/configs?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotConfigs := respSlaveConfig{}
	err = jsoniter.Unmarshal(respBody, &respGotConfigs)

	assert.NoError(t, err, string(respBody))
	gotConfigs := respGotConfigs.Data

	rc := RunnerConfig{}
	err = jsoniter.Unmarshal(runnerConf, &rc)
	assert.NoError(t, err)

	sc, ok := gotConfigs[rs[1].cluster.Address]
	assert.Equal(t, true, ok)
	scc, ok := sc.Configs[rs[1].mgr.RestDir+"/"+runnerName+".conf"]
	assert.Equal(t, true, ok)
	assert.Equal(t, rc.RunnerName, scc.RunnerName)
	assert.Equal(t, rc.MaxBatchLen, scc.MaxBatchLen)
	assert.Equal(t, rc.MaxBatchInterval, scc.MaxBatchInterval)
	assert.Equal(t, rc.SendersConfig[0]["file_send_path"], scc.SendersConfig[0]["file_send_path"])

	sc, ok = gotConfigs[rs[2].cluster.Address]
	assert.Equal(t, true, ok)
	scc, ok = sc.Configs[rs[2].mgr.RestDir+"/"+runnerName+".conf"]
	assert.Equal(t, true, ok)
	assert.Equal(t, rc.RunnerName, scc.RunnerName)
	assert.Equal(t, rc.MaxBatchLen, scc.MaxBatchLen)
	assert.Equal(t, rc.MaxBatchInterval, scc.MaxBatchInterval)
	assert.Equal(t, rc.SendersConfig[0]["file_send_path"], scc.SendersConfig[0]["file_send_path"])

	// tag 为空
	url = rs[0].cluster.Address + "/logkit/cluster/configs?tag="
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotConfigs = respSlaveConfig{}
	err = jsoniter.Unmarshal(respBody, &respGotConfigs)
	assert.NoError(t, err, string(respBody))
	gotConfigs = respGotConfigs.Data
	assert.Equal(t, logkitCount-1, len(gotConfigs))

	// url 方式
	url = rs[0].cluster.Address + "/logkit/cluster/configs?url=" + rs[3].cluster.Address
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGotConfigs = respSlaveConfig{}
	err = jsoniter.Unmarshal(respBody, &respGotConfigs)
	assert.NoError(t, err, string(respBody))
	gotConfigs = respGotConfigs.Data

	rc = RunnerConfig{}
	err = jsoniter.Unmarshal([]byte(runnerConf), &rc)
	assert.NoError(t, err)

	sc, ok = gotConfigs[rs[3].cluster.Address]
	assert.Equal(t, true, ok)
	scc, ok = sc.Configs[rs[3].mgr.RestDir+"/"+runnerName+".conf"]
	assert.Equal(t, true, ok)
	assert.Equal(t, rc.RunnerName, scc.RunnerName)
	assert.Equal(t, rc.MaxBatchLen, scc.MaxBatchLen)
	assert.Equal(t, rc.MaxBatchInterval, scc.MaxBatchInterval)
	assert.Equal(t, rc.SendersConfig[0]["file_send_path"], scc.SendersConfig[0]["file_send_path"])

	sc, ok = gotConfigs[rs[1].cluster.Address]
	assert.Equal(t, false, ok)
	sc, ok = gotConfigs[rs[2].cluster.Address]
	assert.Equal(t, false, ok)
}

func changeTagsTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	dir := "changeTagsTest"
	rootDir := filepath.Join(rd, dir)
	if err := mkTestDir(rootDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	// 测试通过 master 改变 slave tag
	req := TagReq{Tag: "test-test"}
	marshaled, err := jsoniter.Marshal(req)
	assert.NoError(t, err)
	url := rs[0].cluster.Address + "/logkit/cluster/slaves/tag?tag=" + rs[1].cluster.Tag
	respCode, respBody, err := makeRequest(url, http.MethodPost, marshaled)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)

	url = rs[0].cluster.Address + "/logkit/cluster/slaves?tag="
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respGetSlaves respSlave
	err = jsoniter.Unmarshal(respBody, &respGetSlaves)
	assert.NoError(t, err)
	getSlaves := respGetSlaves.Data
	for i := range getSlaves {
		getSlaves[i].LastTouch = time.Time{}
	}

	url = rs[0].cluster.Address + "/logkit/cluster/slaves/tag?url=" + rs[3].cluster.Address
	respCode, respBody, err = makeRequest(url, http.MethodPost, marshaled)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)

	url = rs[0].cluster.Address + "/logkit/cluster/slaves"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGetSlaves = respSlave{}
	err = jsoniter.Unmarshal(respBody, &respGetSlaves)
	assert.NoError(t, err)
	getSlaves = respGetSlaves.Data
	for i := range getSlaves {
		getSlaves[i].LastTouch = time.Time{}
	}

	// 测试通过 master 改变 slave tag
	url = rs[0].cluster.Address + "/logkit/cluster/slaves/tag?tag=" + rs[1].cluster.Tag
	tmp := rs[0].cluster
	rs[0].cluster = nil
	respCode, respBody, err = makeRequest(url, http.MethodPost, marshaled)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, respCode)
	var got respDataMessage
	err = jsoniter.Unmarshal(respBody, &got)
	assert.Nil(t, err)
	assert.EqualValues(t, "L2013", got.Code)
	assert.EqualValues(t, "cluster function not configed", got.Message)
	rs[0].cluster = tmp
}

func clusterSlavesDeleteTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	dir := "clusterSlavesDeleteTest"
	rootDir := filepath.Join(rd, dir)
	if err := mkTestDir(rootDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	// 测试删除 master 上的 slave  tag = change_test
	url := rs[0].cluster.Address + "/logkit/cluster/slaves?tag=" + rs[3].cluster.Tag
	respCode, respBody, err := makeRequest(url, http.MethodDelete, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)

	slaves := make([]Slave, 0)
	slaves = append(slaves, Slave{Url: rs[1].cluster.Address, Tag: rs[1].cluster.Tag, Status: StatusOK})
	slaves = append(slaves, Slave{Url: rs[2].cluster.Address, Tag: rs[2].cluster.Tag, Status: StatusOK})

	url = rs[0].cluster.Address + "/logkit/cluster/slaves?tag="
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respGetSlaves respSlave
	err = jsoniter.Unmarshal(respBody, &respGetSlaves)
	assert.NoError(t, err)
	getSlaves := respGetSlaves.Data
	for i := range getSlaves {
		getSlaves[i].LastTouch = time.Time{}
	}
	assert.Equal(t, slaves, getSlaves)

	// 重新注册所有 slave
	for i := 1; i < logkitCount; i++ {
		if err = Register(rs[i].cluster.MasterUrl, rs[i].cluster.Address, rs[i].cluster.Tag); err != nil {
			t.Fatalf("register master failed error is %v", err)
		}
	}

	// 删除 rs[1] url = rs[1]
	url = rs[0].cluster.Address + "/logkit/cluster/slaves?url=" + rs[1].cluster.Address
	respCode, respBody, err = makeRequest(url, http.MethodDelete, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)

	slaves = make([]Slave, 0)
	slaves = append(slaves, Slave{Url: rs[2].cluster.Address, Tag: rs[2].cluster.Tag, Status: StatusOK})
	slaves = append(slaves, Slave{Url: rs[3].cluster.Address, Tag: rs[3].cluster.Tag, Status: StatusOK})

	url = rs[0].cluster.Address + "/logkit/cluster/slaves"
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respGetSlaves = respSlave{}
	err = jsoniter.Unmarshal(respBody, &respGetSlaves)
	assert.NoError(t, err)
	getSlaves = respGetSlaves.Data
	for i := range getSlaves {
		getSlaves[i].LastTouch = time.Time{}
	}
	assert.Equal(t, slaves, getSlaves)
}

func getSlavesRunnerTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	resvName := "sendData"
	runnerName := "getSlavesRunnerTest"
	dir := runnerName + "Dir"
	rootDir := filepath.Join(rd, dir)
	metaDir := filepath.Join(rootDir, "meta")
	logDir := filepath.Join(rootDir, "logdir")
	resvDir := filepath.Join(rootDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(rootDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log string to file failed error is %v", err)
	}
	time.Sleep(time.Millisecond)
	mode := ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 测试增加 runner, tag = test
	url := rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	// 获取 tag = test 的 runner name
	url = rs[0].cluster.Address + "/logkit/cluster/runners?tag="
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respRss respRunnersNameList
	err = jsoniter.Unmarshal(respBody, &respRss)
	assert.NoError(t, err, string(respBody))
	nameList := respRss.Data
	isExist := false
	for _, n := range nameList {
		if n == runnerName {
			isExist = true
		}
	}
	assert.Equal(t, true, isExist)

	// 获取 tag = test_change 的 runner name
	url = rs[0].cluster.Address + "/logkit/cluster/runners?tag=" + rs[3].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	respRss = respRunnersNameList{}
	err = jsoniter.Unmarshal(respBody, &respRss)
	assert.NoError(t, err, string(respBody))
	nameList = respRss.Data
	isExist = false
	for _, n := range nameList {
		if n == runnerName {
			isExist = true
		}
	}
	assert.Equal(t, false, isExist)
}

func getSlaveConfigTest(p *testCluParam) {
	t := p.t
	rs := p.rs
	rd := p.rd
	resvName := "sendData"
	runnerName := "getSlaveConfigTest"
	dir := runnerName + "Dir"
	rootDir := filepath.Join(rd, dir)
	metaDir := filepath.Join(rootDir, "meta")
	logDir := filepath.Join(rootDir, "logdir")
	resvDir := filepath.Join(rootDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(rootDir, logDir, metaDir, resvDir); err != nil {
		t.Fatalf("mkdir error %v", err)
	}

	log1 := `{"a":1,"b":2,"c":"3","d":"4"}`
	if err := writeLogFile([]string{log1}, logDir); err != nil {
		t.Fatalf("write log string to file failed error is %v", err)
	}
	time.Sleep(time.Millisecond)
	mode := ModeDir
	runnerConf, err := getRunnerConfig(runnerName, logDir, metaDir, mode, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed error is %v", err)
	}

	// 测试增加 runner, tag = test
	url := rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	// 获取 tag = test, runner 的 config
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[1].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	var respRss respRunnerConfig
	err = jsoniter.Unmarshal(respBody, &respRss)
	assert.NoError(t, err, string(respBody))
	respConfig := respRss.Data
	respConfig.CreateTime = ""

	expConfig := RunnerConfig{}
	err = jsoniter.Unmarshal([]byte(runnerConf), &expConfig)
	assert.NoError(t, err, runnerConf)

	expConfig.ReaderConfig["name"] = runnerName
	expConfig.ReaderConfig["runner_name"] = runnerName
	expConfig.ParserConf["runner_name"] = runnerName
	expConfig.SendersConfig[0]["runner_name"] = runnerName
	expConfig.IsInWebFolder = true

	assert.Equal(t, expConfig, respConfig)

	// 获取 tag = test_change, runner 的 config
	url = rs[0].cluster.Address + "/logkit/cluster/configs/" + runnerName + "?tag=" + rs[3].cluster.Tag
	respCode, respBody, err = makeRequest(url, http.MethodGet, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.NotEqual(t, http.StatusOK, respCode)
}

func TestJsoniterMashalUnmashal(t *testing.T) {
	t.Parallel()
	runnerConf := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       "xxx",
			MaxBatchLen:      1,
			MaxBatchSize:     200,
			CollectInterval:  1,
			MaxBatchInterval: 1,
			MaxBatchTryTimes: 3,
		},
		ReaderConfig: conf.MapConf{
			"log_path":      "sx",
			"meta_path":     "/xs/xs",
			"mode":          "dir",
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
			"file_send_path": "/xsxs",
		}},
		Router: router.RouterConfig{Routes: make(map[string]int)},
	}
	bt, err := jsoniter.Marshal(runnerConf)
	assert.NoError(t, err)
	var rc RunnerConfig
	err = jsoniter.Unmarshal(bt, &rc)
	assert.NoError(t, err)
	assert.Equal(t, runnerConf, rc)
	assert.Equal(t, "/xsxs", rc.SendersConfig[0]["file_send_path"])
}

func TestJsoniter(t *testing.T) {
	t.Parallel()
	respGotConfigs1, respGotConfigs2 := respSlaveConfig{}, respSlaveConfig{}
	var teststring = `{"code":"L200","data":{"http://192.168.0.106:6202":{"configs":{"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/slave1/confs/clusterSalveConfigsTest.conf":{"name":"clusterSalveConfigsTest","collect_interval":1,"batch_len":1,"batch_size":200,"batch_interval":1,"batch_try_times":3,"createtime":"2018-01-03T22:25:36.497442704+08:00","reader":{"ignore_hidden":"true","log_path":"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/clusterSalveConfigsTestDir/logdir","meta_path":"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/clusterSalveConfigsTestDir/meta","mode":"dir","name":"clusterSalveConfigsTest","read_from":"oldest","runner_name":"clusterSalveConfigsTest"},"parser":{"name":"json_parser","runner_name":"clusterSalveConfigsTest","type":"json"},"senders":[{"file_send_path":"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/clusterSalveConfigsTestDir/sender/sendData","name":"file_sender","runner_name":"clusterSalveConfigsTest","sender_type":"file"}],"router":{"router_key_name":"","router_match_type":"","router_default_sender":0,"router_routes":null},"web_folder":true}},"tag":"test","error":null},"http://192.168.0.106:6203":{"configs":{"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/slave2/confs/clusterSalveConfigsTest.conf":{"name":"clusterSalveConfigsTest","collect_interval":1,"batch_len":1,"batch_size":200,"batch_interval":1,"batch_try_times":3,"createtime":"2018-01-03T22:25:36.497453622+08:00","reader":{"ignore_hidden":"true","log_path":"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/clusterSalveConfigsTestDir/logdir","meta_path":"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/clusterSalveConfigsTestDir/meta","mode":"dir","name":"clusterSalveConfigsTest","read_from":"oldest","runner_name":"clusterSalveConfigsTest"},"parser":{"name":"json_parser","runner_name":"clusterSalveConfigsTest","type":"json"},"senders":[{"file_send_path":"/Users/sunjianbo/gopath/src/github.com/qiniu/logkit/mgr/testClusterRest/clusterSalveConfigsTestDir/sender/sendData","name":"file_sender","runner_name":"clusterSalveConfigsTest","sender_type":"file"}],"router":{"router_key_name":"","router_match_type":"","router_default_sender":0,"router_routes":null},"web_folder":true}},"tag":"test","error":null}}}`
	err := json.Unmarshal([]byte(teststring), &respGotConfigs1)
	assert.NoError(t, err)
	stjson := jsoniter.ConfigCompatibleWithStandardLibrary
	err = stjson.Unmarshal([]byte(teststring), &respGotConfigs2)
	assert.NoError(t, err)
	assert.Equal(t, respGotConfigs1, respGotConfigs2)
}
