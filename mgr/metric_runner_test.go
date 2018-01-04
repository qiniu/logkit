package mgr

import (
	"bufio"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/metric/system"
	"github.com/stretchr/testify/assert"
)

const (
	bufSize = 1024 * 1024
)

func getMetricRunnerConfig(name string, mc []MetricConfig, senderPath string) ([]byte, error) {
	runnerConf := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:      name,
			CollectInterval: 1,
			MaxBatchInteval: 1,
		},
		MetricConfig: mc,
		SenderConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": senderPath,
		}},
	}
	return jsoniter.Marshal(runnerConf)
}

func TestMetricRunner(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	confDirName := "confs"
	dirName := "testMetricRunner"
	rootDir := filepath.Join(pwd, dirName)
	confDir := filepath.Join(rootDir, confDirName)
	os.RemoveAll(rootDir)
	if err := mkTestDir(rootDir, confDir); err != nil {
		t.Fatalf("testMetricRunner mkdir error %v", err)
	}
	var logkitConf ManagerConfig
	logkitConf.RestDir = confDir
	logkitConf.BindHost = ":6401"
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
		os.RemoveAll("meta")
	}()

	funcMap := map[string]func(*testParam){
		"metricRunTest":    metricRunTest,
		"metricNetTest":    metricNetTest,
		"metricDiskioTest": metricDiskioTest,
	}

	for k, f := range funcMap {
		go func(k string, f func(*testParam), c chan string) {
			f(&testParam{rd: rootDir, t: t, rs: rs})
			c <- k
		}(k, f, c)
	}
	funcCnt := len(funcMap)
	for i := 0; i < funcCnt; i++ {
		<-c
	}
}

func metricRunTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData"
	resvName2 := "sendData1"
	runnerName := "metricRunTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	resvPath2 := filepath.Join(resvDir, resvName2)
	if err := mkTestDir(testDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	time.Sleep(1 * time.Second)
	mc := []MetricConfig{
		{
			MetricType: "cpu",
			Attributes: map[string]bool{},
			Config: map[string]interface{}{
				"total_cpu":        true,
				"per_cpu":          false,
				"collect_cpu_time": true,
			},
		},
	}
	runnerConf, err := getMetricRunnerConfig(runnerName, mc, resvPath1)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 添加 runner
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	// 必须要引入 system 以便执行其中的 init
	log.Println(system.TypeMetricCpu)

	// 读取发送端文件，
	cpuAttr := system.KeyCpuUsages
	var curLine int64 = 0
	f, err := os.Open(resvPath1)
	assert.NoError(t, err)
	br := bufio.NewReaderSize(f, bufSize)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		result := make([]map[string]interface{}, 0)
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("metricRunTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		curLine++
		if curLine == 1 {
			assert.Equal(t, len(cpuAttr)/2+1, len(result[0]))
		} else {
			assert.Equal(t, len(cpuAttr)/2+1, len(result[0]))
			assert.Equal(t, 2, len(result))
		}
	}

	// 更新 metrc, 同时更新配置
	mc = []MetricConfig{
		{
			MetricType: "cpu",
			Attributes: map[string]bool{
				"cpu_usage_guest_nice": false,
			},
			Config: map[string]interface{}{
				"total_cpu":        true,
				"per_cpu":          false,
				"collect_cpu_time": false,
			},
		},
	}
	runnerConf, err = getMetricRunnerConfig(runnerName, mc, resvPath2)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 更新
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodPut, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	curLine = 0
	f, err = os.Open(resvPath2)
	assert.NoError(t, err)
	br = bufio.NewReaderSize(f, bufSize)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		result := make([]map[string]interface{}, 0)
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("metricRunTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, 1, len(result))
		assert.Equal(t, len(cpuAttr)/2, len(result[0]))
		curLine++
	}
}

func metricNetTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName := "sendData"
	runnerName := "metricNetTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	resvDir := filepath.Join(testDir, "sender")
	resvPath := filepath.Join(resvDir, resvName)
	if err := mkTestDir(testDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	time.Sleep(1 * time.Second)
	mc := []MetricConfig{
		{
			MetricType: "net",
			Attributes: map[string]bool{},
			Config: map[string]interface{}{
				"interfaces": []string{"!223@#$%"},
			},
		},
	}
	runnerConf, err := getMetricRunnerConfig(runnerName, mc, resvPath)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 添加 runner
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	var curLine int64 = 0
	f, err := os.Open(resvPath)
	assert.NoError(t, err)
	br := bufio.NewReaderSize(f, bufSize)
	result := make([]map[string]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		curLine++
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("metricNetTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, 1, len(result))
	}
}

func metricDiskioTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData1"
	resvName2 := "sendData2"
	runnerName := "metricDiskioTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	resvPath2 := filepath.Join(resvDir, resvName2)
	if err := mkTestDir(testDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	time.Sleep(1 * time.Second)
	mc := []MetricConfig{
		{
			MetricType: "diskio",
			Attributes: map[string]bool{},
			Config: map[string]interface{}{
				"skip_serial_number": false,
			},
		},
	}
	runnerConf, err := getMetricRunnerConfig(runnerName, mc, resvPath1)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 添加 runner
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	var curLine int64 = 0
	diskIoAttr := system.KeyDiskioUsages
	f, err := os.Open(resvPath1)
	assert.NoError(t, err)
	br := bufio.NewReaderSize(f, bufSize)
	result := make([]map[string]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		curLine++
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("metricDiskioTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, len(diskIoAttr)+2, len(result[0]), string(str))
	}
	mc = []MetricConfig{
		{
			MetricType: "diskio",
			Attributes: map[string]bool{
				"diskio_write_time": false,
			},
			Config: map[string]interface{}{
				"skip_serial_number": true,
			},
		},
	}
	runnerConf, err = getMetricRunnerConfig(runnerName, mc, resvPath2)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 更新
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodPut, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(5 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(2 * time.Second)

	curLine = 0
	f, err = os.Open(resvPath2)
	assert.NoError(t, err)
	br = bufio.NewReaderSize(f, bufSize)
	result = make([]map[string]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		curLine++
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("metricDiskioTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, len(diskIoAttr), len(result[0]), string(str))
	}
}
