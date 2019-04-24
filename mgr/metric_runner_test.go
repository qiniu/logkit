package mgr

import (
	"bufio"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/metric/curl"
	"github.com/qiniu/logkit/metric/system"
	senderConf "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	bufSize = 1024 * 1024
)

func getMetricRunnerConfig(name string, mc []MetricConfig, senderPath string) ([]byte, error) {
	runnerConf := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       name,
			CollectInterval:  1,
			MaxBatchInterval: 1,
		},
		MetricConfig: mc,
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": senderPath,
		}},
	}
	return jsoniter.Marshal(runnerConf)
}

func TestMetricRunner(t *testing.T) {
	t.Parallel()
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
	time.Sleep(time.Millisecond)
	c := make(chan string)
	defer func() {
		close(c)
		rs.Stop()
		os.RemoveAll(rootDir)
		os.Remove(StatsShell)
		os.RemoveAll("meta")
	}()

	funcMap := map[string]func(*testParam){
		"metricRunTest":       metricRunTest,
		"metricNetTest":       metricNetTest,
		"metricDiskioTest":    metricDiskioTest,
		"metricHttpTest":      metricHttpTest,
		"metricRunEnvTagTest": metricRunEnvTagTest,
		"metricRunErrTest":    metricRunErrTest,
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
	time.Sleep(time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	// 必须要引入 system 以便执行其中的 init
	log.Println(system.TypeMetricCpu)

	// 读取发送端文件，
	cpuAttr := system.KeyCpuUsages
	var curLine int64
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
	time.Sleep(time.Second)
	base := filepath.Base("")
	metaPath := "meta/" + runnerName + "_" + Hash(base)
	t.Log("metaPath: ", metaPath)
	for i := 0; !utils.IsExist(metaPath) && i < 6; i++ {
		time.Sleep(500 * time.Millisecond)
		i++
	}
	assert.True(t, utils.IsExist(metaPath))

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

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
		assert.Equal(t, len(cpuAttr)/2, len(result[0])) //cpu_usage_guest_nice为false
		curLine++
	}

	// 删除 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodDelete, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)
	assert.False(t, utils.IsExist(metaPath))
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
	time.Sleep(time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	var curLine int64
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

	// 重置 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/reset"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
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
	time.Sleep(time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	var curLine int64
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
		assert.Equal(t, len(diskIoAttr)+1, len(result[0]), string(str))
	}
	mc = []MetricConfig{
		{
			MetricType: "diskio",
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
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

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

func metricRunEnvTagTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData"
	runnerName := "metricRunEnvTagTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	if err := mkTestDir(testDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	originEnv := os.Getenv(runnerName)
	defer func() {
		os.Setenv(runnerName, originEnv)
	}()
	if err := os.Setenv(runnerName, "{\""+runnerName+"\":\"env_value\"}"); err != nil {
		t.Fatalf("metricRunEnvTagTest set env error %v", err)
	}
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
	rc := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       runnerName,
			CollectInterval:  1,
			MaxBatchInterval: 1,
			EnvTag:           runnerName,
		},
		MetricConfig: mc,
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": resvPath1,
		}},
	}
	runnerConf, err := jsoniter.Marshal(rc)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 添加 runner
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	// 必须要引入 system 以便执行其中的 init
	log.Println(system.TypeMetricCpu)

	// 读取发送端文件，
	f, err := os.Open(resvPath1)
	assert.NoError(t, err)
	defer f.Close()
	br := bufio.NewReaderSize(f, bufSize)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		result := make([]map[string]interface{}, 0)
		err = jsoniter.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("metricRunEnvTagTest error unmarshal %v %v", string(str), err)
		}
		for _, d := range result {
			if v, ok := d[runnerName]; !ok {
				t.Fatalf("metricRunEnvTagTest error, exp got Test_RunForEnvTag:env_value, but not found")
			} else {
				assert.Equal(t, "env_value", v)
			}
		}
	}
}

func metricHttpTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData1"
	resvName2 := "sendData2"
	resvName3 := "sendData3"
	runnerName := "metricHttpTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	resvPath2 := filepath.Join(resvDir, resvName2)
	resvPath3 := filepath.Join(resvDir, resvName3)
	if err := mkTestDir(testDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "<html><body>七牛云</body></html>")
	}))
	defer ts.Close()
	mc := []MetricConfig{
		{
			MetricType: "http",
			Attributes: map[string]bool{
				"http_resp_head": false,
				"http_data":      false,
			},
			Config: map[string]interface{}{
				"http_datas": `[{"method":"GET", "url":"` + ts.URL + `", "expect_code":200, "expect_data":"七牛云"}]`,
			},
		},
	}
	rc := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       runnerName,
			CollectInterval:  60,
			MaxBatchInterval: 60,
			EnvTag:           runnerName,
		},
		MetricConfig: mc,
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": resvPath1,
		}},
	}
	runnerConf, err := jsoniter.Marshal(rc)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 添加 runner
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Millisecond)

	var curLine int64
	httpAttr := curl.KeyHttpUsages
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
		err = jsoniter.Unmarshal(str, &result)
		if err != nil {
			t.Fatalf("metricHttpTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}

		assert.Equal(t, len(httpAttr)+2, len(result[0]), string(str))
		assert.Equal(t, float64(200), result[0]["http__status_code_1"])
		assert.Equal(t, ts.URL, result[0]["http__target_1"])
		assert.Equal(t, float64(1), result[0]["http__err_state_total"])
		assert.Equal(t, "", result[0]["http__err_msg_total"])
		assert.Equal(t, float64(1), result[0]["http__err_state_total"])
	}

	mc2 := []MetricConfig{
		{
			MetricType: "http",
			Attributes: map[string]bool{
				"http_resp_head": false,
				"http_data":      false,
				"http_time_cost": false,
			},
			Config: map[string]interface{}{
				"http_datas": `[{"method":"GET", "url":"` + ts.URL + `", "expect_code":200, "expect_data":"七牛云"},{"method":"GET", "url":"https://www.logkit-pandora.com", "expect_code":200, "expect_data":"七牛云"}]`,
			},
		},
	}
	rc2 := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       runnerName,
			CollectInterval:  60,
			MaxBatchInterval: 60,
			EnvTag:           runnerName,
		},
		MetricConfig: mc2,
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": resvPath2,
		}},
	}
	runnerConf2, err := jsoniter.Marshal(rc2)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 更新
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodPut, runnerConf2)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Millisecond)

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
		err = jsoniter.Unmarshal(str, &result)
		if err != nil {
			t.Fatalf("metricHttpTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}

		assert.Equal(t, float64(200), result[0]["http__status_code_1"])
		assert.Equal(t, ts.URL, result[0]["http__target_1"])
		assert.Equal(t, float64(1), result[0]["http__err_state_1"])
		assert.Equal(t, float64(-1), result[0]["http__status_code_2"])
		assert.Equal(t, "https://www.logkit-pandora.com", result[0]["http__target_2"])
		assert.Equal(t, float64(0), result[0]["http__err_state_2"])
		assert.Equal(t, float64(0), result[0]["http__err_state_total"])
	}

	mc3 := []MetricConfig{
		{
			MetricType: "http",
			Attributes: map[string]bool{
				"http_resp_head": false,
				"http_data":      false,
				"http_time_cost": false,
				"http_err_msg":   false,
			},
			Config: map[string]interface{}{
				"http_datas": `[{"method":"GET", "url":"` + ts.URL + `", "expect_code":200, "expect_data":"潘多拉"}]`,
			},
		},
	}
	rc3 := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       runnerName,
			CollectInterval:  60,
			MaxBatchInterval: 60,
			EnvTag:           runnerName,
		},
		MetricConfig: mc3,
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": resvPath3,
		}},
	}
	runnerConf3, err := jsoniter.Marshal(rc3)
	if err != nil {
		t.Fatalf("get runner config failed, error is %v", err)
	}

	// 更新
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodPut, runnerConf3)
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(3 * time.Second)

	// 停止 runner
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName + "/stop"
	respCode, respBody, err = makeRequest(url, http.MethodPost, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	time.Sleep(time.Second)

	curLine = 0
	f, err = os.Open(resvPath3)
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
		err = jsoniter.Unmarshal(str, &result)
		if err != nil {
			t.Fatalf("metricHttpTest error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}

		assert.Equal(t, float64(200), result[0]["http__status_code_1"])
		assert.Equal(t, ts.URL, result[0]["http__target_1"])
		assert.Equal(t, float64(0), result[0]["http__err_state_1"])
		assert.Equal(t, float64(0), result[0]["http__err_state_total"])
		assert.Equal(t, "don't contain: 潘多拉", result[0]["http__err_msg_total"])
	}
}

func metricRunErrTest(p *testParam) {
	t := p.t
	rd := p.rd
	rs := p.rs
	resvName1 := "sendData"
	runnerName := "metricRunErrTest"
	dir := runnerName + "Dir"
	testDir := filepath.Join(rd, dir)
	resvDir := filepath.Join(testDir, "sender")
	resvPath1 := filepath.Join(resvDir, resvName1)
	if err := mkTestDir(testDir, resvDir); err != nil {
		t.Fatalf("mkdir test path error %v", err)
	}

	mc := []MetricConfig{
		{
			MetricType: "metric_test",
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

	// 添加 runner 失败
	url := "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err := makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, respCode)
	var got1 respDataMessage
	if err = jsoniter.Unmarshal(respBody, &got1); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.EqualValues(t, "L1002", got1.Code)
	assert.EqualValues(t, "forkRunner metricRunErrTest error NewRunner(metricRunErrTest) failed: no collectors were added", got1.Message)

	// 添加 runner 失败
	runnerConfStruct := RunnerConfig{
		RunnerInfo: RunnerInfo{
			RunnerName:       "metricRunErrTest",
			MaxBatchInterval: 1,
		},
		SendersConfig: []conf.MapConf{{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": "",
		}},
	}
	runnerConf, _ = jsoniter.Marshal(runnerConfStruct)
	url = "http://127.0.0.1" + rs.address + "/logkit/configs/" + runnerName
	respCode, respBody, err = makeRequest(url, http.MethodPost, runnerConf)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, respCode)
	var got2 respDataMessage
	if err = jsoniter.Unmarshal(respBody, &got2); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.EqualValues(t, "L1002", got2.Code)
	assert.EqualValues(t, "forkRunner metricRunErrTest error NewRunner(metricRunErrTest) failed: get logpath in new meta error MissingKey: The configs must contains log_path, dataType must be string", got2.Message)
}

func TestSendType(t *testing.T) {
	t.Parallel()
	abc := make(map[string]string)
	if abc["abc"] == "hello" {
		t.Errorf("xx")
	}
	abc["type"] = "pandora"
	if abc[senderConf.KeySenderType] == senderConf.TypePandora {
		t.Errorf("type should equal")
	}
}
