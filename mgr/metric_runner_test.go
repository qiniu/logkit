package mgr

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"bufio"
	"bytes"
	"io"
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/metric/system"
	"github.com/stretchr/testify/assert"
)

const (
	bufSize = 1024 * 1024
)

func TestMetricRun(t *testing.T) {
	var testMetricConf = `{
		"name":"test1",
		"batch_len": 1,
		"batch_size": 20,
		"batch_interval": 60,
		"batch_try_times": 3,
		"collect_interval": 1,
		"metric": [
			{
				"type": "cpu",
				"attributes": {},
				"config": {
					"total_cpu": true,
					"per_cpu": false,
					"collect_cpu_time": true
				}
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
	os.RemoveAll("meta")
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
	confDir := pwd + "/" + dir
	logConfs := dir + "/confs"
	fileSenderData := dir + "/filesenderdata"
	fileSenderData1 := dir + "/filesenderdata1"
	if err := os.Mkdir(logConfs, 0755); err != nil {
		log.Fatalf("TestMetricRun error mkdir %v %v", logConfs, err)
	}
	err = ioutil.WriteFile(logConfs+"/test1.conf", []byte(testMetricConf), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	var conf ManagerConfig
	conf.RestDir = confDir
	conf.BindHost = ":6846"
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRestService(m, echo.New())
	defer func() {
		rs.Stop()
	}()
	time.Sleep(3 * time.Second)

	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test1", bytes.NewReader([]byte(testMetricConf)))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	// 停止 runner
	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test1/stop", bytes.NewReader([]byte{}))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(2 * time.Second)

	// 必须要引入 system 以便执行其中的 init
	log.Println(system.TypeMetricCpu)

	// 读取发送端文件，
	cpuAttr := system.KeyCpuUsages
	var curLine int64 = 0
	f, err := os.Open(fileSenderData)
	assert.NoError(t, err)
	br := bufio.NewReaderSize(f, bufSize)
	result := make([]map[string]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal %v curLine = %v %v", string(str), curLine, err)
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
	var testMetricConf1 = `{
		"name":"test1",
		"batch_len": 1,
		"batch_size": 20,
		"batch_interval": 60,
		"batch_try_times": 3,
		"collect_interval": 1,
		"metric": [
			{
				"type": "cpu",
				"attributes": {
					"cpu_usage_guest_nice": false
				},
				"config": {
					"total_cpu": true,
					"per_cpu": false,
					"collect_cpu_time": false
				}
			}
		],
		"senders":[{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": "./TestMetricRun/filesenderdata1"
		}]
	}`

	req, err = http.NewRequest(http.MethodPut, "http://127.0.0.1"+rs.address+"/logkit/configs/test1", bytes.NewReader([]byte(testMetricConf1)))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test1/stop", bytes.NewReader([]byte{}))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(2 * time.Second)

	curLine = 0
	f, err = os.Open(fileSenderData1)
	assert.NoError(t, err)
	br = bufio.NewReaderSize(f, bufSize)
	result = make([]map[string]interface{}, 0)
	for {
		str, _, c := br.ReadLine()
		if c == io.EOF {
			f.Close()
			break
		}
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, 1, len(result))
		assert.Equal(t, len(cpuAttr)/2, len(result[0]))
		curLine++
	}
}

func TestMetricNet(t *testing.T) {
	dir := "TestMetricNet"
	os.RemoveAll(dir)
	os.RemoveAll("meta")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestMetricNet error mkdir %v %v", dir, err)
	}
	defer func() {
		os.RemoveAll(dir)
		os.RemoveAll("meta")
	}()
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	fileSenderData := dir + "/filesenderdata"
	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6847"

	master, err := NewManager(conf)
	assert.NoError(t, err)
	rs := NewRestService(master, echo.New())

	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	var testMetricConf = `{
		"name":"test2",
		"batch_len": 1,
		"batch_size": 20,
		"batch_interval": 60,
		"batch_try_times": 3,
		"collect_interval": 1,
		"metric": [
			{
				"type": "net",
				"config": {
					"interfaces": ["!223@#$%"]
				}
			}
		],
		"senders":[{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": "./TestMetricNet/filesenderdata"
		}]
	}`

	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test2", bytes.NewReader([]byte(testMetricConf)))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test2/stop", bytes.NewReader([]byte{}))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(2 * time.Second)

	var curLine int64 = 0
	f, err := os.Open(fileSenderData)
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
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, 1, len(result))
	}
}

func TestMetricDiskio(t *testing.T) {
	dir := "TestMetricDiskio"
	os.RemoveAll(dir)
	os.RemoveAll("meta")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("TestMetricDiskio error mkdir %v %v", dir, err)
	}
	defer func() {
		os.RemoveAll(dir)
		os.RemoveAll("meta")
	}()
	pwd, err := os.Getwd()
	assert.NoError(t, err)
	confdir := pwd + "/" + dir
	fileSenderData := dir + "/filesenderdata"
	fileSenderData1 := dir + "/filesenderdata1"
	var conf ManagerConfig
	conf.RestDir = confdir
	conf.BindHost = ":6848"

	master, err := NewManager(conf)
	assert.NoError(t, err)
	rs := NewRestService(master, echo.New())

	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
		os.RemoveAll(".logkitconfs")
	}()

	var testMetricConf = `{
		"name":"test3",
		"batch_len": 1,
		"batch_size": 20,
		"batch_interval": 60,
		"batch_try_times": 3,
		"collect_interval": 3,
		"metric": [
			{
				"type": "diskio",
				"config": {
					"skip_serial_number": false
				}
			}
		],
		"senders":[{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": "./TestMetricDiskio/filesenderdata"
		}]
	}`

	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test3", bytes.NewReader([]byte(testMetricConf)))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(4 * time.Second)

	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test3/stop", bytes.NewReader([]byte{}))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	var curLine int64 = 0
	diskIoAttr := system.KeyDiskioUsages
	f, err := os.Open(fileSenderData)
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
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, len(diskIoAttr)+2, len(result[0]))
	}

	var testMetricConf1 = `{
		"name":"test3",
		"batch_len": 1,
		"batch_size": 20,
		"batch_interval": 60,
		"batch_try_times": 3,
		"collect_interval": 3,
		"metric": [
			{
				"type": "diskio",
				"attributes": {
					"diskio_write_time": false
				},
				"config": {
					"skip_serial_number": true
				}
			}
		],
		"senders":[{
			"name":           "file_sender",
			"sender_type":    "file",
			"file_send_path": "./TestMetricDiskio/filesenderdata1"
		}]
	}`

	req, err = http.NewRequest(http.MethodPut, "http://127.0.0.1"+rs.address+"/logkit/configs/test3", bytes.NewReader([]byte(testMetricConf1)))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(4 * time.Second)

	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1"+rs.address+"/logkit/configs/test3/stop", bytes.NewReader([]byte{}))
	if err != nil {
		t.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	content, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Error(string(content))
	}
	time.Sleep(5 * time.Second)

	curLine = 0
	f, err = os.Open(fileSenderData1)
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
		err = json.Unmarshal([]byte(str), &result)
		if err != nil {
			log.Fatalf("Test_Run error unmarshal %v curLine = %v %v", string(str), curLine, err)
		}
		assert.Equal(t, len(diskIoAttr), len(result[0]))
	}
}
