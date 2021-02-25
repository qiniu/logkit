package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	parserconf "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/reader"
	readerconf "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/reader/http"
	senderconf "github.com/qiniu/logkit/sender/config"
	. "github.com/qiniu/logkit/utils/models"
)

func Test_RawData(t *testing.T) {
	t.Parallel()
	var testRawData = `{
    "name":"testGetRawData.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./Test_RawData/logdir",
        "meta_path":"./Test_RawData1/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    }
}
`
	logfile := "./Test_RawData/logdir/log1"
	logdir := "./Test_RawData/logdir"
	if err := os.MkdirAll("./Test_RawData/confs1", 0777); err != nil {
		t.Error(err)
	}
	defer func() {
		os.RemoveAll("./Test_RawData")
		os.RemoveAll("./Test_RawData1")
	}()

	if err := os.MkdirAll(logdir, 0777); err != nil {
		t.Error(err)
	}
	err := createFile(logfile, 20000000)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./Test_RawData/confs1/test1.conf", []byte(testRawData), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Millisecond)
	confPathAbs, _, err := GetRealPath("./Test_RawData/confs1/test1.conf")
	if err != nil {
		t.Error(err)
	}

	var runnerConf RunnerConfig
	err = conf.LoadEx(&runnerConf, confPathAbs)
	if err != nil {
		t.Error(err)
	}

	rawData, err := RawData(runnerConf.ReaderConfig)
	if err != nil {
		t.Error(err)
	}

	expected := []string{"abc\n", "abc\n", "abc\n", "abc\n", "abc\n", "abc\n", "abc\n", "abc\n", "abc\n", "abc\n"}
	assert.Equal(t, expected, rawData)

	_, err = RawData(nil)
	assert.Error(t, err)

	runnerConf.ReaderConfig["raw_data_lines"] = "200"
	rawData, err = RawData(runnerConf.ReaderConfig)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 100, len(rawData))

	runnerConf.ReaderConfig["log_path"] = "./Test_RawDataNotExist"
	_, err = RawData(runnerConf.ReaderConfig)
	assert.Error(t, err)

	runnerConf.ReaderConfig["mode"] = "test_read"
	_, err = RawData(runnerConf.ReaderConfig)
	assert.Error(t, err)
}

func Test_RawDataWithReadData(t *testing.T) {
	t.Parallel()
	var testRawData = `{
    "name":"testReadData",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "mode":"mockreader",
		"raw_data_lines": "1"
    }
}
`
	if err := os.MkdirAll("./Test_RawDataWithReadData/confs", 0777); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./Test_RawDataWithReadData")

	err := ioutil.WriteFile("./Test_RawDataWithReadData/confs/test1.conf", []byte(testRawData), 0666)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Millisecond)
	confPathAbs, _, err := GetRealPath("./Test_RawDataWithReadData/confs/test1.conf")
	if err != nil {
		t.Error(err)
	}

	var runnerConf RunnerConfig
	err = conf.LoadEx(&runnerConf, confPathAbs)
	if err != nil {
		t.Error(err)
	}
	runnerConf.ReaderConfig[readerconf.KeyRawDataTimeout] = "30"
	rawData, err := RawData(runnerConf.ReaderConfig)
	if err != nil {
		t.Error(err)
	}

	expected := []string{"{\n  \"logkit\": \"logkit\"\n}"}
	assert.Equal(t, expected, rawData)
}

func Test_RawData_DaemonReader(t *testing.T) {
	t.Parallel()
	var testRawData = `{
    "name":"testGetRawData.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./Test_RawData_DaemonReader/logdir/*",
        "meta_path":"./Test_RawData_DaemonReader1/meta_req_csv",
        "mode":"tailx",
        "read_from":"oldest",
        "ignore_hidden":"true",
		"raw_data_lines": "1"
    }
}
`
	logfile := "./Test_RawData_DaemonReader/logdir/log1"
	logdir := "./Test_RawData_DaemonReader/logdir"
	if err := os.MkdirAll("./Test_RawData_DaemonReader/confs1", 0777); err != nil {
		t.Error(err)
	}
	defer func() {
		os.RemoveAll("./Test_RawData_DaemonReader")
		os.RemoveAll("./Test_RawData_DaemonReader1")
	}()

	if err := os.MkdirAll(logdir, 0777); err != nil {
		t.Error(err)
	}
	err := createFile(logfile, 20000000)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./Test_RawData_DaemonReader/confs1/test1.conf", []byte(testRawData), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Millisecond)
	confPathAbs, _, err := GetRealPath("./Test_RawData_DaemonReader/confs1/test1.conf")
	if err != nil {
		t.Error(err)
	}

	var runnerConf RunnerConfig
	err = conf.LoadEx(&runnerConf, confPathAbs)
	if err != nil {
		t.Error(err)
	}

	rawData, err := RawData(runnerConf.ReaderConfig)
	if err != nil {
		t.Error(err)
	}

	expected := []string{"abc\n"}
	assert.Equal(t, expected, rawData)
}

func Test_ParseData(t *testing.T) {
	t.Parallel()

	_, err := ParseData(nil)
	assert.NotNil(t, err)
	c := conf.MapConf{}

	c[parserconf.KeyParserName] = "testparser"
	c[parserconf.KeyParserType] = "csv"
	c[parserconf.KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[parserconf.KeyCSVSplitter] = " "
	c[parserconf.KeyDisableRecordErrData] = "true"
	tmstr := time.Now().Format(time.RFC3339Nano)
	line1 := `1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr
	c[KeySampleLog] = line1
	parsedData, err := ParseData(c)
	assert.NoError(t, err)

	exp := make(map[string]interface{})
	exp["a"] = int64(1)
	exp["b"] = "fufu"
	exp["c"] = 3.14
	exp["d-x"] = json.Number("1")
	exp["d-y"] = "2"
	exp["e"] = tmstr
	for k, v := range parsedData[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}
	expNum := 1
	assert.Equal(t, expNum, len(parsedData), fmt.Sprintln(parsedData))

	if parsedData[0]["a"] != int64(1) {
		t.Errorf("a should be 1  but got %v", parsedData[0]["a"])
	}
	if "fufu" != parsedData[0]["b"] {
		t.Error("b should be fufu")
	}

	jsonConf := conf.MapConf{}
	jsonConf[parserconf.KeyParserName] = "jsonparser"
	jsonConf[parserconf.KeyParserType] = "json"
	jsonConf[parserconf.KeyDisableRecordErrData] = "true"
	line := "{\t\"my key\":\"myvalue\"\t}\n"
	jsonConf[KeySampleLog] = line
	parsedJsonData, jsonErr := ParseData(jsonConf)
	assert.NoError(t, jsonErr)

	jsonExp := make(map[string]interface{})
	jsonExp["my key"] = "myvalue"
	for k, v := range parsedData[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}
	jsonExpNum := 1
	assert.Equal(t, jsonExpNum, len(parsedJsonData), fmt.Sprintln(parsedJsonData))

	jsonConf[parserconf.KeyParserType] = ""
	_, jsonErr = ParseData(jsonConf)
	assert.Error(t, jsonErr)

	jsonConf[parserconf.KeyParserType] = "parse_test"
	_, jsonErr = ParseData(jsonConf)
	assert.Error(t, jsonErr)

	jsonConf[parserconf.KeyParserType] = "json"
	jsonConf[KeySampleLog] = "{\t\"my key:\"myvalue\"\t}\n"
	_, jsonErr = ParseData(jsonConf)
	assert.Error(t, jsonErr)
}

func Test_TransformData(t *testing.T) {
	t.Parallel()
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/test_data/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`

	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	transformData, err := TransformData(rc)
	if err != nil {
		t.Error(err)
	}
	exp := []Data{{
		"ip":      "111.2.3.4",
		"Region":  "浙江",
		"City":    "宁波",
		"Country": "中国",
		"Isp":     "N/A"}}
	assert.Equal(t, exp, transformData)

	_, err = TransformData(nil)
	assert.Error(t, err)

	rc["type"] = ""
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["type"] = 1
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["type"] = "transform_test"
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["type"] = "ip"
	delete(rc, "sampleLog")
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["sampleLog"] = 1
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["sampleLog"] = ""
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["sampleLog"] = "{\"ip\": }"
	_, err = TransformData(rc)
	assert.Error(t, err)

	rc["sampleLog"] = "{\"ip\": \"{a:b}\"}"
	_, err = TransformData(rc)
	assert.Error(t, err)
}

func Test_getTransformerCreator(t *testing.T) {
	t.Parallel()
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/test_data/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`
	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	creator, err := getTransformerCreator(rc)
	if err != nil {
		t.Errorf("get transformer creator from transformer config fail, error : %v", err.Error())
	}

	if creator == nil {
		t.Errorf("expect get creator, but is nil")
	}
}

func Test_getDataFromTransformConfig(t *testing.T) {
	t.Parallel()
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/test_data/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`
	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	data, err := getDataFromTransformConfig(rc)
	if err != nil {
		t.Errorf("get data from transfomer config fail, error : %v", err.Error())
	}

	if len(data) != 1 {
		t.Errorf("expect 1 data but got %v", len(data))
	}

	for _, val := range data {
		assert.Equal(t, "111.2.3.4", val["ip"])
	}
}

func Test_getTransformer(t *testing.T) {
	t.Parallel()
	config1 := `{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/test_data/17monipdb.dat",
			"sampleLog": "{\"ip\": \"111.2.3.4\"}"
	}`
	var rc map[string]interface{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	creator, err := getTransformerCreator(rc)
	if err != nil {
		t.Errorf("get transformer creator from transformer config fail, error : %v", err.Error())
	}

	transformer, err := getTransformer(rc, creator)
	if err != nil {
		t.Errorf("get transformer from transformer config fail, error : %v", err.Error())
	}
	if transformer == nil {
		t.Error("expect get transformer but is empty")
	}
}

func Test_SendData(t *testing.T) {
	t.Parallel()
	c := conf.MapConf{
		readerconf.KeyHTTPServiceAddress: "127.0.0.1:8000",
		readerconf.KeyHTTPServicePath:    "/logkit/data",
	}
	readConf := conf.MapConf{
		readerconf.KeyMetaPath: "./meta",
		readerconf.KeyFileDone: "./meta",
		readerconf.KeyMode:     readerconf.ModeHTTP,
		KeyRunnerName:          "TestNewHttpReader",
	}
	meta, err := reader.NewMetaWithConf(readConf)
	assert.NoError(t, err)
	defer os.RemoveAll("./meta")
	reader, err := http.NewReader(meta, c)
	httpReader := reader.(*http.Reader)
	assert.NoError(t, err)
	err = httpReader.Start()
	assert.NoError(t, err)
	defer httpReader.Close()

	testInput := `[
		{
			"a": 1,
			"b": true,
			"c": "1",
			"e": 1.43,
			"d": {
				"a1": 1,
				"b1": true,
				"c1": "1",
				"d1": {}
			}
		},
		{
			"a": 1,
			"b": true,
			"c": "1",
			"d": {
				"a1": 1,
				"b1": true,
				"c1": "1",
				"d1": {}
			}
		}
	]`
	testJsonExp := [][]string{
		{
			`"a":1`,
			`"b":true`,
			`"c":"1"`,
			`"e":1.43`,
			`"d":{"`,
			`"a1":1`,
			`"b1":true`,
			`"c1":"1"`,
			`"d1":{}`,
		},
		{
			`"a":1`,
			`"b":true`,
			`"c":"1"`,
			`"d":{"`,
			`"a1":1`,
			`"b1":true`,
			`"c1":"1"`,
			`"d1":{}`,
		},
	}

	var senders []conf.MapConf
	senderConf := conf.MapConf{
		senderconf.KeySenderType:         senderconf.TypeHttp,
		senderconf.KeyHttpSenderGzip:     "true",
		senderconf.KeyHttpSenderCsvSplit: "\t",
		senderconf.KeyHttpSenderProtocol: "json",
		senderconf.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                    "testRunner",
		senderconf.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	senders = append(senders, senderConf)

	senderConfig := map[string]interface{}{
		"sampleLog": testInput,
		"senders":   senders,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _, exp := range testJsonExp {
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			for _, e := range exp {
				if !strings.Contains(got, e) {
					t.Fatalf("exp: %v contains %v, but not", got, e)
				}
			}
		}
		wg.Done()
	}()
	err = SendData(senderConfig)
	if err != nil {
		t.Error(err)
	}
	wg.Wait()

	err = SendData(nil)
	assert.Error(t, err)

	senderConfig["senders"] = ""
	err = SendData(senderConfig)
	assert.Error(t, err)

	senderConfig["senders"] = []map[string]interface{}{
		{senderconf.KeySenderType: "sender_test"},
	}
	err = SendData(senderConfig)
	assert.Error(t, err)
}

func Test_getSendersConfig(t *testing.T) {
	t.Parallel()
	testInput := `[
		{
			"a": 1,
			"b": true,
			"c": "1",
			"e": 1.43
		}
	]`

	var senders []conf.MapConf
	senderMapConf := conf.MapConf{
		senderconf.KeySenderType:         senderconf.TypeHttp,
		senderconf.KeyHttpSenderGzip:     "true",
		senderconf.KeyHttpSenderCsvSplit: "\t",
		senderconf.KeyHttpSenderProtocol: "json",
		senderconf.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                    "testRunner",
		senderconf.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	senders = append(senders, senderMapConf)

	senderConfig := map[string]interface{}{
		"sampleLog": testInput,
		"senders":   senders,
	}

	sendersConfig, err := getSendersConfig(senderConfig)
	if err != nil {
		t.Errorf("get senders config fail, error : %v", err.Error())
	}

	if len(sendersConfig) != 1 {
		t.Errorf("expect 1 sender config but got %v", len(sendersConfig))
	}

	for _, val := range sendersConfig {
		assert.Equal(t, senderconf.TypeHttp, val[senderconf.KeySenderType])
		assert.Equal(t, "true", val[senderconf.KeyHttpSenderGzip])
	}
}

func Test_getDataFromSenderConfig(t *testing.T) {
	t.Parallel()
	testInput := `[
		{
			"a": 1,
			"b": true,
			"c": "1",
			"e": 1.43
		}
	]`

	var senders []conf.MapConf
	senderConf := conf.MapConf{
		senderconf.KeySenderType:         senderconf.TypeHttp,
		senderconf.KeyHttpSenderGzip:     "true",
		senderconf.KeyHttpSenderCsvSplit: "\t",
		senderconf.KeyHttpSenderProtocol: "json",
		senderconf.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                    "testRunner",
		senderconf.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	senders = append(senders, senderConf)

	senderConfig := map[string]interface{}{
		"sampleLog": testInput,
		"senders":   senders,
	}

	data, err := getDataFromSenderConfig(senderConfig)
	if err != nil {
		t.Errorf("get data from config fail, error : %v", err.Error())
	}

	if len(data) != 1 {
		t.Errorf("expect 1 data but got %v", len(data))
	}

	for _, val := range data {
		assert.Equal(t, float64(1), val["a"])
		assert.Equal(t, true, val["b"])
		assert.Equal(t, "1", val["c"])
		assert.Equal(t, float64(1.43), val["e"])
	}
}

func Test_getSenders(t *testing.T) {
	t.Parallel()
	var sendersConfig []conf.MapConf
	senderConf := conf.MapConf{
		senderconf.KeySenderType:         senderconf.TypeHttp,
		senderconf.KeyHttpSenderGzip:     "true",
		senderconf.KeyHttpSenderCsvSplit: "\t",
		senderconf.KeyHttpSenderProtocol: "json",
		senderconf.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                    "testRunner",
		senderconf.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	sendersConfig = append(sendersConfig, senderConf)

	senders, err := getSenders(sendersConfig)
	if err != nil {
		t.Errorf("get senders from config fail, error : %v", err.Error())
	}

	if len(senders) != 1 {
		t.Errorf("expect 1 sender but got %v", len(senders))
	}
}

func Test_RawData_MultiLines(t *testing.T) {
	t.Parallel()
	fileName := filepath.Join(os.TempDir(), "Test_RawData_MultiLines")
	//create file & write file
	err := createFile(fileName, 2000)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(fileName)

	readConfig := conf.MapConf{
		"log_path":        fileName,
		"meta_path":       "",
		"reader_buf_size": "",
		"read_from":       "oldest",
		"datasource_tag":  "datasource",
		"encoding":        "UTF-8",
		"readio_limit":    "",
		"head_pattern":    "",
		"mode":            "file",
		"delete_enable":   "false",
		"raw_data_lines":  "3",
	}
	actual, err := RawData(readConfig)
	assert.Nil(t, err)
	assert.Equal(t, []string{"abc\n", "abc\n", "abc\n"}, actual)

	readConfig["raw_data_lines"] = "2"
	actual, err = RawData(readConfig)
	assert.Nil(t, err)
	assert.Equal(t, []string{"abc\n", "abc\n"}, actual)

	readConfig[readerconf.KeyRawDataTimeout] = "3"
	os.RemoveAll(fileName)
	createRawDataFile(fileName, "abc\n")
	actual, err = RawData(readConfig)
	readConfig[readerconf.KeyRawDataTimeout] = "30"
	assert.Nil(t, err)
	assert.Equal(t, []string{"abc\n"}, actual)
}

// createRawDataFile creates file in given path.
func createRawDataFile(fileName string, content string) {
	f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}
