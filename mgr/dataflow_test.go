package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/http"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

func Test_RawData(t *testing.T) {
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
	time.Sleep(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
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

	expected := "abc\n"
	assert.Equal(t, expected, rawData)
}

func Test_ParseData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[parser.KeyCSVSplitter] = " "
	c[parser.KeyDisableRecordErrData] = "true"
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
	jsonConf[parser.KeyParserName] = "jsonparser"
	jsonConf[parser.KeyParserType] = "json"
	jsonConf[parser.KeyDisableRecordErrData] = "true"
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
}

func Test_TransformData(t *testing.T) {
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
}

func Test_getTransformerCreator(t *testing.T) {
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
	c := conf.MapConf{
		reader.KeyHTTPServiceAddress: ":8000",
		reader.KeyHTTPServicePath:    "/logkit/data",
	}
	readConf := conf.MapConf{
		reader.KeyMetaPath: "./meta",
		reader.KeyFileDone: "./meta",
		reader.KeyMode:     reader.ModeHTTP,
		KeyRunnerName:      "TestNewHttpReader",
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
		sender.KeySenderType:         sender.TypeHttp,
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	senders = append(senders, senderConf)

	senderConfig := map[string]interface{}{
		"sampleLog": testInput,
		"senders":   senders,
	}

	err = SendData(senderConfig)
	if err != nil {
		t.Error(err)
	}
	for _, exp := range testJsonExp {
		got, err := httpReader.ReadLine()
		assert.NoError(t, err)
		for _, e := range exp {
			if !strings.Contains(got, e) {
				t.Fatalf("exp: %v contains %v, but not", got, e)
			}
		}
	}
}

func Test_getSendersConfig(t *testing.T) {
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
		sender.KeySenderType:         sender.TypeHttp,
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	senders = append(senders, senderConf)

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
		assert.Equal(t, sender.TypeHttp, val[sender.KeySenderType])
		assert.Equal(t, "true", val[sender.KeyHttpSenderGzip])
	}
}

func Test_getDataFromSenderConfig(t *testing.T) {
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
		sender.KeySenderType:         sender.TypeHttp,
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
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
	var sendersConfig []conf.MapConf
	senderConf := conf.MapConf{
		sender.KeySenderType:         sender.TypeHttp,
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
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
