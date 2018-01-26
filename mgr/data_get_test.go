package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

func Test_GetRawData(t *testing.T) {
	var testGetRawData = `{
    "name":"testGetRawData.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./Test_GetRawData/logdir",
        "meta_path":"./Test_GetRawData1/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    }
}
`
	logfile := "./Test_GetRawData/logdir/log1"
	logdir := "./Test_GetRawData/logdir"
	if err := os.MkdirAll("./Test_GetRawData/confs1", 0777); err != nil {
		t.Error(err)
	}
	defer func() {
		os.RemoveAll("./Test_GetRawData")
		os.RemoveAll("./Test_GetRawData1")
	}()

	if err := os.MkdirAll(logdir, 0777); err != nil {
		t.Error(err)
	}
	err := createFile(logfile, 20000000)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./Test_GetRawData/confs1/test1.conf", []byte(testGetRawData), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	confPathAbs, _, err := utils.GetRealPath("./Test_GetRawData/confs1/test1.conf")
	if err != nil {
		t.Error(err)
	}

	var runnerConf RunnerConfig
	err = conf.LoadEx(&runnerConf, confPathAbs)
	if err != nil {
		t.Error(err)
	}

	rawData, err := GetRawData(runnerConf.ReaderConfig)
	if err != nil {
		t.Error(err)
	}

	expected := "abc\n"
	assert.Equal(t, expected, rawData)
}

func Test_GetParseData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[parser.KeyCSVSplitter] = " "
	tmstr := time.Now().Format(time.RFC3339Nano)
	lines := []string{
		`1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr,       //correct
		`cc jj uu {"x":1,"y":"2"} ` + tmstr,          // error => uu 不是float
		`2 fufu 3.15 999 ` + tmstr,                   //error，999不是jsonmap
		`3 fufu 3.16 {"x":1,"y":["xx:12"]} ` + tmstr, //correct
		`   `,
		`4 fufu 3.17  ` + tmstr, //correct,jsonmap允许为空
	}
	parseData, err := GetParseData(c, lines)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Error(t, err)

	exp := make(map[string]interface{})
	exp["a"] = int64(1)
	exp["b"] = "fufu"
	exp["c"] = 3.14
	exp["d-x"] = json.Number("1")
	exp["d-y"] = "2"
	exp["e"] = tmstr
	for k, v := range parseData[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}

	expNum := 3
	assert.Equal(t, expNum, len(parseData), fmt.Sprintln(parseData))

	if parseData[0]["a"] != int64(1) {
		t.Errorf("a should be 1  but got %v", parseData[0]["a"])
	}
	if "fufu" != parseData[0]["b"] {
		t.Error("b should be fufu")
	}
}
