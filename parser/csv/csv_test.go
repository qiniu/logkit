package csv

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	bench    []Data
	testData = utils.GetParseTestData(`123 fufu 3.16 {\"x\":1,\"y\":[\"xx:12\"]}`, DefaultMaxBatchSize)
)

// old: 5	 258934606 ns/op routine = 1  (2MB)
// now: 3	 356798749 ns/op routine = 1  (2MB)
// now: 5	 225912351 ns/op routine = 2  (2MB)
func Benchmark_ParseLine(b *testing.B) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float, d jsonmap"
	c[parser.KeyCSVSplitter] = " "
	p, _ := NewParser(c)

	var m []Data
	for n := 0; n < b.N; n++ {
		m, _ = p.Parse(testData)
	}
	bench = m
}

func Test_Parser(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[parser.KeyCSVSplitter] = " "
	c[parser.KeyDisableRecordErrData] = "true"
	p, err := NewParser(c)
	if err != nil {
		t.Error(err)
	}
	tmstr := time.Now().Format(time.RFC3339Nano)
	lines := []string{
		`1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr,       //correct
		`cc jj uu {"x":1,"y":"2"} ` + tmstr,          // error => uu 不是float
		`2 fufu 3.15 999 ` + tmstr,                   //error，999不是jsonmap
		`3 fufu 3.16 {"x":1,"y":["xx:12"]} ` + tmstr, //correct
		`   `,
		`4 fufu 3.17  ` + tmstr, //correct,jsonmap允许为空
	}
	datas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
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
	for k, v := range datas[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}

	expNum := 3
	assert.Equal(t, expNum, len(datas), fmt.Sprintln(datas))

	if datas[0]["a"] != int64(1) {
		t.Errorf("a should be 1 but got %v", datas[0]["a"])
	}
	if "fufu" != datas[0]["b"] {
		t.Error("b should be fufu")
	}
	assert.EqualValues(t, p.Name(), "testparser")
}

func Test_CsvParserForErrData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[parser.KeyCSVSplitter] = " "
	c[parser.KeyDisableRecordErrData] = "false"
	p, err := NewParser(c)
	if err != nil {
		t.Error(err)
	}
	tmstr := time.Now().Format(time.RFC3339Nano)
	lines := []string{
		`1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr,       //correct
		`cc jj uu {"x":1,"y":"2"} ` + tmstr,          // error => uu 不是float
		`2 fufu 3.15 999 ` + tmstr,                   //error，999不是jsonmap
		`3 fufu 3.16 {"x":1,"y":["xx:12"]} ` + tmstr, //correct
		`   `,
		`4 fufu 3.17  ` + tmstr, //correct,jsonmap允许为空
	}
	datas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
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
	for k, v := range datas[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}

	assert.Equal(t, 6, len(datas), "parse lines error")

	expErrData := `2 fufu 3.15 999 ` + tmstr
	assert.Equal(t, expErrData, datas[2]["pandora_stash"])
}

func Test_Jsonmap(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testjsonmap"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, d jsonmap,e jsonmap{x string,y long},f jsonmap{z float, ...}"
	c[parser.KeyCSVSplitter] = " "
	p, err := NewParser(c)
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{
		"123 {\"x\":1,\"y\":\"2\"} {\"x\":1,\"y\":\"2\",\"z\":\"3\"} {\"x\":1.0,\"y\":\"2\",\"z\":\"3.0\"}",
	}
	datas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	d := datas[0]
	if d["f-x"] != json.Number("1.0") {
		t.Errorf("f-x should be json.Number 1.0 but %v %v", reflect.TypeOf(d["f-x"]), d["f-x"])
	}
	if d["f-z"] != 3.0 {
		t.Errorf("f-z should be float 3.0 but type %v %v", reflect.TypeOf(d["f-z"]), d["f-z"])
	}
	if _, ok := d["e-z"]; ok {
		t.Errorf("e-z should not exist but %v", d["e-z"])
	}
	if d["e-x"] != "1" {
		t.Errorf("e-x should be string 1 but %v %v", reflect.TypeOf(d["e-x"]), d["e-x"])
	}
}

func Test_CsvParserLabel(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, b string, c float"
	c[parser.KeyLabels] = "d nb1684"
	c[parser.KeyCSVSplitter] = " "
	p, err := NewParser(c)
	if err != nil {
		t.Error(err)
	}
	lines := []string{
		"123 fufu 3.14",
		"cc jj uu",
		"123 fufu 3.14 999",
	}
	datas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Error(t, err)
	if len(datas) != 3 {
		t.Errorf("correct line should be one, but got %v", len(datas))
	}
	if datas[0]["a"] != int64(123) {
		t.Errorf("a should be 123  but got %v", datas[0]["a"])
	}
	if "fufu" != datas[0]["b"] {
		t.Error("b should be fufu")
	}
	if "nb1684" != datas[0]["d"] {
		t.Error("d should be nb1684")
	}
	expErrData := "cc jj uu"
	assert.Equal(t, expErrData, datas[1]["pandora_stash"])
}

func Test_CsvParserDupColumn1(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testparser"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "a long, a string, c float"
	c[parser.KeyCSVSplitter] = " "
	_, err := NewParser(c)
	if err == nil {
		t.Error("there must be an error about duplicate key a")
	}
}

func Test_ParseField(t *testing.T) {
	schema := "logtype string,timestamp long, method jsonmap{a string,b float}, path string , reqheader string"
	fields, err := parseSchemaFieldList(schema)
	if err != nil {
		t.Error(err)
	}
	got := strings.Join(fields, "|")
	exp := "logtype string|timestamp long|method jsonmap{a string,b float}|path string|reqheader string"
	if got != exp {
		t.Error("parseFieldList error")
	}
	_, err = parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}

	schema = "a long, d jsonmap,e jsonmap{x string,y long},f jsonmap{z float,...}"
	fields, err = parseSchemaFieldList(schema)
	if err != nil {
		t.Error(err)
	}
	got = strings.Join(fields, "|")
	exp = "a long|d jsonmap|e jsonmap{x string,y long}|f jsonmap{z float,...}"
	if got != exp {
		t.Error("parseFieldList error")
	}
	_, err = parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}
}

func Test_convertValue(t *testing.T) {
	jsonraw := "{\"a\":null}"
	m := make(map[string]interface{})
	if err := jsoniter.Unmarshal([]byte(jsonraw), &m); err != nil {
		t.Error(err)
	}
	for _, v := range m {
		//不panic就是胜利
		checkValue(v)
		convertValue(v, "jsonmap")
	}
}

func TestField_MakeValue(t *testing.T) {
	tm, err := makeValue("2017/01/02 15:00:00", parser.TypeDate, 1)
	if err != nil {
		t.Error(err)
	}
	exp, err := times.StrToTime("2017/01/02 16:00:00")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, exp.Format(time.RFC3339Nano), tm)
}

func TestRename(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testRename"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "logType string, service string, timestamp string, method string, path string, reqHeader jsonmap, nullStr string, code long, resBody jsonmap, info string, t1 long, t2 long"
	c[parser.KeyCSVSplitter] = "	"
	p, err := NewParser(c)
	assert.NoError(t, err)
	lines := []string{
		`REQ	REPORT	15112467445566096	POST	/v1/activate	{"Accept-Encoding":"gzip","Content-Length":"0","Host":"10.200.20.68:2308","IP":"10.200.20.41","User-Agent":"Go-http-client/1.1"}		200    	{"Content-Length":"55","Content-Type":"application/json","X-Log":["REPORT:1"],"X-Reqid":"pyAAAO0mQ0HoBvkU"}	{"user":"13805xxxx4","password":"abcjofewfj"}	55	14946`,
	}
	gotDatas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	expDatas := []Data{
		{
			"logType":                   "REQ",
			"service":                   "REPORT",
			"timestamp":                 "15112467445566096",
			"method":                    "POST",
			"path":                      "/v1/activate",
			"reqHeader-User-Agent":      "Go-http-client/1.1",
			"reqHeader-Accept-Encoding": "gzip",
			"reqHeader-Host":            "10.200.20.68:2308",
			"reqHeader-IP":              "10.200.20.41",
			"reqHeader-Content-Length":  "0",
			"nullStr":                   "",
			"code":                      int64(200),
			"resBody-Content-Length": "55",
			"resBody-Content-Type":   "application/json",
			"resBody-X-Reqid":        "pyAAAO0mQ0HoBvkU",
			"resBody-X-Log": []interface{}{
				"REPORT:1",
			},
			"info": `{"user":"13805xxxx4","password":"abcjofewfj"}`,
			"t1":   int64(55),
			"t2":   int64(14946),
		},
	}
	assert.Equal(t, len(gotDatas), len(expDatas))
	for i := range expDatas {
		assert.Equal(t, len(expDatas[i]), len(gotDatas[i]))
		for key, exp := range expDatas[i] {
			got, exist := gotDatas[i][key]
			assert.Equal(t, true, exist, key)
			assert.Equal(t, exp, got)
		}
	}

	c[parser.KeyCSVAutoRename] = "true"
	p, err = NewParser(c)
	assert.NoError(t, err)
	gotDatas, err = p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	expDatas = []Data{
		{
			"logType":                   "REQ",
			"service":                   "REPORT",
			"timestamp":                 "15112467445566096",
			"method":                    "POST",
			"path":                      "/v1/activate",
			"reqHeader_User_Agent":      "Go-http-client/1.1",
			"reqHeader_Accept_Encoding": "gzip",
			"reqHeader_Host":            "10.200.20.68:2308",
			"reqHeader_IP":              "10.200.20.41",
			"reqHeader_Content_Length":  "0",
			"nullStr":                   "",
			"code":                      int64(200),
			"resBody_Content_Length": "55",
			"resBody_Content_Type":   "application/json",
			"resBody_X_Reqid":        "pyAAAO0mQ0HoBvkU",
			"resBody_X_Log": []interface{}{
				"REPORT:1",
			},
			"info": `{"user":"13805xxxx4","password":"abcjofewfj"}`,
			"t1":   int64(55),
			"t2":   int64(14946),
		},
	}
	assert.Equal(t, len(gotDatas), len(expDatas))
	for i := range expDatas {
		assert.Equal(t, len(expDatas[i]), len(gotDatas[i]))
		for key, exp := range expDatas[i] {
			got, exist := gotDatas[i][key]
			assert.Equal(t, true, exist, key)
			assert.Equal(t, exp, got)
		}
	}
}

func TestJSONMap(t *testing.T) {
	fd := field{
		name:     "c",
		dataType: parser.TypeJSONMap,
	}
	testx := "999"
	data, err := fd.ValueParse(testx, 0)
	assert.Error(t, err)
	assert.Equal(t, data, Data{})
}

func TestGetUnmachedMessage(t *testing.T) {
	got := getUnmachedMessage([]string{"a", "b"}, []field{{name: "a"}})
	assert.Equal(t, `matched: [a]=>[a],  unmatched log: [b]`, got)
	got = getUnmachedMessage([]string{"a"}, []field{{name: "a"}, {name: "b"}})
	assert.Equal(t, `matched: [a]=>[a],  unmatched schema: [b]`, got)
}

func TestAllMoreName(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestAllMoreName"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "logType string"
	c[parser.KeyCSVSplitter] = "|"
	c[parser.KeyCSVAllowMore] = "ha"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|b|c|d"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"ha0": "b", "logType": "a", "ha1": "c", "ha2": "d"}}, datas)

	datas, err = pp.Parse([]string{"a"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a"}}, datas)
}

func TestAllowLess(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestAllowLess"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "logType string,a long,b float,c string"
	c[parser.KeyCSVSplitter] = "|"
	c[parser.KeyCSVAllowMore] = "ha"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|1|1.2|d"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"a": int64(1), "logType": "a", "b": 1.2, "c": "d"}}, datas)

	datas, err = pp.Parse([]string{"a|1|1.2|d|xx|yy"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"a": int64(1), "logType": "a", "b": 1.2, "c": "d", "ha0": "xx", "ha1": "yy"}}, datas)

	datas, err = pp.Parse([]string{"a|1"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"a": int64(1), "logType": "a"}}, datas)

	datas, err = pp.Parse([]string{"a|1.2|1.2|d"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Error(t, err)
}

func TestIgnoreField(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestIgnoreField"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "logType string,a long,b float,c string"
	c[parser.KeyCSVSplitter] = "|"
	c[parser.KeyCSVIgnoreInvalidField] = "true"
	c[parser.KeyCSVAllowNoMatch] = "false"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|1.2|1.2|d"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "b": 1.2, "c": "d"}}, datas)

	datas, err = pp.Parse([]string{"a|1.2|1.2|d|xx"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Error(t, err)
}

func TestAllowNotMatch(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestAllowNotMatch"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "logType string,a long,b float,c string"
	c[parser.KeyCSVSplitter] = "|"
	c[parser.KeyCSVAllowNoMatch] = "true"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|1|1.2|d|e"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "a": int64(1), "b": 1.2, "c": "d"}}, datas)

	datas, err = pp.Parse([]string{"a|1|1.2"})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "a": int64(1), "b": 1.2}}, datas)
}

func TestCsvlastempty(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestCsvlastempty"
	c[parser.KeyParserType] = "csv"
	c[parser.KeyCSVSchema] = "logType string,a long,b float,c string"
	c[parser.KeyCSVSplitter] = "\t"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a\t1\t1.2\t "})
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "a": int64(1), "b": 1.2, "c": " "}}, datas)
}
