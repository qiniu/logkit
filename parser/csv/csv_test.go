package csv

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/logkit/parser"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
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
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap"
	c[KeyCSVSplitter] = " "
	p, _ := NewParser(c)

	var m []Data
	for n := 0; n < b.N; n++ {
		m, _ = p.Parse(testData)
	}
	bench = m
}

func Test_Parser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[KeyCSVSplitter] = " "
	c[KeyDisableRecordErrData] = "true"
	p, err := NewParser(c)
	if err != nil {
		t.Error(err)
	}

	pType, ok := p.(parser.ParserType)
	assert.True(t, ok)
	assert.EqualValues(t, TypeCSV, pType.Type())

	datas, err := p.Parse(nil)
	assert.Nil(t, err)
	assert.EqualValues(t, datas, []Data{})

	datas, err = p.Parse([]string{"", "", ""})
	assert.NotNil(t, err)
	assert.EqualValues(t, datas, []Data{})

	tmstr := time.Now().Format(time.RFC3339Nano)
	lines := []string{
		`1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr,       //correct
		`cc jj uu {"x":1,"y":"2"} ` + tmstr,          // error => uu 不是float
		`2 fufu 3.15 999 ` + tmstr,                   //error，999不是jsonmap
		`3 fufu 3.16 {"x":1,"y":["xx:12"]} ` + tmstr, //correct
		`   `,                   // 空行被省略
		`4 fufu 3.17  ` + tmstr, //correct,jsonmap允许为空
	}
	datas, err = p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
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

	delete(c, KeyCSVSchema)
	_, err = NewParser(c)
	assert.Nil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap{x string,y long}},e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap{{x string,y long},e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap{x string,y long}{,e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b, c float, d jsonmap,e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap x string,y long,e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap{x string,y},y long,e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	c[KeyCSVSchema] = "a long, b string, c float, d test,e date"
	_, err = NewParser(c)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	// 增加测试 没有schema的情况
	c[KeyCSVSchema] = ""
	c[KeyCSVLazyQuotes] = "true"

	p, err = NewParser(c)
	if err != nil {
		t.Error(err)
	}
	// 1.正常情况
	lines = []string{
		`id name pos array time `,
		`1 fufu 3.14 {"x":1,"y":"2"} ` + tmstr,
	}
	expectData := []Data{
		{
			"id":    "1",
			"name":  "fufu",
			"pos":   "3.14",
			"array": "{\"x\":1,\"y\":\"2\"}",
			"time":  tmstr,
		},
	}
	datas, err = p.Parse(lines)
	assert.Nil(t, err)
	assert.EqualValues(t, expectData, datas)
	delete(c, KeyCSVLazyQuotes)
	// 2. 没有数据
	p, err = NewParser(c)
	lines = []string{""}
	datas, err = p.Parse(lines)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	// 3.有标签，重复
	c[KeyCSVSchema] = ""
	c[KeyCSVLabels] = "name string, time date"
	p, err = NewParser(c)
	lines = []string{
		"name age",
		"li 21",
	}
	expectData = []Data{
		{
			"name": "li",
			"age":  "21",
			"time": "date",
		},
	}
	datas, err = p.Parse(lines)
	assert.Nil(t, err)
	assert.EqualValues(t, expectData, datas)

	// 4.有标签，不重复
	c[KeyCSVLabels] = "pos string, time date"
	c[KeyCSVSchema] = ""
	p, err = NewParser(c)
	lines = []string{
		"name age",
		"li 21",
		"sun 22",
		"zhang 23",
		"wang 24",
		"zhou 25",
	}
	expectData = []Data{
		{
			"name": "li",
			"age":  "21",
			"pos":  "string",
			"time": "date",
		},
		{
			"name": "sun",
			"age":  "22",
			"pos":  "string",
			"time": "date",
		},
		{
			"name": "zhang",
			"age":  "23",
			"pos":  "string",
			"time": "date",
		},
		{
			"name": "wang",
			"age":  "24",
			"pos":  "string",
			"time": "date",
		},
		{
			"name": "zhou",
			"age":  "25",
			"pos":  "string",
			"time": "date",
		},
	}
	datas, err = p.Parse(lines)
	assert.Nil(t, err)
	assert.EqualValues(t, expectData, datas)

	// 5.允许有多余列
	c[KeyCSVAllowNoMatch] = "true"
	c[KeyCSVLabels] = ""
	c[KeyCSVSchema] = ""
	p, err = NewParser(c)
	lines = []string{
		"name age pos",
		"li 21",
	}
	expectData = []Data{
		{
			"name": "li",
			"age":  "21",
		},
	}
	datas, err = p.Parse(lines)
	assert.Nil(t, err)
	assert.EqualValues(t, expectData, datas)

	// 6.不允许有多余列
	c[KeyCSVAllowNoMatch] = "false"
	c[KeyCSVSchema] = ""
	p, err = NewParser(c)
	lines = []string{
		"name age pos",
		"li 21",
	}
	expectData = []Data{}
	datas, err = p.Parse(lines)
	assert.NotNil(t, err)
	assert.EqualValues(t, expectData, datas)
	t.Log("err: ", err)

	// 7.缺少列
	c[KeyCSVSchema] = ""
	p, err = NewParser(c)
	lines = []string{
		"name age",
		"li 21 3.14",
	}
	expectData = []Data{}
	datas, err = p.Parse(lines)
	assert.NotNil(t, err)
	assert.EqualValues(t, expectData, datas)
	t.Log("err: ", err)

	// 8.包含隔离键 但首部不存在
	c[KeyCSVSchema] = ""
	c[KeyCSVContainSplitterKey] = "split"
	p, err = NewParser(c)
	lines = []string{
		"name age",
		"li 21",
	}
	expectData = []Data{}
	datas, err = p.Parse(lines)
	assert.NotNil(t, err)
	assert.EqualValues(t, expectData, datas)
	t.Log("err: ", err)

	// 9.包含隔离键 且首部存在
	c[KeyCSVSchema] = ""
	p, err = NewParser(c)
	lines = []string{
		"name age split pos",
		"li 21 test 3.14",
	}
	expectData = []Data{
		{
			"name":  "li",
			"age":   "21",
			"pos":   "3.14",
			"split": "test",
		},
	}
	datas, err = p.Parse(lines)
	assert.Nil(t, err)
	assert.EqualValues(t, expectData, datas)

	// 10.有双引号存在
	c[KeyCSVSchema] = ""
	p, err = NewParser(c)
	lines = []string{
		"name age split pos",
		`li "21 21" test 3.14`,
	}
	expectData = []Data{
		{
			"name":  "li",
			"age":   "21 21",
			"pos":   "3.14",
			"split": "test",
		},
	}
	datas, err = p.Parse(lines)
	assert.Nil(t, err)
	assert.EqualValues(t, expectData, datas)

}

func Test_CsvParserForErrData(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[KeyCSVSplitter] = " "
	c[KeyDisableRecordErrData] = "false"
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
		err = errors.New(c.LastError)
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

func Test_CsvParserKeepRawData(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[KeyCSVSplitter] = " "
	c[KeyDisableRecordErrData] = "false"
	c[KeyKeepRawData] = "true"
	p, err := NewParser(c)
	if err != nil {
		t.Error(err)
	}
	tmstr := time.Now().Format(time.RFC3339Nano)
	lines := []string{
		`1 fufu 3.14 {"x":"1","y":"2"} ` + tmstr,       //correct
		`3 fufu 3.16 {"x":"1","y":["xx:12"]} ` + tmstr, //correct
		`4 fufu 3.17  ` + tmstr,                        //correct,jsonmap允许为空
	}
	datas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{
		{
			"e":        tmstr,
			"raw_data": `1 fufu 3.14 {"x":"1","y":"2"} ` + tmstr,
			"a":        int64(1),
			"b":        "fufu",
			"c":        float64(3.14),
			"d-x":      "1",
			"d-y":      "2",
		},
		{
			"e":        tmstr,
			"raw_data": `3 fufu 3.16 {"x":"1","y":["xx:12"]} ` + tmstr,
			"a":        int64(3),
			"b":        "fufu",
			"c":        float64(3.16),
			"d-x":      "1",
			"d-y":      []interface{}{"xx:12"},
		},
		{
			"e":        tmstr,
			"raw_data": `4 fufu 3.17  ` + tmstr,
			"a":        int64(4),
			"b":        "fufu",
			"c":        float64(3.17),
		},
	}, datas)
}

func Test_Jsonmap(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testjsonmap"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, d jsonmap,e jsonmap{x string,y long},f jsonmap{z float, ...}"
	c[KeyCSVSplitter] = " "
	p, err := NewParser(c)
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{
		"123 {\"x\":1,\"y\":\"2\"} {\"x\":1,\"y\":\"2\",\"z\":\"3\"} {\"x\":1.0,\"y\":\"2\",\"z\":\"3.0\"}",
	}
	datas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	if err != nil {
		t.Error(err)
	}
	d := datas[0]
	if d["f-x"] != json.Number("1.0") {
		t.Errorf("f-x should be json.Number 1.0 but %T %v", d["f-x"], d["f-x"])
	}
	if d["f-z"] != 3.0 {
		t.Errorf("f-z should be float 3.0 but type %T %v", d["f-z"], d["f-z"])
	}
	if _, ok := d["e-z"]; ok {
		t.Errorf("e-z should not exist but %v", d["e-z"])
	}
	if d["e-x"] != "1" {
		t.Errorf("e-x should be string 1 but %T %v", d["e-x"], d["e-x"])
	}
}

func Test_CsvParserLabel(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float"
	c[KeyLabels] = "d nb1684"
	c[KeyCSVSplitter] = " "
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
		err = errors.New(c.LastError)
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
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, a string, c float"
	c[KeyCSVSplitter] = " "
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
	schemaFields, err := parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}
	expectFiled := field{
		typeChange: map[string]DataType{
			"a": "string",
			"b": "float",
		},
	}
	for _, schema := range schemaFields {
		if schema.name == "method" {
			assert.EqualValues(t, expectFiled.typeChange, schema.typeChange)
		}
	}

	schema = "logtype string,timestamp long, method|method2 jsonmap{a | c string,b|d float}, path | reqheader string"
	fields, err = parseSchemaFieldList(schema)
	if err != nil {
		t.Error(err)
	}
	got = strings.Join(fields, "|")
	exp = "logtype string|timestamp long|method jsonmap{a | c string,b|d float}|method2 jsonmap{a | c string,b|d float}|path string|reqheader string"
	if got != exp {
		t.Error("parseFieldList error")
	}
	schemaFields, err = parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}
	expect := map[string]DataType{
		"a": "string",
		"c": "string",
		"b": "float",
		"d": "float",
	}
	for _, schema := range schemaFields {
		if schema.name == "method" || schema.name == "method2" {
			assert.EqualValues(t, expect, schema.typeChange)
		}
	}

	schema = "a long, d jsonmap,e jsonmap{x string,y long},f jsonmap{z|l float,...}"
	fields, err = parseSchemaFieldList(schema)
	if err != nil {
		t.Error(err)
	}
	got = strings.Join(fields, "|")
	exp = "a long|d jsonmap|e jsonmap{x string,y long}|f jsonmap{z|l float,...}"
	if got != exp {
		t.Error("parseFieldList error")
	}
	schemaFields, err = parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}
	expectFiled = field{
		typeChange: map[string]DataType{
			"z": "float",
			"l": "float",
		},
		allin: true,
	}
	for _, schema := range schemaFields {
		if schema.name == "f" {
			assert.EqualValues(t, expectFiled.typeChange, schema.typeChange)
			assert.EqualValues(t, expectFiled.allin, schema.allin)
		}
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
	tm, err := makeValue("2017/01/02 15:00:00", TypeDate, 1)
	if err != nil {
		t.Error(err)
	}
	exp, err := times.StrToTime("2017/01/02 16:00:00")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, exp.Format(time.RFC3339Nano), tm)

	_, err = makeValue("2017/01/02 15:00:00", "test", 1)
	assert.NotNil(t, err)
	t.Log("err: ", err)

	f, err := makeValue("", TypeFloat, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, f)

	l, err := makeValue("", TypeLong, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, l)

	_, err = makeValue("", TypeDate, 0)
	assert.Nil(t, err)

	_, err = makeValue("2017aaa", TypeDate, 0)
	assert.NotNil(t, err)
	t.Log("err: ", err)
}

func TestRename(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testRename"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "logType string, service string, timestamp string, method string, path string, reqHeader jsonmap, nullStr string, code long, resBody jsonmap, info string, t1 long, t2 long"
	c[KeyCSVSplitter] = "	"
	p, err := NewParser(c)
	assert.NoError(t, err)
	lines := []string{
		`REQ	REPORT	15112467445566096	POST	/v1/activate	{"Accept-Encoding":"gzip","Content-Length":"0","Host":"10.200.20.68:2308","IP":"10.200.20.41","User-Agent":"Go-http-client/1.1"}		200    	{"Content-Length":"55","Content-Type":"application/json","X-Log":["REPORT:1"],"X-Reqid":"pyAAAO0mQ0HoBvkU"}	{"user":"13805xxxx4","password":"abcjofewfj"}	55	14946`,
	}
	gotDatas, err := p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
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
			"resBody-Content-Length":    "55",
			"resBody-Content-Type":      "application/json",
			"resBody-X-Reqid":           "pyAAAO0mQ0HoBvkU",
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

	c[KeyCSVAutoRename] = "true"
	p, err = NewParser(c)
	assert.NoError(t, err)
	gotDatas, err = p.Parse(lines)
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
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
			"resBody_Content_Length":    "55",
			"resBody_Content_Type":      "application/json",
			"resBody_X_Reqid":           "pyAAAO0mQ0HoBvkU",
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

func TestValueParse(t *testing.T) {
	t.Parallel()
	fd := field{
		name:     "c",
		dataType: TypeJSONMap,
	}
	testx := "999"
	data, err := fd.ValueParse(testx, 0)
	assert.NotNil(t, err)
	t.Log("err: ", err)
	assert.Equal(t, data, Data{})

	fd.typeChange = map[string]DataType{
		"a": TypeLong,
	}
	testMap := map[string]interface{}{
		"a": "c",
	}
	testBytes, err := jsoniter.Marshal(testMap)
	assert.Nil(t, err)
	data, err = fd.ValueParse(string(testBytes), 0)
	assert.NotNil(t, err)
	t.Log("err: ", err)
	assert.Equal(t, data, Data{})
}

func TestGetUnmachedMessage(t *testing.T) {
	t.Parallel()
	got := getUnmachedMessage([]string{"a", "b"}, []field{{name: "a"}})
	assert.Equal(t, `matched: [a]=>[a],  unmatched log: [b]`, got)
	got = getUnmachedMessage([]string{"a"}, []field{{name: "a"}, {name: "b"}})
	assert.Equal(t, `matched: [a]=>[a],  unmatched schema: [b]`, got)
}

func TestAllMoreName(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "TestAllMoreName"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "logType string"
	c[KeyCSVSplitter] = "|"
	c[KeyCSVAllowMore] = "ha"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|b|c|d"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"ha0": "b", "logType": "a", "ha1": "c", "ha2": "d"}}, datas)

	datas, err = pp.Parse([]string{"a"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a"}}, datas)
}

func TestAllowLess(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "TestAllowLess"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "logType string,a long,b float,c string"
	c[KeyCSVSplitter] = "|"
	c[KeyCSVAllowMore] = "ha"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|1|1.2|d"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"a": int64(1), "logType": "a", "b": 1.2, "c": "d"}}, datas)

	datas, err = pp.Parse([]string{"a|1|1.2|d|xx|yy"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"a": int64(1), "logType": "a", "b": 1.2, "c": "d", "ha0": "xx", "ha1": "yy"}}, datas)

	datas, err = pp.Parse([]string{"a|1"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"a": int64(1), "logType": "a"}}, datas)

	datas, err = pp.Parse([]string{"a|1.2|1.2|d"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.Error(t, err)
	assert.EqualValues(t, []Data{{"pandora_stash": "a|1.2|1.2|d"}}, datas)
}

func TestIgnoreField(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "TestIgnoreField"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "logType string,a long,b float,c string"
	c[KeyCSVSplitter] = "|"
	c[KeyCSVIgnoreInvalidField] = "true"
	c[KeyCSVAllowNoMatch] = "false"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|1.2|1.2|d"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "b": 1.2, "c": "d"}}, datas)

	datas, err = pp.Parse([]string{"a|1.2|1.2|d|xx"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.Error(t, err)
	assert.EqualValues(t, []Data{{"pandora_stash": "a|1.2|1.2|d|xx"}}, datas)
}

func TestAllowNotMatch(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "TestAllowNotMatch"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "logType string,a long,b float,c string"
	c[KeyCSVSplitter] = "|"
	c[KeyCSVAllowNoMatch] = "true"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a|1|1.2|d|e"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "a": int64(1), "b": 1.2, "c": "d"}}, datas)

	datas, err = pp.Parse([]string{"a|1|1.2"})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "a": int64(1), "b": 1.2}}, datas)
}

func TestCsvlastempty(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "TestCsvlastempty"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "logType string,a long,b float,c string"
	c[KeyCSVSplitter] = "\t"
	pp, err := NewParser(c)
	assert.NoError(t, err)
	datas, err := pp.Parse([]string{"a\t1\t1.2\t "})
	if c, ok := err.(*StatsError); ok {
		err = errors.New(c.LastError)
	}
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"logType": "a", "a": int64(1), "b": 1.2, "c": ""}}, datas)
}

func Test_spitFields(t *testing.T) {
	actual := splitFields("a string")
	assert.EqualValues(t, []string{"a string"}, actual)

	actual = splitFields("a|b string")
	assert.EqualValues(t, []string{"a string", "b string"}, actual)

	actual = splitFields("a | b string")
	assert.EqualValues(t, []string{"a string", "b string"}, actual)

	actual = splitFields("a_b string")
	assert.EqualValues(t, []string{"a_b string"}, actual)

	actual = splitFields("a_b")
	assert.EqualValues(t, []string{"a_b"}, actual)

	actual = splitFields("method|method2 jsonmap{a | c string,b|d float}")
	assert.EqualValues(t, []string{"method jsonmap{a | c string,b|d float}", "method2 jsonmap{a | c string,b|d float}"}, actual)

	actual = splitFields("a|b")
	assert.EqualValues(t, []string{"a|b"}, actual)
}

func Test_Rename(t *testing.T) {
	datas := []Data{
		{"a": "c", "b": "d"},
		{"a1": "c1"},
	}
	newDatas := Rename(datas)
	assert.EqualValues(t, datas, newDatas)

	newDatas[0] = nil
	assert.NotEqual(t, datas[0], newDatas[0])
}

func Test_ContainSplitterParse(t *testing.T) {
	parserName := "testContainSplitter"
	parserType := "csv"
	schema := "a jsonmap, b float, c long, d string"
	splitter := ","
	autoRename := "true"

	testCases := []struct {
		parserConf conf.MapConf
		line       []string
		wanted     []Data
	}{
		{
			conf.MapConf{
				KeyParserName:            parserName,
				KeyParserType:            parserType,
				KeyCSVSchema:             schema,
				KeyCSVSplitter:           splitter,
				KeyCSVAutoRename:         autoRename,
				KeyCSVContainSplitterKey: "a",
			},
			[]string{"{\"foo\":\"aaa\", \"bar\":\"bbb\"},1.23,123,foo"},
			[]Data{{"a_foo": "aaa", "a_bar": "bbb", "b": 1.23, "c": int64(123), "d": "foo"}},
		},
		{
			conf.MapConf{
				KeyParserName:            parserName,
				KeyParserType:            parserType,
				KeyCSVSchema:             schema,
				KeyCSVSplitter:           splitter,
				KeyCSVAutoRename:         autoRename,
				KeyCSVContainSplitterKey: "d",
			},
			[]string{"{\"foo\":\"aaa\"},1.23,123,this,is,one"},
			[]Data{{"a_foo": "aaa", "b": 1.23, "c": int64(123), "d": "this,is,one"}},
		},
		{
			conf.MapConf{
				KeyParserName:            parserName,
				KeyParserType:            parserType,
				KeyCSVSchema:             schema,
				KeyCSVSplitter:           splitter,
				KeyCSVAutoRename:         autoRename,
				KeyCSVContainSplitterKey: "",
			},
			[]string{"{\"foo\":\"aaa\"},1.23,123,this"},
			[]Data{{"a_foo": "aaa", "b": 1.23, "c": int64(123), "d": "this"}},
		},
		{
			conf.MapConf{
				KeyParserName:            parserName,
				KeyParserType:            parserType,
				KeyCSVSchema:             schema,
				KeyCSVSplitter:           splitter,
				KeyCSVAutoRename:         autoRename,
				KeyCSVContainSplitterKey: "d",
			},
			[]string{"{\"foo\":\"aaa\"},1.23"},
			[]Data{{"a_foo": "aaa", "b": 1.23}},
		},
	}

	for _, tc := range testCases {
		parser, err := NewParser(tc.parserConf)
		assert.NoError(t, err)
		res, err := parser.Parse(tc.line)
		assert.NoError(t, err)
		assert.Equal(t, tc.wanted, res, "")
	}
}

func Test_setHeaderWithSchema(t *testing.T) {
	type args struct {
		schema string
	}
	tests := []struct {
		name       string
		args       args
		wantFields []field
		wantErr    bool
	}{
		{
			name: "normal_test",
			args: args{
				schema: "name string,age string",
			},
			wantFields: []field{
				{
					name:     "name",
					dataType: "string",
				},
				{
					name:     "age",
					dataType: "string",
				},
			},
			wantErr: false,
		},
		{
			name: "symbol_match",
			args: args{
				schema: "name string,age long",
			},
			wantFields: []field{
				{
					name:     "name",
					dataType: "string",
				},
				{
					name:     "age",
					dataType: "long",
				},
			},
			wantErr: false,
		},
		{
			name: "symbol_not_match",
			args: args{
				schema: "{name string,age string{",
			},
			wantFields: nil,
			wantErr:    true,
		},
		{
			name: "schema_error",
			args: args{
				schema: "name string age string",
			},
			wantFields: []field{
				{
					name:     "name",
					dataType: "string",
				},
			},
			wantErr: false,
		},
		{
			name: "schema_error_2",
			args: args{
				schema: "name, age string",
			},
			wantFields: nil,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFields, err := setHeaderWithSchema(tt.args.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("setHeaderWithSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotFields, tt.wantFields) {
				t.Errorf("setHeaderWithSchema() gotFields = %v, want %v", gotFields, tt.wantFields)
			}
		})
	}
}

func Test_setHeaderWithoutSchema(t *testing.T) {
	type args struct {
		line  string
		delim string
		c     conf.MapConf
	}
	tests := []struct {
		name    string
		args    args
		want    []field
		wantErr bool
	}{
		{
			name: "normal_test",
			args: args{
				line:  "name age major",
				delim: " ",
				c:     make(conf.MapConf),
			},
			want: []field{
				{
					name:     "name",
					dataType: "string",
				},
				{
					name:     "age",
					dataType: "string",
				},
				{
					name:     "major",
					dataType: "string",
				},
			},
			wantErr: false,
		},
		{
			name: "field_len_zero",
			args: args{
				line:  "",
				delim: ",",
				c:     make(conf.MapConf),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := setHeaderWithoutSchema(tt.args.line, tt.args.delim, tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("setHeaderWithoutSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setHeaderWithoutSchema() got = %v, want %v", got, tt.want)
			}
		})
	}

	line := "name age"
	delim := " "
	c := make(conf.MapConf)
	got, err := setHeaderWithoutSchema(line, delim, c)
	expect := []field{
		{
			name:     "name",
			dataType: "string",
		},
		{
			name:     "age",
			dataType: "string",
		},
	}
	keyCSVSchema := "name string,age string"
	assert.Nil(t, err)
	assert.Equal(t, expect, got)
	assert.Equal(t, keyCSVSchema, c[KeyCSVSchema])
}

func Test_checkLabelAndSplitterKey(t *testing.T) {
	type args struct {
		schema             []field
		labelList          []string
		containSplitterKey string
	}
	tests := []struct {
		name      string
		args      args
		want      []GrokLabel
		wantIndex int
		wantErr   bool
	}{
		{
			name: "test_with_label_and_splitter_key",
			args: args{
				schema: []field{
					{
						name:     "name",
						dataType: "string",
					},
					{
						name:     "age",
						dataType: "long",
					},
				},
				labelList: []string{
					"major string",
					"time date",
				},
				containSplitterKey: "age",
			},
			want: []GrokLabel{
				{
					Name:  "major",
					Value: "string",
				},
				{
					Name:  "time",
					Value: "date",
				},
			},
			wantIndex: 1,
			wantErr:   false,
		},
		{
			name: "test_without_label_and_splitter_key",
			args: args{
				schema: []field{
					{
						name:     "name",
						dataType: "string",
					},
					{
						name:     "age",
						dataType: "long",
					},
				},
			},
			want:      []GrokLabel{},
			wantIndex: -1,
			wantErr:   false,
		},
		{
			name: "column_duplicated",
			args: args{
				schema: []field{
					{
						name:     "name",
						dataType: "string",
					},
					{
						name:     "name",
						dataType: "string",
					},
				},
				labelList: []string{},
			},
			want:      nil,
			wantIndex: -1,
			wantErr:   true,
		},
		{
			name: "splitter_key_exist",
			args: args{
				schema: []field{
					{
						name:     "name",
						dataType: "string",
					},
					{
						name:     "age",
						dataType: "long",
					},
				},
				containSplitterKey: "age",
			},
			want:      []GrokLabel{},
			wantIndex: 1,
			wantErr:   false,
		},
		{
			name: "splitter_key_exist_but_not_found",
			args: args{
				schema: []field{
					{
						name:     "name",
						dataType: "string",
					},
					{
						name:     "age",
						dataType: "long",
					},
				},
				containSplitterKey: "major",
			},
			want:      nil,
			wantIndex: -1,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, index, err := checkLabelAndSplitterKey(tt.args.schema, tt.args.labelList, tt.args.containSplitterKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkLabel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if index != tt.wantIndex {
				t.Errorf("checkLabel() containSplitterKey = %v, wantKeyIndex %v", index, tt.wantIndex)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkLabel() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFields_toString(t *testing.T) {
	tests := []struct {
		name string
		f    Fields
		want string
	}{
		{
			name: "test_empty",
			f:    Fields{},
			want: "",
		},
		{
			name: "test_normal",
			f: Fields{
				{
					name:     "name",
					dataType: "string",
				},
				{
					name:     "age",
					dataType: "string",
				},
			},
			want: "name string,age string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.toString(); got != tt.want {
				t.Errorf("toString() = %v, want %v", got, tt.want)
			}
		})
	}
}
