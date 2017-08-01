package parser

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

var csvBench []sender.Data

func Benchmark_CsvParseLine(b *testing.B) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap"
	c[KeyCSVSplitter] = " "
	p, _ := NewCsvParser(c)

	var m []sender.Data
	for n := 0; n < b.N; n++ {
		m, _ = p.Parse([]string{`123 fufu 3.16 {\"x\":1,\"y\":[\"xx:12\"]}`})
	}
	csvBench = m
}

func Test_CsvParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float, d jsonmap,e date"
	c[KeyCSVSplitter] = " "
	parser, err := NewCsvParser(c)
	if err != nil {
		t.Error(err)
	}
	tmstr := time.Now().Format(time.RFC3339Nano)
	lines := []string{
		"123 fufu 3.14 {\"x\":1,\"y\":\"2\"} " + tmstr,       //correct
		"cc jj uu {\"x\":1,\"y\":\"2\"} " + tmstr,            // error => uu 不是float
		"123 fufu 3.15 999 " + tmstr,                         //error，999不是jsonmap
		"123 fufu 3.16 {\"x\":1,\"y\":[\"xx:12\"]} " + tmstr, //correct
		"   ",
		"123 fufu 3.17  " + tmstr, //correct,jsonmap允许为空
	}
	datas, err := parser.Parse(lines)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}

	exp := make(map[string]interface{})
	exp["a"] = int64(123)
	exp["b"] = "fufu"
	exp["c"] = 3.14
	exp["d-x"] = float64(1)
	exp["d-y"] = "2"
	exp["e"] = tmstr
	for k, v := range datas[0] {
		if v != exp[k] {
			t.Errorf("expect %v but got %v", v, exp[k])
		}
	}

	expNum := 3
	if len(datas) != expNum {
		t.Errorf("correct line should be %v, but got %v", expNum, len(datas))
	}
	if datas[0]["a"] != int64(123) {
		t.Errorf("a should be 123  but got %v", datas[0]["a"])
	}
	if "fufu" != datas[0]["b"] {
		t.Error("b should be fufu")
	}
	assert.EqualValues(t, parser.Name(), "testparser")
}

func Test_Jsonmap(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testjsonmap"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, d jsonmap,e jsonmap{x string,y long},f jsonmap{z float, ...}"
	c[KeyCSVSplitter] = " "
	parser, err := NewCsvParser(c)
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{
		"123 {\"x\":1,\"y\":\"2\"} {\"x\":1,\"y\":\"2\",\"z\":\"3\"} {\"x\":1.0,\"y\":\"2\",\"z\":\"3.0\"}",
	}
	datas, err := parser.Parse(lines)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	d := datas[0]
	if d["f-x"] != 1.0 {
		t.Errorf("f-x should be float 1 but %v %v", reflect.TypeOf(d["f-x"]), d["f-x"])
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
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, b string, c float"
	c[KeyLabels] = "d nb1684"
	c[KeyCSVSplitter] = " "
	parser, err := NewCsvParser(c)
	if err != nil {
		t.Error(err)
	}
	lines := []string{
		"123 fufu 3.14",
		"cc jj uu",
		"123 fufu 3.14 999",
	}
	datas, err := parser.Parse(lines)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	if len(datas) != 1 {
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

}

func Test_CsvParserDupColumn1(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "testparser"
	c[KeyParserType] = "csv"
	c[KeyCSVSchema] = "a long, a string, c float"
	c[KeyCSVSplitter] = " "
	_, err := NewCsvParser(c)
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
	f, err := parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(f)

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
	f, err = parseSchemaFields(fields)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(f)
}

func Test_convertValue(t *testing.T) {
	jsonraw := "{\"a\":null}"
	m := make(map[string]interface{})
	if err := json.Unmarshal([]byte(jsonraw), &m); err != nil {
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
}
