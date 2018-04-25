package models

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ReadDirSortByTime(t *testing.T) {
	testreaddir := "../tests/testreaddir/"
	err := os.MkdirAll(testreaddir, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(testreaddir)
	exps := []string{"4", "1", "2", "3"}
	for i := len(exps) - 1; i >= 0; i-- {
		e := exps[i]
		if i > 0 {
			_, err := os.Create(testreaddir + e)
			if err != nil {
				t.Error(err)
			}
		} else {
			err := os.Mkdir(testreaddir+e, os.ModePerm)
			if err != nil {
				t.Error(err)
			}
		}
		time.Sleep(time.Second)
	}
	files, err := ReadDirByTime(testreaddir)
	if err != nil {
		t.Fatal(err)
	}
	var gots []string
	for _, f := range files {
		gots = append(gots, f.Name())
	}
	if !reflect.DeepEqual(gots, exps) {
		t.Fatalf("Test_ReadDirSortByTime error exps %v got %v ", exps, gots)
	}
}

func Test_TrimeList(t *testing.T) {
	s := []string{"1", "  \t \n", " \n ", "2"}
	exps := []string{"1", "2"}
	gots := TrimeList(s)
	if !reflect.DeepEqual(gots, exps) {
		t.Errorf("Test_TrimeList error exps %v got %v", exps, gots)
	}
}

func Test_GetLogFiles(t *testing.T) {
	logfiles := "Test_getLogFiles"
	log1 := logfiles + "/log1"
	log2 := logfiles + "/log2"
	log3 := logfiles + "/log3"
	logs := log1 + "\n" + log2 + "\n" + log3 + "\n"
	err := os.Mkdir(logfiles, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	filedone := filepath.Join(logfiles, "file.done.2016-10-01")
	_, err = os.Create(log1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	_, err = os.Create(log2)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(filedone, []byte(logs), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	files := GetLogFiles(filedone)
	exps := []string{"log2", "log1"}
	var gots []string
	for _, f := range files {
		gots = append(gots, f.Info.Name())
	}
	if !reflect.DeepEqual(exps, gots) {
		t.Errorf("Test_getLogFiles error exp %v but got %v", exps, gots)
	}
	err = os.RemoveAll(logfiles)
	if err != nil {
		t.Error(err)
	}
}

func TestParseSystemEnv(t *testing.T) {
	var exceptedValue string = "mockEnv"
	err := os.Setenv("test", exceptedValue)
	if err != nil {
		t.Error(err)
	}

	defer os.Clearenv()

	var envOr string = "${test}"
	result := GetEnv(envOr)

	assert.Equal(t, exceptedValue, result)
}

func TestTuoEncodeDecode(t *testing.T) {
	tests := []struct {
		exp []string
	}{
		{
			exp: []string{"a", "1.1", "1", "2016.1.2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		},
		{
			exp: []string{"0.1", "1", "2016.1.2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "a"},
		},
		{
			exp: []string{"100000", "2016.1.2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "a", "1.1"},
		},
		{
			exp: []string{"2016.1.2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "a", "1.1", "100000"},
		},
	}
	for _, ti := range tests {
		var (
			values []sql.RawBytes
			exps   []string
		)
		for _, v := range ti.exp {
			values = append(values, sql.RawBytes(v))
		}
		ret := TuoEncode(values)
		gots, err := TuoDecode(ret)
		if err != nil {
			t.Error(err)
		}
		for _, g := range gots {
			exps = append(exps, string(g))
		}
		assert.Equal(t, ti.exp, exps)
	}
}

func TestIsJsonString(t *testing.T) {
	cases := []struct {
		c   string
		exp bool
	}{
		{
			`[{"a":1}]`,
			true,
		},
		{
			`{"a":1}`,
			true,
		},
		{
			`{"a":1`,
			false,
		},
		{
			`xsx`,
			false,
		},
		{
			` `,
			false,
		},
		{
			`null`,
			false,
		},
		{
			`{"a": null}`,
			true,
		},
	}
	for _, c := range cases {
		got := IsJsonString(c.c)
		assert.Equal(t, c.exp, got)
	}
}

func TestAddRemoveHttpProc(t *testing.T) {
	exp := "127.0.0.1:122"
	url := AddHttpProtocal(exp)
	assert.Equal(t, "http://"+exp, url)
	got, chttp := RemoveHttpProtocal(url)
	assert.Equal(t, exp, got)
	assert.Equal(t, "http://", chttp)

	exp2 := ":1233"
	got2, chttp2 := RemoveHttpProtocal(exp2)
	assert.Equal(t, exp2, got2)
	assert.Equal(t, "http://", chttp2)

}

func TestExtractField(t *testing.T) {
	slice1 := []string{"default"}
	slice1, err1 := ExtractField(slice1)
	assert.NoError(t, err1)
	assert.Equal(t, slice1, []string{"default"})

	slice2 := []string{"%{[type]}", "default"}
	slice2, err2 := ExtractField(slice2)
	assert.NoError(t, err2)
	assert.Equal(t, []string{"type", "default"}, slice2)

	slice3 := []string{"%{[type}", "default"}
	slice3, err3 := ExtractField(slice3)
	assert.Error(t, err3)

}

func TestGetKeys(t *testing.T) {
	exp := []string{}

	var keyStr string
	res := GetKeys(keyStr)
	assert.Equal(t, exp, res)

	keyStr2 := "."
	res2 := GetKeys(keyStr2)
	assert.Equal(t, exp, res2)

	keyStr3 := "a..."
	res3 := GetKeys(keyStr3)
	exp3 := []string{"a"}
	assert.Equal(t, exp3, res3)
}

func TestGetMapValue(t *testing.T) {
	m3 := map[string]interface{}{"name": "小明"}
	m2 := map[string]interface{}{"m3": m3}
	m1 := map[string]interface{}{"m2": m2}
	//keys存在
	value, err := GetMapValue(m1, []string{"m2", "m3", "name"}...)
	assert.NoError(t, err)
	assert.Equal(t, value, "小明")
	//keys不存在
	value2, err2 := GetMapValue(m1, []string{"m2", "m3", "m4"}...)
	assert.Error(t, err2)
	assert.Equal(t, nil, value2)
	//keys为空
	value3, err3 := GetMapValue(m1, []string{}...)
	assert.NoError(t, err3)
	assert.Equal(t, m1, value3)
	//存在非map[string]interface{}
	m4 := map[string]interface{}{"m5": map[string]string{"name": "小明"}}
	value4, err4 := GetMapValue(m4, []string{"m5", "name"}...)
	assert.Error(t, err4)
	assert.Equal(t, nil, value4)
}

func getTestMap() map[string]interface{} {
	m := map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": "v22",
		},
	}
	return m
}

func TestSetMapValue(t *testing.T) {
	var m map[string]interface{}

	//key为空,原map不变
	m = getTestMap()
	err1 := SetMapValue(m, "", false)
	assert.NoError(t, err1)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": "v22",
		},
	}, m)

	m = getTestMap()
	err2 := SetMapValue(m, "小明", false, "k11", "k21", "k31")
	assert.NoError(t, err2)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "小明",
			},
		},
		"k12": map[string]interface{}{
			"k22": "v22",
		},
	}, m)

	m = getTestMap()
	err3 := SetMapValue(m, map[string]interface{}{"k32": "v32"}, false, "k12", "k22")
	assert.NoError(t, err3)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": map[string]interface{}{
				"k32": "v32",
			},
		},
	}, m)

	m = getTestMap()
	err4 := SetMapValue(m, "小明", false, "k13")
	assert.NoError(t, err4)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": "v22",
		},
		"k13": "小明",
	}, m)

	m = getTestMap()
	err5 := SetMapValue(m, "小明", false, "k11")
	assert.Error(t, err5)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": "v22",
		},
	}, m)

	m = getTestMap()
	err6 := SetMapValue(m, "小明", false, "k12", "k22", "k32")
	assert.Error(t, err6)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": "v22",
		},
	}, m)

	m = getTestMap()
	err7 := SetMapValue(m, "小明", true, "k12", "k22", "k32")
	assert.NoError(t, err7)
	assert.Equal(t, map[string]interface{}{
		"k11": map[string]interface{}{
			"k21": map[string]interface{}{
				"k31": "v31",
			},
		},
		"k12": map[string]interface{}{
			"k22": map[string]interface{}{
				"k32": "小明",
			},
		},
	}, m)
}

func TestDeleteMapValue(t *testing.T) {
	m3 := map[string]interface{}{"name": "小明"}
	m2 := map[string]interface{}{"m3": m3}
	m1 := map[string]interface{}{"m2": m2}
	val, b := DeleteMapValue(m1, []string{"m2", "m3", "name"}...)
	assert.Equal(t, val, "小明")
	assert.Equal(t, b, true)

	val2, b2 := DeleteMapValue(m1, []string{"m2", "m3", "name", "name2"}...)
	assert.Equal(t, val2, nil)
	assert.Equal(t, b2, false)
}

func TestDeepCopy(t *testing.T) {
	m := getTestMap()
	m1 := DeepCopy(m)
	SetMapValue(m, "value", true, "k13")
	assert.Equal(t, m1, getTestMap())
	s1 := []interface{}{getTestMap()}
	s2 := DeepCopy(s1)
	s1 = append(s1, m)
	assert.Equal(t, s2, []interface{}{getTestMap()})
}

func TestHashSet(t *testing.T) {
	set := NewHashSet()
	const CNT = 100
	arr := make([]int, CNT)
	for i := 0; i < CNT; i++ {
		arr[i] = i
	}
	for _, v := range arr {
		set.Add(v)
	}
	assert.Equal(t, false, set.IsEmpty())
	assert.Equal(t, len(arr), set.Len())

	for i := 0; i < CNT; i += 2 {
		set.Remove(i)
	}
	assert.Equal(t, len(arr)/2, set.Len())
	for i := 1; i < CNT; i += 2 {
		assert.Equal(t, true, set.IsIn(i), i)
	}
	set.Clear()
	assert.Equal(t, true, set.IsEmpty())
	arrStr := []string{"1", "2", "3", "4", "5"}
	set.AddStringArray(arrStr)
	assert.Equal(t, len(arrStr), set.Len())
}

func TestLogDirAndPattern(t *testing.T) {
	dir1, pt1, err := LogDirAndPattern("TestLogDirAndPattern.log")
	assert.NoError(t, err)
	assert.Equal(t, pt1, "TestLogDirAndPattern.log")

	dir2, pt2, err := LogDirAndPattern("./TestLogDirAndPattern.log")
	assert.NoError(t, err)
	assert.Equal(t, pt2, "TestLogDirAndPattern.log")
	assert.Equal(t, dir1, dir2)

	absf, err := filepath.Abs("TestLogDirAndPattern")
	if err != nil {
		return
	}
	dir1, pt1, err = LogDirAndPattern("TestLogDirAndPattern/TestLogDirAndPattern.log")
	assert.NoError(t, err)
	assert.Equal(t, absf, dir1)
	assert.Equal(t, pt1, "TestLogDirAndPattern.log")
	defer os.RemoveAll("TestLogDirAndPattern")

}

func Test_checkFileMode(t *testing.T) {
	fileName := os.TempDir() + "/checkFileMode.sh"
	//create file & write file
	createTestFile(fileName, "echo \"hello world\"")
	defer os.RemoveAll(fileName)
	err := os.Chmod(fileName, 0666)
	if err != nil {
		t.Error(err)
	}

	realPath, fileInfo, err := GetRealPath(fileName)
	if err != nil {
		t.Error(err)
	}
	if fileInfo == nil {
		err = fmt.Errorf("fileInfo of fileName [%v] is nil", fileName)
		t.Error(err)
	}
	fileMode := fileInfo.Mode()
	assert.Equal(t, os.FileMode(0x1b6), fileMode)

	err = CheckFileMode(realPath, fileMode)
	if err != nil {
		t.Error(err)
	}

	_, fileInfoNew, err := GetRealPath(fileName)
	if err != nil {
		t.Error(err)
	}
	if fileInfo == nil {
		err = fmt.Errorf("fileInfo of fileName [%v] is nil", fileName)
		t.Error(err)
	}
	fileModeNew := fileInfoNew.Mode()
	assert.Equal(t, os.FileMode(0x1ff), fileModeNew)
}

func Test_EncodeString(t *testing.T) {
	originEncodeStr1 := "test encode string \n and then decode string"
	encode1 := EncodeString(originEncodeStr1)
	decode1, err := DecodeString(encode1)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, originEncodeStr1, decode1)

	originEncodeStr2 := "中文加密测试 \n then 解密"
	encode2 := EncodeString(originEncodeStr2)
	decode2, err := DecodeString(encode2)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, originEncodeStr2, decode2)
}

func Test_DecodeString(t *testing.T) {
	originEncodeStr1 := "发送时间[2017-03-29 10:45:12.07],接收时间[20T08:25:59.124345]"
	encode1 := EncodeString(originEncodeStr1)
	decode1, err := DecodeString(encode1)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, originEncodeStr1, decode1)

	originEncodeStr2 := `<?xml version="1.0" encoding="UTF-8" ?>`
	encode2 := EncodeString(originEncodeStr2)
	decode2, err := DecodeString(encode2)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, originEncodeStr2, decode2)
}

func createTestFile(fileName string, content string) {
	f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}
