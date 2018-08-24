package models

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/times"
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
		time.Sleep(100 * time.Millisecond)
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

func Test_SortFilesByTime(t *testing.T) {
	testreaddir := "SortFilesByTime"
	err := os.MkdirAll(testreaddir, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(testreaddir)
	exps := []string{"4", "1", "3", "2"}
	now := time.Now()
	for i := 0; i < 4; i++ {
		e := exps[i]
		filename := filepath.Join(testreaddir, e)
		os.Create(filename)
		os.Chtimes(filename, now, now)
	}
	files, err := ReadDirByTime(testreaddir)
	if err != nil {
		t.Fatal(err)
	}
	exps = []string{"4", "3", "2", "1"}
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
	time.Sleep(100 * time.Millisecond)
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

	m6 := map[string]interface{}{"m6": Data{"name": "小明"}}
	value6, err6 := GetMapValue(m6, []string{"m6", "name"}...)
	assert.NoError(t, err6)
	assert.Equal(t, "小明", value6)
}

func TestSetMapValue(t *testing.T) {
	m3 := map[string]interface{}{"name": "小明"}
	m2 := map[string]interface{}{"m3": m3}
	m1 := map[string]interface{}{"m2": m2}

	err := SetMapValue(m1, "m1", false)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"m2": map[string]interface{}{"m3": map[string]interface{}{"name": "小明"}}}, m1)

	err11 := SetMapValue(m1, "小红", false, []string{"m2", "m3", "name"}...)
	value1, err12 := GetMapValue(m1, []string{"m2", "m3", "name"}...)
	assert.NoError(t, err11)
	assert.NoError(t, err12)
	assert.Equal(t, value1, "小红")

	err21 := SetMapValue(m1, "小黑", false, []string{"m2", "m3", "m4", "name"}...)
	value2, err22 := GetMapValue(m1, []string{"m2", "m3", "m4", "name"}...)
	assert.NoError(t, err21)
	assert.NoError(t, err22)
	assert.Equal(t, value2, "小黑")

	err31 := SetMapValue(m1, "name1", false, []string{"m2", "m3", "name", "name1"}...)
	value31, err32 := GetMapValue(m1, []string{"m2", "m3", "name"}...)
	value32, err33 := GetMapValue(m1, []string{"m2", "m3", "name", "name1"}...)
	assert.Error(t, err31)
	assert.NoError(t, err32)
	assert.Equal(t, "小红", value31)
	assert.Error(t, err33)
	assert.Equal(t, nil, value32)

	err41 := SetMapValue(m1, "name1", true, []string{"m2", "m3", "name", "name1"}...)
	value41, err42 := GetMapValue(m1, []string{"m2", "m3", "name"}...)
	value42, err43 := GetMapValue(m1, []string{"m2", "m3", "name", "name1"}...)
	assert.NoError(t, err41)
	assert.NoError(t, err42)
	assert.Equal(t, map[string]interface{}{"name1": "name1"}, value41)
	assert.NoError(t, err43)
	assert.Equal(t, "name1", value42)

	data := Data{"dkey1": "data1"}
	err51 := SetMapValue(m1, data, true, []string{"d1", "name1"}...)
	value51, err52 := GetMapValue(m1, []string{"d1", "name1"}...)
	value52, err53 := GetMapValue(m1, []string{"d1", "name1", "dkey1"}...)
	assert.NoError(t, err51)
	assert.NoError(t, err52)
	assert.NoError(t, err53)
	assert.Equal(t, data, value51)
	assert.Equal(t, "data1", value52)

	err54 := SetMapValue(m1, data, true, []string{"d1", "name1", "dkey2"}...)
	value53, err55 := GetMapValue(m1, []string{"d1", "name1", "dkey2"}...)
	value54, err56 := GetMapValue(m1, []string{"d1", "name1", "dkey2", "dkey1"}...)
	assert.NoError(t, err54)
	assert.NoError(t, err55)
	assert.NoError(t, err56)
	assert.Equal(t, data, value53)
	assert.Equal(t, "data1", value54)

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

	m4 := map[string]interface{}{"name": "小明", "data": Data{"name": "Lily"}}
	val3, b3 := DeleteMapValue(m4, []string{"data", "name"}...)
	assert.Equal(t, val3, "Lily")
	assert.Equal(t, b3, true)
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

func TestDecompressZip(t *testing.T) {
	testdataDir := "testdata"
	unpackDir := "testout_zip"
	defer os.RemoveAll(unpackDir)

	tests := []struct {
		name       string
		srcPath    string
		dstPath    string
		targetFile string
		targetDir  string
	}{
		{
			"case 1",
			filepath.Join(testdataDir, "target_in_root.zip"),
			filepath.Join(unpackDir, "target_in_root"),
			"logkit",
			filepath.Join(unpackDir, "target_in_root"),
		},
		{
			"case 2",
			filepath.Join(testdataDir, "target_in_subdir.zip"),
			filepath.Join(unpackDir, "target_in_subdir"),
			"logkit.exe",
			filepath.Join(unpackDir, "target_in_subdir", "windows"),
		},
		{
			"case 3",
			filepath.Join(testdataDir, "two_targets.zip"),
			filepath.Join(unpackDir, "two_targets"),
			"logkit.exe",
			filepath.Join(unpackDir, "two_targets"),
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			targetDir, err := DecompressZip(tc.srcPath, tc.dstPath, tc.targetFile)
			assert.NoError(t, err)
			assert.Equal(t, tc.targetDir, targetDir)
		})
	}
}

func TestDecompressTarGzip(t *testing.T) {
	testdataDir := "testdata"
	unpackDir := "testout_targz"
	defer os.RemoveAll(unpackDir)

	tests := []struct {
		name       string
		srcPath    string
		dstPath    string
		targetFile string
		targetDir  string
	}{
		{
			"case 1",
			filepath.Join(testdataDir, "target_in_root.tar.gz"),
			filepath.Join(unpackDir, "target_in_root"),
			"logkit",
			filepath.Join(unpackDir, "target_in_root"),
		},
		{
			"case 2",
			filepath.Join(testdataDir, "target_in_subdir.tar.gz"),
			filepath.Join(unpackDir, "target_in_subdir"),
			"logkit",
			filepath.Join(unpackDir, "target_in_subdir", "linux"),
		},
		{
			"case 3",
			filepath.Join(testdataDir, "two_targets.tar.gz"),
			filepath.Join(unpackDir, "two_targets"),
			"logkit",
			filepath.Join(unpackDir, "two_targets"),
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			targetDir, err := DecompressTarGzip(tc.srcPath, tc.dstPath, tc.targetFile)
			assert.NoError(t, err)
			assert.Equal(t, tc.targetDir, targetDir)
		})
	}
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

func Test_ConvertDate(t *testing.T) {
	date, err := ConvertDate("", "", 0, time.UTC, 1525422699)
	assert.NoError(t, err)
	expect, err := getTimeStr(int64(1525422699))
	assert.NoError(t, err)
	assert.Equal(t, expect, date)

	date, err = ConvertDate("", "", 0, time.UTC, "Feb 05 01:02:03")
	assert.NoError(t, err)
	assert.Equal(t, "0000-02-05T01:02:03Z", date)

	date, err = ConvertDate("", "", 0, time.UTC, "19/Aug/2000:14:47:37 -0400")
	assert.NoError(t, err)
	assert.Equal(t, "2000-08-19T14:47:37-04:00", date)

	date, err = ConvertDate("20060102150405", "", 0, time.UTC, "20180204221045")
	assert.NoError(t, err)
	assert.Equal(t, "2018-02-04T22:10:45Z", date)
}

func Test_FormatWithUserOption(t *testing.T) {
	ti, err := times.StrToTime("Feb 05 01:02:03")
	assert.NoError(t, err)
	date := FormatWithUserOption("", 0, ti)
	assert.Equal(t, "0000-02-05T01:02:03Z", date)

	ti, err = time.Parse("20060102150405", "20180204221045")
	assert.NoError(t, err)
	date = FormatWithUserOption("", 0, ti)
	assert.Equal(t, "2018-02-04T22:10:45Z", date)

	ti, err = getTime(int64(1525422699))
	assert.NoError(t, err)

	date = FormatWithUserOption("", 0, ti)
	assert.Equal(t, ti.Format(time.RFC3339Nano), date)
}

func getTime(tiTmp int64) (ti time.Time, err error) {
	timestamp := strconv.FormatInt(tiTmp, 10)
	timeSecondPrecision := 16
	//补齐16位
	for i := len(timestamp); i < timeSecondPrecision; i++ {
		timestamp += "0"
	}
	// 取前16位，截取精度 微妙
	timestamp = timestamp[0:timeSecondPrecision]
	parseTi, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return
	}

	return time.Unix(0, parseTi*int64(time.Microsecond)), nil
}

func getTimeStr(tiTmp int64) (tiStr string, err error) {
	ti, err := getTime(tiTmp)
	if err != nil {
		return
	}
	return ti.Format(time.RFC3339Nano), err
}

func TestGetMapList(t *testing.T) {
	cases := []struct {
		c   string
		exp map[string]string
	}{
		{
			`a b,1,2 c`,
			map[string]string{
				"a": "b",
				"2": "c",
			},
		},
		{
			`1 abc,2 xyz`,
			map[string]string{
				"1": "abc",
				"2": "xyz",
			},
		},
		{
			`1 2,3,3,,4 aby`,
			map[string]string{
				"1": "2",
				"4": "aby",
			},
		},
		{
			``,
			map[string]string{},
		},
	}
	for _, c := range cases {
		got := GetMapList(c.c)
		assert.Equal(t, c.exp, got)
	}
}

func TestPickMapValue(t *testing.T) {
	var m = map[string]interface{}{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}

	var exp = map[string]interface{}{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}
	pick := map[string]interface{}{}
	PickMapValue(m, pick, "multi")
	assert.Equal(t, exp, pick)

	exp = map[string]interface{}{"multi": map[string]interface{}{"abc": "x1 y2"}}
	pick = map[string]interface{}{}
	PickMapValue(m, pick, "multi", "abc")
	assert.Equal(t, exp, pick)

	exp = map[string]interface{}{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}
	pick = map[string]interface{}{}
	PickMapValue(m, pick, "multi", "abc")
	PickMapValue(m, pick, "multi", "myword")
	assert.Equal(t, exp, pick)

	exp = map[string]interface{}{"multi": map[string]interface{}{"abc": "x1 y2"}}
	pick = map[string]interface{}{}
	PickMapValue(m, pick, "multi", "abc")
	PickMapValue(m, pick, "multi", "otherword")
	assert.Equal(t, exp, pick)

	exp = map[string]interface{}{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}
	pick = map[string]interface{}{}
	PickMapValue(m, pick, "multi")
	PickMapValue(m, pick, "multi", "otherword")
	assert.Equal(t, exp, pick)

	exp = map[string]interface{}{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}
	pick = map[string]interface{}{}
	PickMapValue(m, pick, "multi", "abc", "xxx")
	PickMapValue(m, pick, "multi", "otherword")
	assert.NotEqual(t, exp, pick)

	exp = map[string]interface{}{"multi": Data{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}
	pick = map[string]interface{}{}
	PickMapValue(m, pick, "multi", "abc", "xxx")
	PickMapValue(m, pick, "multi", "otherword")
	assert.NotEqual(t, exp, pick)
}

func TestPandoraKey(t *testing.T) {
	testKeys := []string{"@timestamp", ".dot", "percent%100", "^^^^^^^^^^", "timestamp"}
	expectKeys := []string{"timestamp", "dot", "percent_100", "", "timestamp"}
	expectValid := []bool{false, false, false, false, true}
	for idx, key := range testKeys {
		actual, valid := PandoraKey(key)
		assert.Equal(t, expectKeys[idx], actual)
		assert.Equal(t, expectValid[idx], valid)
	}
}

func TestCheckPandoraKey(t *testing.T) {
	testKeys := []string{"@timestamp", ".dot", "percent%100", "^^^^^^^^^^", "timestamp"}
	expectValid := []bool{false, false, false, false, true}
	for idx, key := range testKeys {
		valid := CheckPandoraKey(key)
		assert.Equal(t, expectValid[idx], valid)
	}
}

func BenchmarkPandoraKey(b *testing.B) {
	b.ReportAllocs()
	testKeys := []string{"@timestamp", ".dot", "percent%100", "^^^^^^^^^^", "timestamp", "aaa"}
	for i := 0; i < b.N; i++ {
		for _, key := range testKeys {
			PandoraKey(key)
		}
	}
}

func BenchmarkCheckPandoraKey(b *testing.B) {
	b.ReportAllocs()
	testKeys := []string{"@timestamp", ".dot", "percent%100", "^^^^^^^^^^", "timestamp", "aaa"}
	for i := 0; i < b.N; i++ {
		for _, key := range testKeys {
			CheckPandoraKey(key)
		}
	}
}

//1000000          1493 ns/op          32 B/op           2 allocs/op
func BenchmarkDeepConvertKey(b *testing.B) {
	b.ReportAllocs()
	testDatas := []map[string]interface{}{
		{
			"@timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			".dot": map[string]interface{}{".dot2": "dot"},
		},
		{
			"dot":         map[string]interface{}{".dot2": "dot"},
			"percent%100": 100,
			"^^^^^^^^^^":  "mytest",
		},
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":        map[string]interface{}{"dot2": "dot"},
			"percent100": 100,
			"axsxs":      "mytest",
		},
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
			//"timestamp":  "2018-07-19T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":        map[string]interface{}{"dot2": "dot"},
			"percent100": 100,
			"axsxs":      "mytest",
		},
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":        map[string]interface{}{"dot2": "dot"},
			"percent100": 100,
			"axsxs":      "mytest",
		},
	}
	for i := 0; i < b.N; i++ {
		for _, data := range testDatas {
			DeepConvertKey(data)
		}
	}
}

func TestDeepConvertKey(t *testing.T) {
	testDatas := []map[string]interface{}{
		{
			"@timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			".dot": map[string]interface{}{".dot2": "dot"},
		},
		{
			"dot":         map[string]interface{}{".dot2": "dot"},
			"percent%100": 100,
			"^^^^^^^^^^":  "mytest",
		},
	}
	expectDatas := []map[string]interface{}{
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":         map[string]interface{}{"dot2": "dot"},
			"percent_100": 100,
			"":            "mytest",
		},
	}

	for idx, data := range testDatas {
		actual := DeepConvertKey(data)
		assert.Equal(t, expectDatas[idx], actual)
	}
}

func TestDeepConvertKeyWithCache(t *testing.T) {
	testDatas := []map[string]interface{}{
		{
			"@timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			".dot": map[string]interface{}{".dot2": "dot"},
		},
		{
			"dot":         map[string]interface{}{".dot2": "dot"},
			"percent%100": 100,
			"^^^^^^^^^^":  "mytest",
		},
	}
	expectDatas := []map[string]interface{}{
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":         map[string]interface{}{"dot2": "dot"},
			"percent_100": 100,
			"":            "mytest",
		},
	}
	cache := make(map[string]KeyInfo)
	for idx, data := range testDatas {
		actual := DeepConvertKeyWithCache(data, cache)
		assert.Equal(t, expectDatas[idx], actual)
	}
}

func Test_DeepConvertKey(t *testing.T) {
	testDatas := []Data{
		{
			"a.....b": "a.....b",
			"b":       true,
		},
		{
			"....a+b": []string{"a", "b", "....a+b"},
			"abc":     "abc",
		},
		{
			"a": Data{"a-=b++": "a-=b++"},
			"b": Data{"--ab": Data{"abc++": "abc++"}},
		},
		{
			"a": map[string]interface{}{"b:1": 123},
		},
	}
	expectDatas := []map[string]interface{}{
		{
			"a_____b": "a.....b",
			"b":       true,
		},
		{
			"a_b": []string{"a", "b", "....a+b"},
			"abc": "abc",
		},
		{
			"a": Data{"a__b__": "a-=b++"},
			"b": Data{"ab": map[string]interface{}{"abc__": "abc++"}},
		},
		{
			"a": map[string]interface{}{"b_1": 123},
		},
	}

	for idx, testData := range testDatas {
		actualData := DeepConvertKey(testData)
		assert.Equal(t, expectDatas[idx], actualData, fmt.Sprintf("index %v", idx))
	}
}

//1000000          1647 ns/op           0 B/op           0 allocs/op
func BenchmarkDeepConvertKeyWithCache(b *testing.B) {
	b.ReportAllocs()
	testDatas := []map[string]interface{}{
		{
			"@timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			".dot": map[string]interface{}{".dot2": "dot"},
		},
		{
			"dot":         map[string]interface{}{".dot2": "dot"},
			"percent%100": 100,
			"^^^^^^^^^^":  "mytest",
		},
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":        map[string]interface{}{"dot2": "dot"},
			"percent100": 100,
			"axsxs":      "mytest",
		},
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":        map[string]interface{}{"dot2": "dot"},
			"percent100": 100,
			"axsxs":      "mytest",
		},
		{
			"timestamp": "2018-07-18T10:17:36.549054846+08:00",
		},
		{
			"dot": map[string]interface{}{"dot2": "dot"},
		},
		{
			"dot":        map[string]interface{}{"dot2": "dot"},
			"percent100": 100,
			"axsxs":      "mytest",
		},
	}
	cache := make(map[string]KeyInfo)
	for i := 0; i < b.N; i++ {
		for _, data := range testDatas {
			DeepConvertKeyWithCache(data, cache)
		}
	}
}

func Test_CheckErrorSize(t *testing.T) {
	err := "Test_CheckErrorSize"
	actualErr := TruncateStrSize(err, DefaultTruncateMaxSize)
	assert.Equal(t, err, actualErr)

	for {
		if len(err) > DefaultTruncateMaxSize {
			break
		}
		err += "Test_CheckErrorSize"
	}

	actualErr = TruncateStrSize(err, DefaultTruncateMaxSize)
	assert.Equal(t, err[:DefaultTruncateMaxSize]+
		"......(only show 1024 bytes, remain "+
		strconv.Itoa(len(err)-DefaultTruncateMaxSize)+" bytes)", actualErr)
}
