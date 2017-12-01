package utils

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

/*func Test_GetInode(t *testing.T) {
	os.Mkdir("abc", 0777)
	fi, _ := os.Stat("abc")
	inode := getInode(fi)
	assert.True(t, inode > 0)
	os.RemoveAll("abc")
}*/

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
	}
	for _, c := range cases {
		got := IsJSON(c.c)
		assert.Equal(t, c.exp, got)
	}
}

func TestGetLocalIp(t *testing.T) {
	ip, err := GetLocalIP()
	assert.NoError(t, err)
	fmt.Println(ip)
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
	fmt.Println(err4)
	assert.Equal(t, nil, value4)
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
