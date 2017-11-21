package utils

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fmt"

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

func Test_GetInode(t *testing.T) {
	os.Mkdir("abc", 0777)
	fi, _ := os.Stat("abc")
	inode := getInode(fi)
	assert.True(t, inode > 0)
	os.RemoveAll("abc")
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
		assert.EqualValues(t, ti.exp, exps)
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
