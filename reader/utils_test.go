package reader

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestFindFile(t *testing.T) {
	CreateFileForTest(1000)
	defer DestroyDir()

	fi, err := getLatestFile(Dir)
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != "f1" {
		t.Errorf("Latest file is f4, not %v", fi.Name())
	}

	fi, err = getOldestFile(Dir)
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != "f3" {
		t.Errorf("Oldest file is f1, not %v", fi.Name())
	}

}

func TestCondition(t *testing.T) {
	var fi os.FileInfo
	trueCondition := NoCondition
	falseCondition := NotCondition(trueCondition)

	assert.True(t, trueCondition(fi))
	assert.False(t, falseCondition(fi))
	assert.True(t, OrCondition(trueCondition, falseCondition)(fi))
	assert.False(t, AndCondition(trueCondition, falseCondition)(fi))
}

func TestHeadPatternMode(t *testing.T) {
	headreg, err := regexp.Compile("^xx$")
	if err != nil {
		t.Error(err)
	}
	tests := []struct {
		mode   string
		value  interface{}
		experr error
	}{
		{
			mode:   "hahah",
			experr: errors.New("unknown HeadPatternMode hahah"),
		},
		{
			mode:  ReadModeHeadPatternRegexp,
			value: headreg,
		},
		{
			mode:  ReadModeHeadPatternString,
			value: "^xx$",
		},
	}
	for _, ti := range tests {
		reg, err := HeadPatternMode(ti.mode, ti.value)
		if ti.experr != nil {
			assert.EqualError(t, err, ti.experr.Error())
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, headreg, reg)
	}
	headreg, err = regexp.Compile("^{\n$")
	assert.NoError(t, err)
	ret := headreg.Match([]byte(`{
`))
	assert.Equal(t, true, ret)
}

func TestParseDuration(t *testing.T) {
	dur, err := ParseLoopDuration("loop 1s")
	assert.NoError(t, err)
	assert.Equal(t, time.Second, dur)

	dur, err = ParseLoopDuration("loop 1-")
	assert.Error(t, err)
	assert.Equal(t, time.Duration(0), dur)
}

func TestModTimeLater(t *testing.T) {
	dir := "TestModTimeLater"
	err := os.Mkdir(dir, DefaultDirPerm)
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	for _, v := range []string{"f1", "f2", "f3"} {
		err := ioutil.WriteFile(filepath.Join(dir, v), []byte("abc"), 0644)
		assert.NoError(t, err)
	}
	cs, err := GetMaxFile(dir, func(info os.FileInfo) bool { return true }, ModTimeLater)
	assert.NoError(t, err)
	assert.Equal(t, "f3", cs.Name())
	cs, err = GetMinFile(dir, func(info os.FileInfo) bool { return true }, ModTimeLater)
	assert.NoError(t, err)
	assert.Equal(t, "f1", cs.Name())
}

func TestGetTags(t *testing.T) {
	tagFile := "./tagFile.json"
	err := ioutil.WriteFile(tagFile, []byte(`{  
	   	"Title":"tags",
	    "Author":["john","ada","alice"],
	    "IsTrue":true,
	    "Host":99
	  	}`), 0644)
	assert.NoError(t, err)
	defer os.Remove(tagFile)
	err = nil
	exp := map[string]interface{}{
		"Title":  "tags",
		"Author": []interface{}{"john", "ada", "alice"},
		"IsTrue": bool(true),
		"Host":   float64(99),
	}
	tags, err := getTags(tagFile)
	assert.NoError(t, err)
	assert.Equal(t, exp, tags)
}

func TestSetMapValueExistWithPrefix(t *testing.T) {
	data1 := map[string]interface{}{
		"a": "b",
	}
	err1 := SetMapValueExistWithPrefix(data1, "newVal", "prefix", "a")
	assert.NoError(t, err1)
	exp1 := map[string]interface{}{
		"a":        "b",
		"prefix_a": "newVal",
	}
	assert.Equal(t, exp1, data1)

	data2 := map[string]interface{}{
		"a": map[string]interface{}{
			"name": "qiniu",
			"age":  45,
		},
	}
	err2 := SetMapValueExistWithPrefix(data2, "newVal", "prefix", []string{"a", "name"}...)
	assert.NoError(t, err2)
	exp2 := map[string]interface{}{
		"a": map[string]interface{}{
			"name":        "qiniu",
			"age":         45,
			"prefix_name": "newVal",
		},
	}
	assert.Equal(t, exp2, data2)

	err3 := SetMapValueExistWithPrefix(data2, "newVal", "prefix", []string{"xy", "name"}...)
	assert.NoError(t, err3)

	err4 := SetMapValueExistWithPrefix(data2, "newVal", "prefix", []string{"a", "hello"}...)
	assert.NoError(t, err4)
	exp4 := map[string]interface{}{
		"a": map[string]interface{}{
			"name":        "qiniu",
			"age":         45,
			"prefix_name": "newVal",
			"hello":       "newVal",
		},
		"xy": map[string]interface{}{
			"name": "newVal",
		},
	}
	assert.Equal(t, exp4, data2)
}

func TestCompressFile(t *testing.T) {
	assert.Equal(t, true, CompressedFile("abc.gz"))
	assert.Equal(t, true, CompressedFile("xxx.tar"))
	assert.Equal(t, true, CompressedFile("as.zip"))
	assert.Equal(t, true, CompressedFile("123.tar.gz"))
	assert.Equal(t, false, CompressedFile("abc.taxs"))
}

func TestIgnoreFileSuffixes(t *testing.T) {
	suffixes := []string{".swap", ".gz", ".tar"}
	assert.Equal(t, false, IgnoreFileSuffixes("abc.log", suffixes))
	assert.Equal(t, true, IgnoreFileSuffixes("abc.tar", suffixes))
	assert.Equal(t, true, IgnoreFileSuffixes("abc.swap", suffixes))
}

func TestValidFile(t *testing.T) {
	assert.Equal(t, true, ValidFileRegex("abc.log", "*.log"))
	assert.Equal(t, true, ValidFileRegex("abc.1", "abc.[1-9]"))
	assert.Equal(t, false, ValidFileRegex("abc.1", "abc.[1-9]1"))
	assert.Equal(t, false, ValidFileRegex("abc.swap", "abc.s[*"))
	assert.Equal(t, true, ValidFileRegex("abc.swap", "abc.s*"))
	assert.Equal(t, true, ValidFileRegex("abc.log", ""))
}

func TestIgnoreHidden(t *testing.T) {
	assert.Equal(t, true, IgnoreHidden(".log", true))
	assert.Equal(t, false, IgnoreHidden(".1", false))
}
