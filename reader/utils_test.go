package reader

import (
	"errors"
	"os"
	"regexp"
	"testing"

	"time"

	"io/ioutil"
	"path/filepath"

	"github.com/stretchr/testify/assert"
)

func TestFindFile(t *testing.T) {
	createFile(1000)
	defer destroyFile()

	fi, err := getLatestFile(dir)
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != "f1" {
		t.Errorf("Latest file is f4, not %v", fi.Name())
	}

	fi, err = getOldestFile(dir)
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != "f3" {
		t.Errorf("Oldest file is f1, not %v", fi.Name())
	}

}

func TestCondition(t *testing.T) {
	var fi os.FileInfo
	trueCondition := noCondition
	falseCondition := notCondition(trueCondition)

	assert.True(t, trueCondition(fi))
	assert.False(t, falseCondition(fi))
	assert.True(t, orCondition(trueCondition, falseCondition)(fi))
	assert.False(t, andCondition(trueCondition, falseCondition)(fi))
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
	dur, err := parseLoopDuration("loop 1s")
	assert.NoError(t, err)
	assert.Equal(t, time.Second, dur)

	dur, err = parseLoopDuration("loop 1-")
	assert.Error(t, err)
	assert.Equal(t, time.Duration(0), dur)
}

func TestModTimeLater(t *testing.T) {
	dir := "TestModTimeLater"
	err := os.Mkdir(dir, 0755)
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	for _, v := range []string{"f1", "f2", "f3"} {
		err := ioutil.WriteFile(filepath.Join(dir, v), []byte("abc"), 0644)
		assert.NoError(t, err)
	}
	cs, err := getMaxFile(dir, func(info os.FileInfo) bool { return true }, modTimeLater)
	assert.NoError(t, err)
	assert.Equal(t, "f3", cs.Name())
	cs, err = getMinFile(dir, func(info os.FileInfo) bool { return true }, modTimeLater)
	assert.NoError(t, err)
	assert.Equal(t, "f1", cs.Name())
}
