package reader

import (
	"errors"
	"os"
	"regexp"
	"testing"

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
