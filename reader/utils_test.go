package reader

import (
	"os"
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
