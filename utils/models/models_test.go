package models

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
)

func Test_KeyValueSlice(t *testing.T) {
	testData := struct {
		origin KeyValueSlice
		expect KeyValueSlice
	}{
		origin: KeyValueSlice{
			{
				"test_start1",
				"",
				"a",
			},
			{
				"test_start2",
				"",
				"a",
			},
			{
				"kafka",
				"",
				"kafa",
			},
			{
				"test_final1",
				"",
				"",
			},
			{
				"test_final2",
				"",
				"",
			},
			{
				"kafkaNew",
				"",
				"kafaNew",
			},
		},
		expect: KeyValueSlice{
			{
				"test_final1",
				"",
				"",
			},
			{
				"test_final2",
				"",
				"",
			},
			{
				"test_start1",
				"",
				"a",
			},
			{
				"test_start2",
				"",
				"a",
			},
			{
				"kafka",
				"",
				"kafa",
			},
			{
				"kafkaNew",
				"",
				"kafaNew",
			},
		},
	}
	sort.Stable(testData.origin)
	assert.Equal(t, testData.expect, testData.origin)
}

func TestIsFileModified(t *testing.T) {
	dirname := "TestIsFileModified"
	createDirWithName(dirname)
	defer os.RemoveAll(dirname)
	filename := filepath.Join(dirname, "file1.log")
	createFileWithContent(filename, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	actual := IsFileModified(filename, 3*time.Minute, time.Now())
	assert.True(t, actual)

	err := os.Chtimes(filename, time.Now().Add(-10*time.Minute), time.Now().Add(-10*time.Minute))
	assert.Nil(t, err)
	actual = IsFileModified(filename, 3*time.Minute, time.Now())
	assert.False(t, actual)
}

func createFileWithContent(filePath, content string) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	if err != nil {
		log.Error(err)
		return
	}
	file.WriteString(content)
	file.Close()
}

func createDirWithName(dir string) {
	err := os.Mkdir(dir, DefaultDirPerm)
	if err != nil {
		log.Error(err)
		return
	}
}
