package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/utils/models"
)

func TestDeepCopyByJSON(t *testing.T) {
	tests := []struct {
		src    map[string]interface{}
		dst    map[string]interface{}
		expect map[string]interface{}
	}{
		{
			src: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
			expect: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
		},
		{
			src:    nil,
			expect: nil,
		},
		{
			src: map[string]interface{}{
				"a": map[string]interface{}{"b": []interface{}{"c", "d", "e"}},
			},
			expect: map[string]interface{}{
				"a": map[string]interface{}{"b": []interface{}{"c", "d", "e"}},
			},
		},
	}

	for _, test := range tests {
		DeepCopyByJSON(&test.dst, &test.src)
		assert.Equal(t, len(test.expect), len(test.dst))
		for key, value := range test.expect {
			assert.Equal(t, value, test.dst[key])
		}
	}
}

func TestCheckNotExistFile(t *testing.T) {
	dir := "TestUpdateExpireMap"
	createDirWithName(dir)
	defer os.RemoveAll(dir)

	files := []string{filepath.Join(dir, "file1"), filepath.Join(dir, "file2"), filepath.Join(dir, "file3")}
	for _, fileEach := range files {
		createFileWithContent(fileEach, "abcd\n")
	}
	createDirWithName(filepath.Join(dir, "test1"))

	fileMap, err := GetFiles("", dir)
	assert.Nil(t, err)
	assert.EqualValues(t, len(files), len(fileMap))

	expireMap := make(map[string]int64)
	UpdateExpireMap("", fileMap, expireMap)
	expect1 := len(expireMap)

	CheckNotExistFile("", expireMap)
	assert.EqualValues(t, expect1, len(expireMap))

	os.RemoveAll(filepath.Join(dir, "file1"))
	CheckNotExistFile("", expireMap)
	assert.EqualValues(t, expect1-1, len(expireMap))
}

func TestUpdateExpireMap(t *testing.T) {
	dir := "TestUpdateExpireMap"
	createDirWithName(dir)
	defer os.RemoveAll(dir)

	files := []string{filepath.Join(dir, "file1"), filepath.Join(dir, "file2"), filepath.Join(dir, "file3")}
	for _, fileEach := range files {
		createFileWithContent(fileEach, "abcd\n")
	}
	createDirWithName(filepath.Join(dir, "test1"))

	fileMap, err := GetFiles("", dir)
	assert.Nil(t, err)
	assert.EqualValues(t, len(files), len(fileMap))

	expireMap := make(map[string]int64)
	UpdateExpireMap("", fileMap, expireMap)
	assert.EqualValues(t, len(files), len(expireMap))
	for file, inode := range fileMap {
		offset, ok := expireMap[inode+"_"+file]
		assert.True(t, ok)
		assert.EqualValues(t, 5, offset)
	}
}

func TestGetFiles(t *testing.T) {
	dir := "TestGetFiles"
	createDirWithName(dir)
	defer os.RemoveAll(dir)

	files := []string{filepath.Join(dir, "file1"), filepath.Join(dir, "file2"), filepath.Join(dir, "file3")}
	for _, fileEach := range files {
		createFileWithContent(fileEach, "abcd\n")
	}
	createDirWithName(filepath.Join(dir, "test1"))

	fileMap, err := GetFiles("", dir)
	assert.Nil(t, err)
	assert.EqualValues(t, 3, len(fileMap))
	for _, fileEach := range files {
		inode, ok := fileMap[fileEach]
		assert.True(t, ok)
		assert.True(t, len(inode) > 0)
	}
}

func createDirWithName(dirx string) {
	err := os.Mkdir(dirx, DefaultDirPerm)
	if err != nil {
		log.Error(err)
		return
	}
}

func createFileWithContent(filepathn, lines string) {
	file, err := os.OpenFile(filepathn, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	if err != nil {
		log.Error(err)
		return
	}
	file.WriteString(lines)
	file.Close()
}
