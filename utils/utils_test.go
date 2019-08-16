package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
				"d": int64(1234567),
				"e": float64(1234567.89),
				"f": json.Number("123"),
			},
			expect: map[string]interface{}{
				"a": "b",
				"c": "d",
				"d": json.Number("1234567"),
				"e": json.Number("1234567.89"),
				"f": json.Number("123"),
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
			assert.EqualValues(t, reflect.TypeOf(value).String(), reflect.TypeOf(test.dst[key]).String())
		}
	}
}

func TestDeepCopyByGob(t *testing.T) {
	tests := []struct {
		src    []Data
		dst    []Data
		expect []Data
	}{
		{
			src: []Data{
				{
					"a": "b",
					"c": "d",
					"d": int64(1234567),
				},
			},
			expect: []Data{
				{
					"a": "b",
					"c": "d",
					"d": int64(1234567),
				},
			},
		},
		{
			src:    nil,
			expect: nil,
		},
	}

	for _, test := range tests {
		DeepCopyByGob(&test.dst, &test.src)
		assert.Equal(t, len(test.expect), len(test.dst))
		for idx, m := range test.expect {
			for key, value := range m {
				assert.Equal(t, value, test.dst[idx][key])
				assert.EqualValues(t, reflect.TypeOf(value).String(), reflect.TypeOf(test.dst[idx][key]).String())
			}
		}
	}
}

// Benchmark_DeepCopyByGob-4   	   20000	     50866 ns/op	   12614 B/op	     302 allocs/op
func Benchmark_DeepCopyByGob(b *testing.B) {
	src := []Data{
		{
			"a": map[string]interface{}{
				"a1": "b2",
				"a2": float64(1234567.89),
				"a3": int64(123456789),
			},
			"c": float64(1234567.89),
			"d": int64(123456789),
		},
	}
	dst := []Data{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		DeepCopyByGob(&dst, &src)
	}
}

// Benchmark_DeepCopyByJSON-4   	  300000	      3623 ns/op	     988 B/op	      34 allocs/op
func Benchmark_DeepCopyByJSON(b *testing.B) {
	src := []Data{
		{
			"a": map[string]interface{}{
				"a1": "b2",
				"a2": float64(1234567.89),
				"a3": int64(123456789),
			},
			"c": float64(1234567.89),
			"d": int64(123456789),
		},
	}
	dst := []Data{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		DeepCopyByJSON(&dst, &src)
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

func TestCheckErr(t *testing.T) {
	tests := []struct {
		err    error
		expect error
	}{
		{
			err:    errors.New("test error 1"),
			expect: fmt.Errorf("1 parse line errors occurred, error test error 1"),
		},
		{
			err: &StatsError{
				StatsInfo: StatsInfo{
					Errors:    2,
					LastError: "last error 1",
				},
				DatasourceSkipIndex: []int{1, 2, 3},
			},
			expect: fmt.Errorf("2 parse line errors occurred, error last error 1"),
		},
		{
			err: &StatsError{
				StatsInfo:           StatsInfo{},
				DatasourceSkipIndex: []int{1, 2, 3},
			},
			expect: nil,
		},
	}

	for _, test := range tests {
		actual := CheckErr(test.err)
		assert.EqualValues(t, test.expect, actual)
	}
}

func createDirWithName(dirx string) {
	if err := os.Mkdir(dirx, DefaultDirPerm); err != nil {
		log.Error(err)
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
