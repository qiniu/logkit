package autofile

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

func TestMatchMode(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	fileName := "test_file"
	dirName := "TestMatchMode"
	rootDir := filepath.Join(pwd, dirName)
	filePath := filepath.Join(rootDir, fileName)
	defer os.RemoveAll(rootDir)
	if err := os.Mkdir(rootDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error %v", rootDir, err)
	}
	if err := ioutil.WriteFile(filePath, []byte("1234567890"), 0666); err != nil {
		t.Fatalf("write test file error %v", err)
	}
	testData := []struct {
		input   string
		expPath string
		expMode string
	}{
		{
			input:   "/usr",
			expPath: "/usr",
			expMode: reader.ModeDir,
		},
		{
			input:   "/usr/",
			expPath: "/usr",
			expMode: reader.ModeDir,
		},
		{
			input:   "/usr/local",
			expPath: "/usr/local",
			expMode: reader.ModeDir,
		},
		{
			input:   "/usr/local/",
			expPath: "/usr/local",
			expMode: reader.ModeDir,
		},
		{
			input:   filePath,
			expPath: filePath,
			expMode: reader.ModeFile,
		},
		{
			input:   rootDir,
			expPath: filepath.Join(rootDir, "*"),
			expMode: reader.ModeTailx,
		},
		{
			input:   filepath.Join(rootDir, "123"),
			expPath: filepath.Join(rootDir, "123"),
			expMode: "",
		},
	}
	for idx, val := range testData {
		path, mode, err := matchMode(val.input)
		if val.expMode == "" {
			assert.Error(t, err, idx)
		} else {
			assert.NoError(t, err, idx)
		}
		assert.Equal(t, val.expPath, path)
		assert.Equal(t, val.expMode, mode)
	}
}

func TestMatchModeDir(t *testing.T) {
	pwd, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	fileName := "test_file"
	dirName := "TestMatchMode"
	rootDir := filepath.Join(pwd, dirName)
	filePath := filepath.Join(rootDir, fileName)
	defer os.RemoveAll(rootDir)
	if err := os.Mkdir(rootDir, DefaultDirPerm); err != nil {
		t.Fatalf("mkdir %v error %v", rootDir, err)
	}
	if err := ioutil.WriteFile(filePath, []byte("1234567890"), 0666); err != nil {
		t.Fatalf("write test file error %v", err)
	}

	// 修改时间
	modifiedTime := time.Now().Local().Add(-48 * time.Hour)
	err = os.Chtimes(filePath, modifiedTime, modifiedTime)
	assert.NoError(t, err)

	path, mode, err := matchMode(rootDir)
	assert.NoError(t, err)
	assert.Equal(t, rootDir, path)
	assert.Equal(t, reader.ModeDir, mode)
}
