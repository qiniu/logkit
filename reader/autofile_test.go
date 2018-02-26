package reader

import (
	"testing"

	"io/ioutil"
	"os"
	"path/filepath"

	. "github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
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
			expPath: "/usr/*",
			expMode: ModeTailx,
		},
		{
			input:   "/usr/",
			expPath: "/usr/*",
			expMode: ModeTailx,
		},
		{
			input:   "/usr/local",
			expPath: "/usr/local/*",
			expMode: ModeTailx,
		},
		{
			input:   "/usr/local/",
			expPath: "/usr/local/*",
			expMode: ModeTailx,
		},
		{
			input:   filePath,
			expPath: filePath,
			expMode: ModeFile,
		},
		{
			input:   filepath.Join(rootDir, "123"),
			expPath: filepath.Join(rootDir, "123"),
			expMode: "",
		},
	}
	for _, val := range testData {
		path, mode, err := matchMode(val.input)
		if val.expMode == "" {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		assert.Equal(t, val.expPath, path)
		assert.Equal(t, val.expMode, mode)
	}
}
