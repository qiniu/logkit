package singlefile

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

const testlogpath = "logpath"

//测试single file rotate的情况
func Test_singleFileRotate(t *testing.T) {
	fileName := filepath.Join(os.TempDir(), "test.singleFile")
	fileNameRotated := filepath.Join(os.TempDir(), "test.singleFile.rotated")
	metaDir := filepath.Join(os.TempDir(), "rotates")

	//create file & write file
	CreateFile(fileName, "12345")

	//create sf
	meta, err := reader.NewMeta(metaDir, metaDir, testlogpath, ModeFile, "", reader.DefautFileRetention)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewSingleFile(meta, fileName, WhenceOldest, 0, false)
	if err != nil {
		t.Error(err)
	}
	absPath, err := filepath.Abs(fileName)
	assert.NoError(t, err)
	assert.Equal(t, absPath, sf.Source())
	oldInode, err := utilsos.GetIdentifyIDByPath(absPath)
	assert.NoError(t, err)

	//rotate file(rename old file + create new file)
	renameTestFile(fileName, fileNameRotated)

	CreateFile(fileName, "67890")
	//read file 正常读
	p := make([]byte, 5)
	n, err := sf.Read(p)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, n)
	assert.Equal(t, "12345", string(p))

	//应该遇到EOF,pfi被更新
	n, err = sf.Read(p)
	if err != nil {
		t.Error(err)
	}

	newInode, err := utilsos.GetIdentifyIDByPath(fileName)
	assert.NoError(t, err)
	assert.NotEqual(t, newInode, oldInode)

	assert.Equal(t, 5, n)
	assert.Equal(t, "67890", string(p))
	filedone, err := ioutil.ReadFile(sf.meta.DoneFile())
	assert.NoError(t, err)
	assert.True(t, strings.Contains(string(filedone), fileNameRotated))
}

//测试single file不rotate的情况
func Test_singleFileNotRotate(t *testing.T) {
	fileName := os.TempDir() + "/test.singleFile"
	metaDir := os.TempDir() + "/rotates"

	//create file & write file
	CreateFile(fileName, "12345")
	defer DeleteFile(fileName)

	//create sf
	meta, err := reader.NewMeta(metaDir, metaDir, testlogpath, ModeFile, "", reader.DefautFileRetention)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewSingleFile(meta, fileName, WhenceOldest, 0, false)
	if err != nil {
		t.Error(err)
	}
	oldInode, err := utilsos.GetIdentifyIDByFile(sf.f)
	assert.NoError(t, err)

	//read file 正常读
	p := make([]byte, 5)
	n, err := sf.Read(p)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, n)
	assert.Equal(t, "12345", string(p))

	//应该遇到EOF,pfi没有被更新
	n, err = sf.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.EqualValues(t, 0, n)

	newInode, err := utilsos.GetIdentifyIDByFile(sf.f)
	assert.NoError(t, err)
	assert.Equal(t, newInode, oldInode)

	//append文件
	appendTestFile(fileName, "67890")
	n, err = sf.Read(p)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, n)
	assert.Equal(t, "67890", string(p))
}

//测试single file有offset的情况
func Test_singleFileOffset(t *testing.T) {
	fileName := os.TempDir() + "/test.singleFile"
	metaDir := os.TempDir() + "/rotates"

	//create file & write file
	CreateFile(fileName, "12345")
	defer DeleteFile(fileName)

	//create sf
	meta, err := reader.NewMeta(metaDir, metaDir, testlogpath, ModeFile, "", reader.DefautFileRetention)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewSingleFile(meta, fileName, WhenceOldest, 5, false)
	if err != nil {
		t.Error(err)
	}
	oldInode, err := utilsos.GetIdentifyIDByFile(sf.f)
	assert.Nil(t, err)
	assert.NotEmpty(t, oldInode)

	//read file 正常读
	p := make([]byte, 5)
	n, err := sf.Read(p)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)

	newInode, err := utilsos.GetIdentifyIDByFile(sf.f)
	assert.NoError(t, err)
	assert.Equal(t, newInode, oldInode)

	//append文件，从 offset 5 开始读
	appendTestFile(fileName, "12345")
	n, err = sf.Read(p)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, n)
	assert.Equal(t, "12345", string(p))

	//append文件，从 offset 10 开始读
	appendTestFile(fileName, "67890")
	n, err = sf.Read(p)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, n)
	assert.Equal(t, "67890", string(p))

	n, err = sf.Read(p)
	assert.Equal(t, io.EOF, err)
}

func appendTestFile(fileName, content string) {
	f, _ := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, DefaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}

func renameTestFile(from, to string) {
	os.Rename(from, to)
}
