package reader

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

//测试single file rotate的情况
func Test_singleFileRotate(t *testing.T) {
	fileName := filepath.Join(os.TempDir(), "test.singleFile")
	fileNameRotated := filepath.Join(os.TempDir(), "test.singleFile.rotated")
	metaDir := filepath.Join(os.TempDir(), "rotates")

	//create file & write file
	createTestFile(fileName, "12345")

	//create sf
	meta, err := NewMeta(metaDir, metaDir, testlogpath, ModeFile, defautFileRetention)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewSingleFile(meta, fileName, WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	absPath, err := filepath.Abs(fileName)
	assert.NoError(t, err)
	assert.Equal(t, absPath, sf.Source())
	oldInode := utils.GetInode(sf.pfi)

	//rotate file(rename old file + create new file)
	renameTestFile(fileName, fileNameRotated)

	createTestFile(fileName, "67890")
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

	newInode := utils.GetInode(sf.pfi)
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
	createTestFile(fileName, "12345")
	defer deleteTestFile(fileName)

	//create sf
	meta, err := NewMeta(metaDir, metaDir, testlogpath, ModeFile, defautFileRetention)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewSingleFile(meta, fileName, WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	oldInode := utils.GetInode(sf.pfi)

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

	newInode := utils.GetInode(sf.pfi)
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

func createTestFile(fileName string, content string) {

	f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, defaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}

func appendTestFile(fileName, content string) {
	f, _ := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, defaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}

func renameTestFile(from, to string) {
	os.Rename(from, to)
}

func deleteTestFile(fileName string) {
	os.RemoveAll(fileName)
}
