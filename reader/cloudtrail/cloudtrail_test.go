package cloudtrail

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

func TestGetDefaultSyncDir(t *testing.T) {
	bucket := "bucket1"
	prefix := "prefix1"
	region := "region1"
	ak := "ak1"
	sk := "sk1"
	runnerName := "runner1"
	createDirWithName("s3data")
	origDir := filepath.Join("s3data", "data"+Hash(ak+sk+region+bucket+prefix+runnerName))
	createDirWithName(origDir)
	_, newDir := GetDefaultSyncDir(bucket, prefix, region, ak, sk, runnerName)
	os.RemoveAll(newDir)
	isOrigDirExist := utils.IsExist(origDir)
	assert.Equal(t, false, isOrigDirExist)
}

func TestGetDefaultMetaStore(t *testing.T) {
	bucket := "bucket1"
	prefix := "prefix1"
	region := "region1"
	ak := "ak1"
	sk := "sk1"
	runnerName := "runner1"
	origMeta := ".metastore" + Hash(ak+sk+region+bucket+prefix+runnerName)
	createFileWithContent(origMeta, "test")
	newMeta := GetDefaultMetaStore(bucket, prefix, region, ak, sk, runnerName)
	os.RemoveAll(newMeta)
	isOrigMetaExist := utils.IsExist(origMeta)
	assert.Equal(t, false, isOrigMetaExist)
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
