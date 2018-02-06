package reader

import (
	"fmt"
	"os"
	"testing"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

func Test_scriptFile(t *testing.T) {
	fileName := os.TempDir() + "/scriptFile.sh"

	//create file & write file
	createTestFile(fileName, "echo \"hello world\"")
	defer deleteTestFile(fileName)

	readerConf := conf.MapConf{
		KeyExecInterpreter: "bash",
		KeyLogPath:         fileName,
	}
	//create sf
	meta, err := NewMetaWithConf(readerConf)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewScriptReader(meta, readerConf)
	if err != nil {
		t.Error(err)
	}
	assert.NoError(t, err)

	data, err := sf.ReadLine()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "hello world\n", data)
}

func Test_checkFileMode(t *testing.T) {
	fileName := os.TempDir() + "/checkFileMode.sh"
	//create file & write file
	createTestFile(fileName, "echo \"hello world\"")
	defer deleteTestFile(fileName)
	err := os.Chmod(fileName, 0666)
	if err != nil {
		t.Error(err)
	}

	realPath, fileInfo, err := utils.GetRealPath(fileName)
	if err != nil {
		t.Error(err)
	}
	if fileInfo == nil {
		err = fmt.Errorf("fileInfo of fileName [%v] is nil", fileName)
		t.Error(err)
	}
	fileMode := fileInfo.Mode()
	assert.Equal(t, os.FileMode(0x1b6), fileMode)

	err = checkFileMode(realPath, fileMode)
	if err != nil {
		t.Error(err)
	}

	_, fileInfoNew, err := utils.GetRealPath(fileName)
	if err != nil {
		t.Error(err)
	}
	if fileInfo == nil {
		err = fmt.Errorf("fileInfo of fileName [%v] is nil", fileName)
		t.Error(err)
	}
	fileModeNew := fileInfoNew.Mode()
	assert.Equal(t, os.FileMode(0x1ff), fileModeNew)
}
