package reader

import (
	"os"
	"testing"

	"github.com/qiniu/logkit/conf"

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
