package script

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
)

func Test_scriptFile(t *testing.T) {
	fileName := os.TempDir() + "/scriptFile.sh"

	//create file & write file
	CreateFile(fileName, "echo \"hello world\"")
	defer DeleteFile(fileName)

	readerConf := conf.MapConf{
		reader.KeyExecInterpreter: "bash",
		reader.KeyLogPath:         fileName,
	}
	//create sf
	meta, err := reader.NewMetaWithConf(readerConf)
	if err != nil {
		t.Error(err)
	}

	sf, err := NewReader(meta, readerConf)
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
