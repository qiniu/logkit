package script

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
)

func Test_scriptFile(t *testing.T) {
	fileName := filepath.Join(os.TempDir(), "scriptFile.sh")

	//create file & write file
	CreateFile(fileName, "echo \"hello world\"")
	defer DeleteFile(fileName)

	readerConf := conf.MapConf{
		KeyExecInterpreter: "bash",
		KeyLogPath:         fileName,
	}
	meta, err := reader.NewMetaWithConf(readerConf)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./meta")

	r, err := NewReader(meta, readerConf)
	if err != nil {
		t.Error(err)
	}
	assert.NoError(t, err)
	sr := r.(*Reader)
	assert.NoError(t, sr.Start())
	defer sr.Close()

	data, err := r.ReadLine()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "hello world\n", data)

	readerConf = conf.MapConf{
		KeyExecInterpreter: "who",
		KeyMode:            ModeScript,
	}
	meta, err = reader.NewMetaWithConf(readerConf)
	assert.Nil(t, err)
	assert.NotNil(t, meta)

	r, err = NewReader(meta, readerConf)
	assert.Nil(t, err)
	assert.NoError(t, err)
	sr = r.(*Reader)
	assert.Nil(t, sr.Start())
	defer sr.Close()

	data, err = r.ReadLine()
	assert.Nil(t, err)
	assert.NotNil(t, data)
	t.Log("data: ", data)
}

func TestCmdRunWithTimeout(t *testing.T) {
	cmdResult, isTimeout := CmdRunWithTimeout("echo", "hello")
	assert.Nil(t, cmdResult.err)
	assert.False(t, isTimeout)
	assert.EqualValues(t, "hello\n", string(cmdResult.content))

	cmdResult, isTimeout = CmdRunWithTimeout("test")
	assert.NotNil(t, cmdResult.err)
	assert.False(t, isTimeout)

	cmdResult, _ = CmdRunWithTimeout("top")
	assert.NotNil(t, cmdResult.err)
}
