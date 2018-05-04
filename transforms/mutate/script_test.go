package mutate

import (
	"os"
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestScriptTransformer(t *testing.T) {
	scriptConf := &Script{
		Key:          "myscript",
		New:          "myscript",
		Interprepter: "bash",
	}
	scriptConf.Init()
	fileName := os.TempDir() + "/scriptFile.sh"
	//create file & write file
	createTestFile(fileName, "echo \"Hello World!\"")
	defer os.RemoveAll(fileName)

	data := []Data{{"key1": "value1", "myscript": fileName}, {"key2": "value2", "myscript": fileName}}
	res, err := scriptConf.Transform(data)
	assert.NoError(t, err)
	exp := []Data{{"key1": "value1", "myscript": "Hello World!\n"}, {"key2": "value2", "myscript": "Hello World!\n"}}
	assert.Equal(t, exp, res)

	scriptConf2 := &Script{
		Key:          "myscript",
		Interprepter: "bash",
	}
	scriptConf2.Init()
	data2 := []Data{{"key1": "value1", "myscript": fileName}}
	res2, err2 := scriptConf2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []Data{{"key1": "value1", "myscript": "Hello World!\n"}}
	assert.Equal(t, exp2, res2)

	scriptConf3 := &Script{
		Key:          "myscript",
		New:          "newKey",
		Interprepter: "bash",
	}
	scriptConf3.Init()
	data3 := []Data{{"key1": "value1", "myscript": fileName}, {"key2": "value2", "myscript": fileName}}
	res3, err3 := scriptConf3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []Data{{"key1": "value1", "myscript": fileName, "newKey": "Hello World!\n"}, {"key2": "value2", "myscript": fileName, "newKey": "Hello World!\n"}}
	assert.Equal(t, exp3, res3)

	scriptConf4 := &Script{
		Key:          "myscript...",
		New:          "newKey...",
		Interprepter: "bash",
	}
	scriptConf4.Init()
	data4 := []Data{{"key1": "value1", "myscript": fileName}}
	res4, err4 := scriptConf4.Transform(data4)
	assert.NoError(t, err4)
	exp4 := []Data{{"key1": "value1", "myscript": fileName, "newKey": "Hello World!\n"}}
	assert.Equal(t, exp4, res4)

	scriptConf5 := &Script{
		Key:          "myscript...",
		Interprepter: "bash",
		ScriptPath:   fileName,
	}
	scriptConf5.Init()
	data5 := []Data{{"key1": "value1", "myscript": "fileName"}}
	res5, err5 := scriptConf5.Transform(data5)
	assert.NoError(t, err5)
	exp5 := []Data{{"key1": "value1", "myscript": "Hello World!\n"}}
	assert.Equal(t, exp5, res5)

	scriptConf6 := &Script{
		Key:          "myscript...",
		Interprepter: "bash",
		Script:       []byte("echo \"hello\""),
	}
	scriptConf6.Init()
	data6 := []Data{{"key1": "value1", "myscript": fileName}}
	res6, err6 := scriptConf6.Transform(data6)
	assert.NoError(t, err6)
	exp6 := []Data{{"key1": "value1", "myscript": "hello\n"}}
	assert.Equal(t, exp6, res6)

	scriptConf7 := &Script{
		Key:          "myscript...",
		Interprepter: "bash",
		ScriptPath:   fileName,
		Script:       []byte("echo hello"),
	}
	scriptConf7.Init()
	data7 := []Data{{"key1": "value1", "myscript": "fileName"}}
	res7, err7 := scriptConf7.Transform(data7)
	assert.NoError(t, err7)
	exp7 := []Data{{"key1": "value1", "myscript": "hello\n"}}
	assert.Equal(t, exp7, res7)
	os.RemoveAll("transformer_scripts")
}

func createTestFile(fileName string, content string) {
	f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}
