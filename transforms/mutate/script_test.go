package mutate

import (
	"os"
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"strings"

	"github.com/stretchr/testify/assert"
)

func TestScriptTransformer(t *testing.T) {
	fileName := os.TempDir() + "/scriptFile.sh"
	//create file & write file
	content := `word=$1
				name=$2
if [[ ${word} == "" ]]; then
    word="hello"
fi
echo "say: ${word} ${name}"`
	createTestFile(fileName, content)
	defer os.RemoveAll(fileName)

	scriptConf := &Script{
		Key:         "key1",
		New:         "myscript",
		Interpreter: "bash",
	}
	scriptConf.Init()
	scriptConf.ScriptPath = fileName
	data := []Data{{"key1": "bye", "other": "other"}, {"key1": "nice", "key2": "qiniu"}}
	res, err := scriptConf.Transform(data)
	assert.NoError(t, err)
	exp := []Data{{"key1": "bye", "other": "other", "myscript": "say: bye \n"}, {"key1": "nice", "key2": "qiniu", "myscript": "say: nice \n"}}
	assert.Equal(t, exp, res)

	scriptConf2 := &Script{
		Key:         "key1, key2",
		New:         "myscript",
		Interpreter: "bash",
	}
	scriptConf2.Init()
	scriptConf2.ScriptPath = fileName
	data2 := []Data{{"key1": "", "key2": "qiniu"}, {"key1": "nice", "key2": "qiniu"}}
	res2, err2 := scriptConf2.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []Data{{"key1": "", "key2": "qiniu", "myscript": "say: hello qiniu\n"}, {"key1": "nice", "key2": "qiniu", "myscript": "say: nice qiniu\n"}}
	assert.Equal(t, exp2, res2)

	scriptConf3 := &Script{
		Key:         "key1..., key2...",
		New:         "myscript...",
		Interpreter: "bash",
	}
	scriptConf3.Init()
	scriptConf3.ScriptPath = fileName
	data3 := []Data{{"key1": "", "key2": "qiniu"}, {"key1": "nice", "key2": "qiniu"}}
	res3, err3 := scriptConf3.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []Data{{"key1": "", "key2": "qiniu", "myscript": "say: hello qiniu\n"}, {"key1": "nice", "key2": "qiniu", "myscript": "say: nice qiniu\n"}}
	assert.Equal(t, exp3, res3)

	scriptConf5 := &Script{
		Key:         "key1..., ..., ...",
		New:         "myscript...",
		Interpreter: "bash",
	}
	scriptConf5.Init()
	scriptConf5.ScriptPath = fileName
	data5 := []Data{{"key1": "", "key2": "qiniu"}, {"key1": "nice", "key2": "qiniu"}}
	res5, err5 := scriptConf5.Transform(data5)
	assert.NoError(t, err5)
	exp5 := []Data{{"key1": "", "key2": "qiniu", "myscript": "say: hello \n"}, {"key1": "nice", "key2": "qiniu", "myscript": "say: nice \n"}}
	assert.Equal(t, exp5, res5)

	os.RemoveAll(fileName)
	fileName = os.TempDir() + "/scriptFile.sh"
	//create file & write file
	content2 := `bash /a/b/c.sh`
	createTestFile(fileName, content2)

	scriptConf4 := &Script{
		Key:         "key1, key2",
		New:         "myscript",
		Interpreter: "bash",
	}
	scriptConf4.Init()
	scriptConf4.ScriptPath = fileName
	data4 := []Data{{"key1": "", "key2": "qiniu"}, {"key1": "nice", "key2": "qiniu"}}
	res4, err4 := scriptConf4.Transform(data4)
	assert.NoError(t, err4)
	exp4 := []Data{{"key1": "", "key2": "qiniu", "myscript": "say: hello qiniu\n"}, {"key1": "nice", "key2": "qiniu", "myscript": "say: nice qiniu\n"}}
	assert.Equal(t, len(exp4), len(res4))
	for _, resVal := range res4 {
		if val, ok := resVal["myscript_error"]; ok {
			if !strings.Contains(val.(string), "run script err info is: exit status 127") {
				t.Errorf("expect get myscript_error, but got: %v", resVal)
			}
		} else {
			t.Errorf("expect key: myscript_error, but got: %v", resVal)
		}
	}

	scriptConf7 := &Script{
		Key:         "myscript...",
		New:         "res",
		Interpreter: "bash",
		ScriptPath:  fileName,
		Script:      EncodeString(`echo "hello"`),
	}
	scriptConf7.Init()
	data7 := []Data{{"key1": "value1", "myscript": "fileName"}}
	res7, err7 := scriptConf7.Transform(data7)
	assert.NoError(t, err7)
	exp7 := []Data{{"key1": "value1", "myscript": "fileName", "res": "hello\n"}}
	assert.Equal(t, exp7, res7)
	os.RemoveAll("transformer_scripts")
}

func createTestFile(fileName string, content string) {
	f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString(content)
	f.Sync()
	f.Close()
}
