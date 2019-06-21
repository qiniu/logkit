package self

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/pandora-go-sdk/base/config"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/parser/raw"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/bufreader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/sender/file"
	"github.com/qiniu/logkit/sender/mock"
	. "github.com/qiniu/logkit/utils/models"
)

func TestLogRunner_Name(t *testing.T) {
	t.Parallel()
	rdConf, psConf, sdConf, meta, rd, ps, sd := getInfo(t, "", "")
	logRunner := NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, "")
	assert.EqualValues(t, DefaultSelfRunnerName, logRunner.Name())
}

func TestNewLogRunner(t *testing.T) {
	t.Parallel()
	rdConf, psConf, sdConf, meta, rd, ps, sd := getInfo(t, "", "")
	logRunner := NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, "")
	assert.NotNil(t, logRunner)
	assert.NotNil(t, logRunner.meta)
	assert.NotNil(t, logRunner.reader)
	assert.NotNil(t, logRunner.parser)
	assert.NotNil(t, logRunner.sender)
	assert.EqualValues(t, rdConf, logRunner.readerConfig)
	assert.EqualValues(t, parserConfig, logRunner.parserConfig)
	assert.EqualValues(t, sdConf, logRunner.senderConfig)
}

func TestLogRunner_Run(t *testing.T) {
	cur, err := os.Getwd()
	assert.Nil(t, err)
	dir := filepath.Join(cur, "TestLogRunner_Run")
	logpath := filepath.Join(dir, "logkit.log")
	sendpath := filepath.Join(dir, "self.txt")
	err = os.Mkdir(dir, DefaultDirPerm)
	assert.Nil(t, err)
	f, err := os.Create(sendpath)
	assert.Nil(t, err)
	f.Close()
	defer os.RemoveAll(dir)

	readerConfig["stat_interval"] = "1s"
	rdConf, psConf, _, meta, rd, ps, _ := getInfo(t, logpath, path.Join(dir, "meta"))
	readerConfig["stat_interval"] = ""
	sdConf := conf.MapConf{
		"name":           "TestLogRunner_Run",
		"sender_type":    "file",
		"file_send_path": sendpath,
	}
	sd, err := file.NewSender(sdConf)
	assert.Nil(t, err)

	expect1 := "2018/11/12 13:31:33 [ERROR][qiniu.com/logkit-enterprise/vendor/github.com/qiniu/logkit/mgr] metric_runner.go:350: SendError: Cannot send data to pandora\n"
	expect2 := "2018/11/13 09:38:51 [WARN][github.com/qiniu/logkit/reader] singlefile.go:85: Runner[] /Users/qiniu/gopath/src/github.com/qiniu/logkit/meta/a/qiniu_logkit_logkit.log-1113093840/file.meta\n"
	expect3 := "2018/11/13 09:38:51 [INFO][github.com/qiniu/logkit/reader/tailx] tailx.go:570: Runner[UndefinedRunnerName] statLogPath find new logpath"
	err = ioutil.WriteFile(logpath, []byte(expect1+
		"2018/11/12 13:31:33 [DEBUG][qiniu.com/logkit-enterprise/vendor/github.com/qiniu/logkit/mgr] metric_runner.go:350: SendError: Cannot send data to pandora\n"+
		expect2+
		"2018/11/13 09:38:41 [DEBUG][github.com/qiniu/logkit/reader/tailx] tailx.go:178: Runner[UndefinedRunnerName] logkit.log-1113093540 >>>>>>readcache <> linecache <>\n"+
		expect3), 0755)
	assert.Nil(t, err)

	logRunner := NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, "")
	go logRunner.Run()
	time.Sleep(5 * time.Second)
	logRunner.Stop()

	result, err := ioutil.ReadFile(sendpath)
	assert.Nil(t, err)

	type Result struct {
		Raw string `json:"raw"`
	}
	var actual []Result
	err = jsoniter.Unmarshal(result, &actual)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(actual))
	assert.EqualValues(t, expect1, actual[0].Raw)
	assert.EqualValues(t, expect2, actual[1].Raw)
	assert.EqualValues(t, expect3, actual[2].Raw)
}

func TestLogRunner_GetReaderConfig(t *testing.T) {
	t.Parallel()
	rdConf, psConf, sdConf, meta, rd, ps, sd := getInfo(t, "", "")
	logRunner := NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, "")
	assert.EqualValues(t, rdConf, logRunner.GetReaderConfig())
}

func TestLogRunner_GetParserConfig(t *testing.T) {
	t.Parallel()
	rdConf, psConf, sdConf, meta, rd, ps, sd := getInfo(t, "", "")
	logRunner := NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, "")
	assert.EqualValues(t, psConf, logRunner.GetParserConfig())
}

func TestLogRunner_GetSenderConfig(t *testing.T) {
	t.Parallel()
	rdConf, psConf, sdConf, meta, rd, ps, sd := getInfo(t, "", "")
	logRunner := NewLogRunnerWithService(rdConf, psConf, sdConf, meta, rd, ps, sd, "")
	assert.EqualValues(t, sdConf, logRunner.GetSenderConfig())
}

func TestSetReaderConfig(t *testing.T) {
	t.Parallel()
	path := "TestSetReaderConfig"
	filePattern := "logkit.log*"
	rdConf := SetReaderConfig(readerConfig, path, filePattern, "", "")
	if !strings.HasSuffix(path, rdConf["log_path"]) {
		t.Fatalf("expect has suffix %v, but got %v", rdConf["log_path"], path)
	}
	assert.EqualValues(t, WhenceNewest, rdConf["read_from"])
}

func TestSetSenderConfig(t *testing.T) {
	t.Parallel()
	pandora := Pandora{
		Name:     "TestSetSenderConfig_name",
		Region:   "TestSetSenderConfig_region",
		Pipeline: "TestSetSenderConfig_pipline",
		AK:       "TestSetSenderConfig_ak",
		SK:       "TestSetSenderConfig_sk",
	}
	sdConf := SetSenderConfig(senderConfig, pandora)
	assert.NotNil(t, sdConf)
	assert.EqualValues(t, pandora.Name, sdConf["pandora_repo_name"])
	assert.EqualValues(t, pandora.Region, sdConf["pandora_region"])
	assert.EqualValues(t, pandora.Pipeline, sdConf["pandora_host"])
	assert.EqualValues(t, pandora.AK, sdConf["pandora_ak"])
	assert.EqualValues(t, pandora.SK, sdConf["pandora_sk"])
	assert.EqualValues(t, config.DefaultLogDBEndpoint, sdConf["pandora_logdb_host"])
}

func TestGetReaderConfig(t *testing.T) {
	t.Parallel()
	assert.EqualValues(t, readerConfig, GetReaderConfig())
}

func TestGetParserConfig(t *testing.T) {
	t.Parallel()
	assert.EqualValues(t, parserConfig, GetParserConfig())
}

func TestGetSenderConfig(t *testing.T) {
	t.Parallel()
	assert.EqualValues(t, senderConfig, GetSenderConfig())
}

func getInfo(t *testing.T, logpath, metapath string) (conf.MapConf, conf.MapConf, conf.MapConf,
	*reader.Meta, reader.Reader, parser.Parser, sender.Sender) {
	var (
		rd  reader.Reader
		ps  parser.Parser
		sd  sender.Sender
		err error
	)

	var filePattern string
	if logpath == "" {
		logpath = "TestNewLogRunner/"
		filePattern = "logkit.log*"
	} else {
		filePattern = filepath.Base(logpath) + "*"
		logpath = filepath.Dir(logpath)
	}
	rdConf := SetReaderConfig(readerConfig, logpath, filePattern, metapath, "oldest")
	meta, err := reader.NewMetaWithConf(rdConf)
	assert.Nil(t, err)
	defer func() {
		if err != nil && rd != nil {
			rd.Close()
		}
	}()

	rd, err = bufreader.NewFileDirReader(meta, rdConf)
	assert.Nil(t, err)

	ps, err = raw.NewParser(nil)
	assert.Nil(t, err)

	sdConf := conf.MapConf{
		"name":        "mock_sender",
		"sender_type": "mock",
	}
	sd, err = mock.NewSender(sdConf)
	assert.Nil(t, err)
	return rdConf, parserConfig, sdConf, meta, rd, ps, sd
}
