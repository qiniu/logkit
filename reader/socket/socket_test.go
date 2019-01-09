package socket

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"log/syslog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/axgle/mahonia"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestUdpSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestUdpSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "udp://127.0.0.1:5140",
		KeyEncoding:             "gbk",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)

	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("udp", "127.0.0.1:5140",
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}
	decoder := mahonia.NewEncoder("gbk")
	encodingConent := decoder.ConvertString("And this is a daemon emergency with demotag. 中文")
	err = sysLog.Emerg(encodingConent)
	assert.NoError(t, err)
	err = sysLog.Emerg("this is OK")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is a daemon emergency with demotag. 中文")
	assert.Contains(t, sr.Source(), "127.0.0.1")
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")
	assert.Contains(t, sr.Source(), "127.0.0.1")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestUdpSocketReaderWithSplit(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestUdpSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "udp://127.0.0.1:5140",
		KeySocketSplitByLine:    "true",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)

	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("udp", "127.0.0.1:5140",
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}
	err = sysLog.Emerg("And this is a daemon emergency with demotag.\n \nthis is OK")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is a daemon emergency with demotag.")
	assert.Contains(t, sr.Source(), "127.0.0.1")
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")
	assert.Contains(t, sr.Source(), "127.0.0.1")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestTCPSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestTCPSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "tcp://127.0.0.1:5141",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("tcp", "127.0.0.1:5141",
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}
	err = sysLog.Emerg("And this is a daemon emergency with demotag.")
	assert.NoError(t, err)
	err = sysLog.Emerg("this is OK")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is a daemon emergency with demotag.")
	assert.Contains(t, sr.Source(), "127.0.0.1")
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")
	assert.Contains(t, sr.Source(), "127.0.0.1")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestTCPSocketReaderWithSplit(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestTCPSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "tcp://127.0.0.1:5141",
		KeySocketSplitByLine:    "true",
		KeyEncoding:             "GBK",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("tcp", "127.0.0.1:5141",
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}

	decoder := mahonia.NewEncoder("gbk")
	encodingConent := decoder.ConvertString("And this is a daemon emergency with demotag 中 文.\n \nthis is OK 中 文")
	err = sysLog.Emerg(encodingConent)
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is a daemon emergency with demotag 中 文.")
	assert.Contains(t, sr.Source(), "127.0.0.1")
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK 中 文")
	assert.Contains(t, sr.Source(), "127.0.0.1")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestTCPSocketReaderWithJson(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestTCPSocketReaderWithJson",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "tcp://127.0.0.1:5141",
		KeySocketRule:           SocketRuleJson,
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	conn, err := net.Dial("tcp", "127.0.0.1:5141")
	if err != nil {
		log.Fatal(err)
	}
	u := map[string]interface{}{
		"mykey1": map[string]interface{}{
			"mykey1_test": map[string]string{
				"mykey2_test": "And this is a daemon emergency with demotag",
			},
		},
	}
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(u)
	fmt.Fprint(conn, b)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	expected := "{\"mykey1\":{\"mykey1_test\":{\"mykey2_test\":\"And this is a daemon emergency with demotag\"}}}"
	assert.Equal(t, expected, line)
	assert.Contains(t, sr.Source(), "127.0.0.1")
	u = map[string]interface{}{"mykey3": "this is OK"}
	b = new(bytes.Buffer)
	json.NewEncoder(b).Encode(u)
	fmt.Fprint(conn, b)
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Equal(t, "{\"mykey3\":\"this is OK\"}", line)
	assert.Contains(t, sr.Source(), "127.0.0.1")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestUnixSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestUnixSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "unix://./TestUnixSocketReader/log.socket",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	err = os.Mkdir("TestUnixSocketReader", DefaultDirPerm)
	assert.NoError(t, err)
	defer os.RemoveAll("TestUnixSocketReader")

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	expectSource := "./TestUnixSocketReader/log.socket"
	sysLog, err := syslog.Dial("unix", expectSource,
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}
	err = sysLog.Emerg("And this is unix socket test.")
	assert.NoError(t, err)
	err = sysLog.Emerg("this is OK")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is unix socket test.")
	//assert.Equal(t, expectSource, sr.Source()) // ci 拿到的是"@"，本地能通过
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")
	//assert.Equal(t, expectSource, sr.Source()) // ci 拿到的是"@"，本地能通过

	err = sr.Close()
	assert.NoError(t, err)
}

func TestUnixGramSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestUnixGramSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "unixgram://./TestUnixGramSocketReader/log.socket",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	err = os.Mkdir("TestUnixGramSocketReader", DefaultDirPerm)
	assert.NoError(t, err)
	defer os.RemoveAll("TestUnixGramSocketReader")

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	expectSource := "./TestUnixGramSocketReader/log.socket"
	sysLog, err := syslog.Dial("unixgram", expectSource,
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}
	err = sysLog.Emerg("And this is unix gram socket test.")
	assert.NoError(t, err)
	err = sysLog.Emerg("this is OK")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is unix gram socket test.")
	//assert.Equal(t, expectSource, sr.Source()) // ci 拿到的是"@"，本地能通过
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")
	//assert.Equal(t, expectSource, sr.Source()) // ci 拿到的是"@"，本地能通过

	err = sr.Close()
	assert.NoError(t, err)
}

func TestSocketReaderClosePanic(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             MetaDir,
		KeyFileDone:             MetaDir,
		KeyRunnerName:           "TestSocketReaderClosePanic",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "tcp://:5141",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("tcp", "127.0.0.1:5141",
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	assert.NoError(t, err)

	err = sysLog.Emerg("And this is a daemon emergency with demotag.")
	assert.NoError(t, err)
	err = sysLog.Emerg("this is OK")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err := sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "And this is a daemon emergency with demotag.")
	assert.Contains(t, sr.Source(), "127.0.0.1")
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")
	assert.Contains(t, sr.Source(), "127.0.0.1")

	err = sr.Close()
	assert.NoError(t, err)

	err = sysLog.Emerg("And this is a daemon emergency with demotag.")
	assert.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Equal(t, "", line)
	sysLog.Emerg("this is OK")
}
