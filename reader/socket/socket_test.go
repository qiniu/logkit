package socket

import (
	"log"
	"log/syslog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestUdpSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath:             MetaDir,
		reader.KeyFileDone:             MetaDir,
		KeyRunnerName:                  "TestUdpSocketReader",
		reader.KeyMode:                 reader.ModeSocket,
		reader.KeySocketServiceAddress: "udp://:5140",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)

	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("udp", "localhost:5140",
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
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestTCPSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath:             MetaDir,
		reader.KeyFileDone:             MetaDir,
		KeyRunnerName:                  "TestTCPSocketReader",
		reader.KeyMode:                 reader.ModeSocket,
		reader.KeySocketServiceAddress: "tcp://:5141",
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)

	ssr, err := NewReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*Reader)
	err = sr.Start()
	assert.NoError(t, err)

	sysLog, err := syslog.Dial("tcp", "localhost:5141",
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
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")

	err = sr.Close()
	assert.NoError(t, err)
}

func TestUnixSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath:             MetaDir,
		reader.KeyFileDone:             MetaDir,
		KeyRunnerName:                  "TestUnixSocketReader",
		reader.KeyMode:                 reader.ModeSocket,
		reader.KeySocketServiceAddress: "unix://./TestUnixSocketReader/log.socket",
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

	sysLog, err := syslog.Dial("unix", "./TestUnixSocketReader/log.socket",
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
	line, err = sr.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, line, "this is OK")

	err = sr.Close()
	assert.NoError(t, err)
}
