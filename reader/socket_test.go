package reader

import (
	"log"
	"log/syslog"
	"os"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
)

func TestUdpSocketReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath:             metaDir,
		KeyFileDone:             metaDir,
		KeyRunnerName:           "TestUdpSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "udp://:5140",
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)

	ssr, err := NewSocketReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*SocketReader)

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
		KeyMetaPath:             metaDir,
		KeyFileDone:             metaDir,
		KeyRunnerName:           "TestTCPSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "tcp://:5141",
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)

	ssr, err := NewSocketReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*SocketReader)
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
		KeyMetaPath:             metaDir,
		KeyFileDone:             metaDir,
		KeyRunnerName:           "TestUnixSocketReader",
		KeyMode:                 ModeSocket,
		KeySocketServiceAddress: "unix://./TestUnixSocketReader/log.socket",
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)

	err = os.Mkdir("TestUnixSocketReader", DefaultDirPerm)
	assert.NoError(t, err)
	defer os.RemoveAll("TestUnixSocketReader")

	ssr, err := NewSocketReader(meta, logkitConf)
	assert.NoError(t, err)
	sr := ssr.(*SocketReader)
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
