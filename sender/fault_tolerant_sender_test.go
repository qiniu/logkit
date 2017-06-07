package sender

import (
	"os"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

const (
	fttestdir = "TestFtSender"
)

func TestFtSender(t *testing.T) {
	_, pt := NewMockPandoraWithPrefix("v2")
	opt := &PandoraOption{
		name:           "p",
		repoName:       "TestFtSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "ab",
		autoCreate:     "ab *s",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = fttestdir
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	defer os.RemoveAll(fttestdir)
	fts, err := NewFtSender(s, mp)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "abcccc"},
		{"ab": "E18111:"},
	}
	err = fts.Send(datas)
	se, ok := err.(*utils.StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*SendError)")
	}
	assert.NoError(t, se.ErrorDetail)
	time.Sleep(10 * time.Second)
	if fts.backupQueue.Depth() != 1 {
		t.Error("Ft sender error exp 1 but got", fts.backupQueue.Depth())
	}
}
