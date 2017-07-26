package sender

import (
	"os"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

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

func TestFtMemorySender(t *testing.T) {
	_, pt := NewMockPandoraWithPrefix("v2")
	opt := &PandoraOption{
		name:           "p",
		repoName:       "TestFtMemorySender",
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
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "3"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
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

func TestFtChannelFullSender(t *testing.T) {
	mockP, pt := NewMockPandoraWithPrefix("v2")
	opt := &PandoraOption{
		name:           "p",
		repoName:       "TestFtMemorySender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "a",
		autoCreate:     "a *s",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	mockP.PostSleep = 1
	mp := conf.MapConf{}
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "1"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts, err := NewFtSender(s, mp)
	assert.NoError(t, err)

	var moreDatas, moreAndMoreDatas [][]Data
	for i := 0; i < 10; i++ {
		err = fts.Send([]Data{
			{"a": i},
		})
		se, ok := err.(*utils.StatsError)
		if !ok {
			t.Fatal("ft send return error should .(*StatsError)")
		}
		if se.ErrorDetail != nil {
			sx, succ := se.ErrorDetail.(*reqerr.SendError)
			if succ {
				datas := ConvertDatas(sx.GetFailDatas())
				moreDatas = append(moreDatas, datas)
			} else {
				t.Fatal("ft send StatsError error should contains send error", se.ErrorDetail)
			}
		}
	}
	mockP.PostSleep = 0
	for len(moreDatas) > 0 {
		for _, v := range moreDatas {
			time.Sleep(100 * time.Millisecond)
			err = fts.Send(v)
			se, ok := err.(*utils.StatsError)
			if !ok {
				t.Fatal("ft send return error should .(*SendError)")
			}
			if se.ErrorDetail != nil {
				sx, succ := se.ErrorDetail.(*reqerr.SendError)
				if succ {
					datas := ConvertDatas(sx.GetFailDatas())
					moreAndMoreDatas = append(moreAndMoreDatas, datas)
				} else {
					t.Fatal("ft send StatsError error should contains send error", se.ErrorDetail)
				}
			}
		}
		moreDatas = moreAndMoreDatas
		moreAndMoreDatas = make([][]Data, 0)
	}
	time.Sleep(time.Second)
	assert.Equal(t, mockP.PostDataNum, 10)

}
