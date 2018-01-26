package sender

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/stretchr/testify/assert"
)

const (
	fttestdir = "TestFtSender"
)

func TestFtSender(t *testing.T) {
	_, pt := NewMockPandoraWithPrefix("/v2")
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
	fts, err := NewFtSender(s, mp, fttestdir)
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
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	_, pt := NewMockPandoraWithPrefix("/v2")
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
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "3"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts, err := NewFtSender(s, mp, tmpDir)
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
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	mockP, pt := NewMockPandoraWithPrefix("/v2")
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
	mockP.SetMux.Lock()
	mockP.PostSleep = 1
	mockP.SetMux.Unlock()
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "1"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts, err := NewFtSender(s, mp, tmpDir)
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
	mockP.SetMux.Lock()
	mockP.PostSleep = 0
	mockP.SetMux.Unlock()
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
	mockP.SetMux.Lock()
	assert.Equal(t, mockP.PostDataNum, 10)
	mockP.SetMux.Unlock()
}

func TestFtSenderConcurrent(t *testing.T) {
	s, err := NewMockSender(conf.MapConf{})
	if err != nil {
		t.Fatal(err)
	}
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtStrategy] = KeyFtStrategyConcurrent
	mp[KeyFtProcs] = "3"
	fts, err := NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "ababab"},
		{"cd": "cdcdcd"},
	}
	for i := 0; i < 100; i++ {
		err = fts.Send(datas)
		se, ok := err.(*utils.StatsError)
		if !ok {
			t.Fatal("ft send return error should .(*SendError)")
		}
		assert.NoError(t, se.ErrorDetail)
	}
	fts.Close()
	ms := s.(*MockSender)
	assert.Equal(t, 100, ms.SendCount())
	assert.Equal(t, len(datas)*100, len(ms.datas))
}

func BenchmarkFtSenderConcurrentDirect(b *testing.B) {
	c := conf.MapConf{}
	c[KeyFtStrategy] = KeyFtStrategyConcurrent
	ftSenderConcurrent(b, c)
}

func BenchmarkFtSenderConcurrentDisk(b *testing.B) {
	c := conf.MapConf{}
	c[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	ftSenderConcurrent(b, c)
}

func BenchmarkFtSenderConcurrentMemory(b *testing.B) {
	c := conf.MapConf{}
	c[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	c[KeyFtMemoryChannel] = "true"
	ftSenderConcurrent(b, c)
}

func ftSenderConcurrent(b *testing.B, c conf.MapConf) {
	log.SetOutputLevel(log.Lerror)
	s, err := NewMockSender(conf.MapConf{})
	if err != nil {
		b.Fatal(err)
	}
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	c[KeyFtSaveLogPath] = tmpDir
	c[KeyFtProcs] = "3"
	fts, err := NewFtSender(s, c, tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	datas := []Data{
		{"ab": "ababab"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			err = fts.Send(datas)
			se, _ := err.(*utils.StatsError)
			if se.ErrorDetail == nil {
				break
			}
		}
	}
	b.StopTimer()
	fts.Close()
	ms := s.(*MockSender)
	b.Logf("Benchmark.N: %d", b.N)
	b.Logf("MockSender.SendCount: %d", ms.SendCount())
}

func TestFtSenderConvertData(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	mockP, pt := NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "p",
		repoName:       "TestFtSenderConvertData",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schemaFree:     true,
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	mockP.SetMux.Lock()
	mockP.PostSleep = 1
	mockP.SetMux.Unlock()
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "false"
	mp[KeyFtStrategy] = KeyFtStrategyBackupOnly
	fts, err := NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)
	expStr := []string{"a=typeBinaryUnpack", `pandora_stash={"a":"typeBinaryUnpack"}`, "a=typeBinaryUnpack", `pandora_stash={"a":"typeBinaryUnpack"}`}

	exitChan := make(chan string)
	go func() {
		now := time.Now()
		curIndex := 0
		for {
			mockP.BodyMux.RLock()
			if mockP.Body == expStr[curIndex] {
				curIndex += 1
				if curIndex%2 != 0 {
					exitChan <- mockP.Body
				}
			}
			mockP.BodyMux.RUnlock()
			if curIndex == 4 {
				break
			}
			if time.Now().Sub(now).Seconds() > 10 {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		assert.Equal(t, 4, curIndex)
	}()

	var moreDatas [][]Data
	for i := 0; i < 2; i++ {
		err = fts.Send([]Data{
			{"a": "typeBinaryUnpack"},
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
		<-exitChan
	}
}
