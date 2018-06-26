package fault_tolerant

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/sender/mock"
	"github.com/qiniu/logkit/sender/mock_pandora"
	"github.com/qiniu/logkit/sender/pandora"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	fttestdir = "TestFtSender"
)

var (
	mockP, pt           = mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig = conf.MapConf{
		"name":                           "p",
		"pandora_region":                 "nb",
		"pandora_host":                   "http://127.0.0.1:" + pt,
		"pandora_schema":                 "ab",
		"pandora_auto_create":            "ab *s",
		"pandora_schema_free":            "false",
		"pandora_ak":                     "ak",
		"pandora_sk":                     "sk",
		"pandora_schema_update_interval": "1",
		"pandora_gzip":                   "false",

		"sender_type": "pandora",
	}
)

func TestFtSender(t *testing.T) {
	pandoraSenderConfig["pandora_repo_name"] = "TestFtSender"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[sender.KeyFtSaveLogPath] = fttestdir
	mp[sender.KeyFtStrategy] = sender.KeyFtStrategyAlwaysSave
	defer os.RemoveAll(fttestdir)
	fts, err := sender.NewFtSender(s, mp, fttestdir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "abcccc"},
		{"ab": "E18111:"},
	}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*SendError)")
	}
	assert.NoError(t, se.ErrorDetail)
	time.Sleep(10 * time.Second)
	if fts.BackupQueue.Depth() != 1 {
		t.Error("Ft send error exp 1 but got", fts.BackupQueue.Depth())
	}
}

func TestFtMemorySender(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	pandoraSenderConfig["pandora_repo_name"] = "TestFtMemorySender"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[sender.KeyFtSaveLogPath] = tmpDir
	mp[sender.KeyFtMemoryChannel] = "true"
	mp[sender.KeyFtMemoryChannelSize] = "3"
	mp[sender.KeyFtStrategy] = sender.KeyFtStrategyAlwaysSave
	fts, err := sender.NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "abcccc"},
		{"ab": "E18111:"},
	}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*SendError)")
	}
	assert.NoError(t, se.ErrorDetail)
	time.Sleep(10 * time.Second)
	if fts.BackupQueue.Depth() != 1 {
		t.Error("Ft send error exp 1 but got", fts.BackupQueue.Depth())
	}
}

func TestFtChannelFullSender(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	pandoraSenderConfig["pandora_repo_name"] = "TestFtChannelFullSender"
	pandoraSenderConfig["pandora_schema"] = "a"
	pandoraSenderConfig["pandora_auto_create"] = "a *s"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mockP.SetMux.Lock()
	mockP.PostSleep = 1
	mockP.SetMux.Unlock()
	mp := conf.MapConf{}
	mp[sender.KeyFtSaveLogPath] = tmpDir
	mp[sender.KeyFtMemoryChannel] = "true"
	mp[sender.KeyFtMemoryChannelSize] = "1"
	mp[sender.KeyFtStrategy] = sender.KeyFtStrategyAlwaysSave
	fts, err := sender.NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)

	var moreDatas, moreAndMoreDatas [][]Data
	for i := 0; i < 10; i++ {
		err = fts.Send([]Data{
			{"a": i},
		})
		se, ok := err.(*StatsError)
		if !ok {
			t.Fatal("ft send return error should .(*StatsError)")
		}
		if se.ErrorDetail != nil {
			sx, succ := se.ErrorDetail.(*reqerr.SendError)
			if succ {
				datas := sender.ConvertDatas(sx.GetFailDatas())
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
			se, ok := err.(*StatsError)
			if !ok {
				t.Fatal("ft send return error should .(*SendError)")
			}
			if se.ErrorDetail != nil {
				sx, succ := se.ErrorDetail.(*reqerr.SendError)
				if succ {
					datas := sender.ConvertDatas(sx.GetFailDatas())
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
	s, err := mock.NewSender(conf.MapConf{})
	if err != nil {
		t.Fatal(err)
	}
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	mp := conf.MapConf{}
	mp[sender.KeyFtSaveLogPath] = tmpDir
	mp[sender.KeyFtStrategy] = sender.KeyFtStrategyConcurrent
	mp[sender.KeyFtProcs] = "3"
	fts, err := sender.NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "ababab"},
		{"cd": "cdcdcd"},
	}
	for i := 0; i < 100; i++ {
		err = fts.Send(datas)
		se, ok := err.(*StatsError)
		if !ok {
			t.Fatal("ft send return error should .(*SendError)")
		}
		assert.NoError(t, se.ErrorDetail)
	}
	fts.Close()
	ms := s.(*mock.Sender)
	assert.Equal(t, 100, ms.SendCount())
	assert.Equal(t, len(datas)*100, len(ms.Datas))
}

func BenchmarkFtSenderConcurrentDirect(b *testing.B) {
	c := conf.MapConf{}
	c[sender.KeyFtStrategy] = sender.KeyFtStrategyConcurrent
	ftSenderConcurrent(b, c)
}

func BenchmarkFtSenderConcurrentDisk(b *testing.B) {
	c := conf.MapConf{}
	c[sender.KeyFtStrategy] = sender.KeyFtStrategyAlwaysSave
	ftSenderConcurrent(b, c)
}

func BenchmarkFtSenderConcurrentMemory(b *testing.B) {
	c := conf.MapConf{}
	c[sender.KeyFtStrategy] = sender.KeyFtStrategyAlwaysSave
	c[sender.KeyFtMemoryChannel] = "true"
	ftSenderConcurrent(b, c)
}

func ftSenderConcurrent(b *testing.B, c conf.MapConf) {
	log.SetOutputLevel(log.Lerror)
	s, err := mock.NewSender(conf.MapConf{})
	if err != nil {
		b.Fatal(err)
	}
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	c[sender.KeyFtSaveLogPath] = tmpDir
	c[sender.KeyFtProcs] = "3"
	fts, err := sender.NewFtSender(s, c, tmpDir)
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
			se, _ := err.(*StatsError)
			if se.ErrorDetail == nil {
				break
			}
		}
	}
	b.StopTimer()
	fts.Close()
	ms := s.(*mock.Sender)
	b.Logf("Benchmark.N: %d", b.N)
	b.Logf("MockSender.SendCount: %d", ms.SendCount())
}

func TestFtSenderConvertData(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	mockP, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	senderConfig := conf.MapConf{
		"pandora_repo_name":              "TestFtSenderConvertData",
		"name":                           "p",
		"pandora_region":                 "nb",
		"pandora_host":                   "http://127.0.0.1:" + pt,
		"pandora_schema":                 "",
		"pandora_auto_create":            "",
		"pandora_schema_free":            "true",
		"pandora_ak":                     "ak",
		"pandora_sk":                     "sk",
		"pandora_schema_update_interval": "1",
		"pandora_gzip":                   "false",
		"logkit_send_time":               "false",

		"sender_type": "pandora",
	}

	s, err := pandora.NewSender(senderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mockP.SetMux.Lock()
	mockP.PostSleep = 1
	mockP.SetMux.Unlock()
	mp := conf.MapConf{}
	mp[sender.KeyFtSaveLogPath] = tmpDir
	mp[sender.KeyFtMemoryChannel] = "false"
	mp[sender.KeyFtStrategy] = sender.KeyFtStrategyBackupOnly
	fts, err := sender.NewFtSender(s, mp, tmpDir)
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
		se, ok := err.(*StatsError)
		if !ok {
			t.Fatal("ft send return error should .(*StatsError)")
		}
		if se.ErrorDetail != nil {
			sx, succ := se.ErrorDetail.(*reqerr.SendError)
			if succ {
				datas := sender.ConvertDatas(sx.GetFailDatas())
				moreDatas = append(moreDatas, datas)
			} else if !(se.Ft && se.FtNotRetry) {
				t.Fatal("ft send StatsError error should contains send error", se.ErrorDetail)
			}
		}
		<-exitChan
	}
}
