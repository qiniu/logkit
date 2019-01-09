package fault_tolerant

import (
	"fmt"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/pipeline"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/sender/mock"
	"github.com/qiniu/logkit/sender/mock_pandora"
	"github.com/qiniu/logkit/sender/pandora"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	fttestdir = "TestFtSender"
)

func TestFtSender(t *testing.T) {
	_, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
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
	pandoraSenderConfig["pandora_repo_name"] = "TestFtSender"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = fttestdir
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	defer os.RemoveAll(fttestdir)
	fts, err := sender.NewFtSender(s, mp, fttestdir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "abcccc"},
		{"ab": "E18110:BackupQueue.Depth"},
	}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.Nil(t, se.SendError)
	time.Sleep(5 * time.Second)
	if fts.BackupQueue.Depth() != 1 {
		t.Error("Ft send error exp 1 but got ", fts.BackupQueue.Depth())
	}

	ftTestDir2 := "TestFtSender2"
	mp[KeyFtSaveLogPath] = ftTestDir2
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts2, err := sender.NewFtSender(s, mp, ftTestDir2)
	defer os.RemoveAll(ftTestDir2)
	assert.Nil(t, err)
	var maxData string
	for {
		if int64(len(maxData)) > 2*DefaultMaxBatchSize {
			break
		}
		maxData += "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789七牛云存储？？？？？？七牛云存储？？？？七牛云存储！！！！！七牛云存储@@@@@七牛云存储&&&&&&七牛云存储…………………………七牛云存储！！！！！！"
	}
	datas2 := []Data{
		{"ab": maxData},
	}
	err = fts2.Send(datas2)
	se, ok = err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.Nil(t, se.SendError)
	time.Sleep(5 * time.Second)
	if fts2.BackupQueue.Depth() != 0 {
		t.Error("Ft send error exp 0 but got ", fts2.BackupQueue.Depth())
	}

	ftTestDir3 := "TestFtSender3"
	mp[KeyFtSaveLogPath] = ftTestDir3
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts3, err := sender.NewFtSender(s, mp, ftTestDir3)
	defer os.RemoveAll(ftTestDir3)
	assert.NoError(t, err)
	datas3 := []Data{
		{"ab": "E18110:"},
	}
	err = fts3.Send(datas3)
	se, ok = err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.Nil(t, se.SendError)
	time.Sleep(5 * time.Second)
	if fts3.BackupQueue.Depth() != 0 {
		t.Error("Ft send error exp 0 but got ", fts3.BackupQueue.Depth())
	}
}

func TestFtDiscardLast(t *testing.T) {
	p, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
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

		"sender_type": "pandora",
	}
	pandoraSenderConfig["pandora_repo_name"] = "TestFtSender"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = fttestdir
	mp[KeyFtStrategy] = KeyFtStrategyBackupOnly
	mp[KeyFtDiscardErr] = "true"
	defer os.RemoveAll(fttestdir)
	fts, err := sender.NewFtSender(s, mp, fttestdir)
	assert.NoError(t, err)
	datas := []Data{
		{"a": "11111"},
		{"b": "22222"},
		{"c": "33333"},
		{"d": "It's-an-error"},
	}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.NotEmpty(t, se.LastError)
	assert.NotNil(t, se.SendError)
	time.Sleep(5 * time.Second)
	//get three records
	p.SetMux.Lock()
	assert.Equal(t, 3, p.DumpDataNum)
	p.SetMux.Unlock()

	mp2 := conf.MapConf{}
	mp2[KeyFtSaveLogPath] = fttestdir
	mp2[KeyFtStrategy] = KeyFtStrategyBackupOnly
	mp2[KeyFtDiscardErr] = "false"
	fts2, err := sender.NewFtSender(s, mp2, fttestdir)
	err = fts2.Send(datas)
	se, ok = err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.NotEmpty(t, se.LastError)
	assert.NotNil(t, se.SendError)
	time.Sleep(5 * time.Second)
	//get four records
	p.SetMux.Lock()
	assert.Equal(t, 7, p.DumpDataNum)
	p.SetMux.Unlock()
}

func TestInvalidData(t *testing.T) {
	p, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
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

		"sender_type": "pandora",
	}
	pandoraSenderConfig["pandora_repo_name"] = "TestFtSender"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = fttestdir
	mp[KeyFtStrategy] = KeyFtStrategyBackupOnly
	mp[KeyFtDiscardErr] = "true"
	defer os.RemoveAll(fttestdir)
	fts, err := sender.NewFtSender(s, mp, fttestdir)
	assert.NoError(t, err)
	datas := []Data{
		{"test1": "11111"},
		{"b": "22222"},
		{"ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc": "33333"},
		{"ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd": "long key"},
	}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.NotEmpty(t, se.LastError)
	assert.NotNil(t, se.SendError)
	if se.SendError != nil{
		assert.Equal(t, reqerr.TypeContainInvalidPoint, se.SendError.ErrorType)
	}
	time.Sleep(5 * time.Second)
	//get three records
	p.SetMux.Lock()
	assert.Equal(t, 4, p.DumpDataNum)
	p.SetMux.Unlock()

	mp2 := conf.MapConf{}
	mp2[KeyFtSaveLogPath] = fttestdir
	mp2[KeyFtStrategy] = KeyFtStrategyBackupOnly
	mp2[KeyFtDiscardErr] = "false"
	fts2, err := sender.NewFtSender(s, mp2, fttestdir)
	err = fts2.Send(datas)
	se, ok = err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.NotEmpty(t, se.LastError)
	assert.NotNil(t, se.SendError)
	time.Sleep(5 * time.Second)
	//get four records
	p.SetMux.Lock()
	assert.Equal(t, 8, p.DumpDataNum)
	p.SetMux.Unlock()
}


func TestFtMemorySender(t *testing.T) {
	_, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
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
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "3"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts, err := sender.NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "abcccc"},
		{"ab": "E18110:BackupQueue.Depth"},
	}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.Nil(t, se.SendError)
	time.Sleep(10 * time.Second)
	if fts.BackupQueue.Depth() != 1 {
		t.Error("Ft send error exp 1 but got", fts.BackupQueue.Depth())
	}
}

func TestFtMemoryEmptySender(t *testing.T) {
	mockPandora, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
		"name":                           "p",
		"pandora_region":                 "nb",
		"pandora_host":                   "http://127.0.0.1:" + pt,
		"pandora_schema":                 "ab a1",
		"pandora_schema_free":            "true",
		"pandora_ak":                     "ak",
		"pandora_sk":                     "sk",
		"pandora_schema_update_interval": "1",
		"pandora_gzip":                   "false",
		"logkit_send_time":               "false",

		"sender_type": "pandora",
	}
	mockPandora.Schemas = []pipeline.RepoSchemaEntry{{Key: "a1", ValueType: "string", Required: false}}
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	pandoraSenderConfig["pandora_repo_name"] = "TestFtMemoryEmptySender"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "3"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
	fts, err := sender.NewFtSender(s, mp, tmpDir)
	assert.NoError(t, err)
	datas := []Data{{"c": "E18006:BackupQueue.Depth"}}
	err = fts.Send(datas)
	se, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	assert.Nil(t, se.SendError)
	time.Sleep(10 * time.Second)
	assert.Zero(t, fts.BackupQueue.Depth())
}

func TestFtChannelFullSender(t *testing.T) {
	mockP, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
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
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "true"
	mp[KeyFtMemoryChannelSize] = "1"
	mp[KeyFtStrategy] = KeyFtStrategyAlwaysSave
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

		if se.SendError != nil {
			moreDatas = append(moreDatas, sender.ConvertDatas(se.SendError.GetFailDatas()))
		}

	}
	assert.NotEmpty(t, fts.Stats().LastError)

	mockP.SetMux.Lock()
	mockP.PostSleep = 0
	mockP.SetMux.Unlock()
	for len(moreDatas) > 0 {
		for _, v := range moreDatas {
			time.Sleep(100 * time.Millisecond)
			err = fts.Send(v)
			se, ok := err.(*StatsError)
			if !ok {
				t.Fatal("ft send return error should .(*StatsError)")
			}
			if se.SendError != nil {
				moreAndMoreDatas = append(moreAndMoreDatas, sender.ConvertDatas(se.SendError.GetFailDatas()))
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
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtProcs] = "3"
	mp[KeyFtStrategy] = KeyFtStrategyConcurrent
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
			t.Fatal("ft send return error should .(*StatsError)")
		}
		assert.Nil(t, se.SendError)
	}
	fts.Close()
	ms := s.(*mock.Sender)
	assert.Equal(t, 100, ms.SendCount())
	assert.Equal(t, len(datas)*100, len(ms.Datas))
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
	s, err := mock.NewSender(conf.MapConf{})
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
			if se.SendError == nil {
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
	mp[KeyFtSaveLogPath] = tmpDir
	mp[KeyFtMemoryChannel] = "false"
	mp[KeyFtStrategy] = KeyFtStrategyBackupOnly
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
		if se.SendError != nil {
			datas := sender.ConvertDatas(se.SendError.GetFailDatas())
			moreDatas = append(moreDatas, datas)
			if !(se.Ft && se.FtNotRetry) {
				t.Fatal("ft send StatsError error should contains send error", se.SendError)
			}
		}
		<-exitChan
	}
}

func Test_SplitData(t *testing.T) {
	var maxData string
	for {
		if int64(len(maxData)) > DefaultMaxBatchSize {
			break
		}
		maxData += "abcdefghijklmnopqrstuvwxyz七牛云？？？********0123456789七牛云？？？********abcdefghijklmnopqrstuvwxyz七牛云？？？********0123456789七牛云？？？********"
	}
	valArray := sender.SplitData(maxData)
	assert.Equal(t, len(maxData), len(strings.Join(valArray, "")))
	assert.Equal(t, 2, len(valArray))

	for {
		if int64(len(maxData)) > 2*DefaultMaxBatchSize {
			break
		}
		maxData += "abcdefghijklmnopqrstuvwxyz七牛云？？？********0123456789七牛云？？？********abcdefghijklmnopqrstuvwxyz七牛云？？？********0123456789七牛云？？？********"
	}

	valArray = sender.SplitData(maxData)
	assert.Equal(t, len(maxData), len(strings.Join(valArray, "")))
	assert.Equal(t, 3, len(valArray))

	maxData += "\n"
	for {
		if int64(len(maxData)) > 3*DefaultMaxBatchSize {
			break
		}
		maxData += "abcdefghijklmnopqrstuvwxyz七牛云？？？********0123456789七牛云？？？********abcdefghijklmnopqrstuvwxyz七牛云？？？********0123456789七牛云？？？********\n"
	}
	valArray = sender.SplitData(maxData)
	assert.Equal(t, 4, len(valArray))
	assert.Equal(t, 1441792, len(valArray[0]))
	assert.Equal(t, 1441792, len(valArray[1]))
	assert.Equal(t, 1310849, len(valArray[2]))
	assert.Equal(t, 2097096, len(valArray[3]))

	maxData = "abc"
	valArray = sender.SplitData(maxData)
	assert.Equal(t, len(maxData), len(strings.Join(valArray, "")))
	assert.Equal(t, 1, len(valArray))

	maxData = ""
	valArray = sender.SplitData(maxData)
	assert.Equal(t, len(maxData), len(strings.Join(valArray, "")))
	assert.Equal(t, 0, len(valArray))

	maxData = "abc七牛云？？？cde"
	valArray = sender.SplitData(maxData)
	assert.Equal(t, len(maxData), len(strings.Join(valArray, "")))
	assert.Equal(t, 1, len(valArray))

	maxData = "abcde"
	valArray = sender.SplitData(maxData)
	assert.Equal(t, len(maxData), len(strings.Join(valArray, "")))
	assert.Equal(t, 1, len(valArray))
}

func TestTypeSchemaRetry(t *testing.T) {
	_, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	pandoraSenderConfig := conf.MapConf{
		"name":                           "p",
		"pandora_region":                 "nb",
		"pandora_host":                   "http://127.0.0.1:" + pt,
		"pandora_schema":                 "ab",
		"pandora_auto_create":            "ab *s",
		"pandora_schema_free":            "true",
		"pandora_ak":                     "ak",
		"pandora_sk":                     "sk",
		"pandora_schema_update_interval": "1",
		"pandora_gzip":                   "false",

		"sender_type": "pandora",
	}
	pandoraSenderConfig["pandora_repo_name"] = "TestTypeSchemaRetry"
	s, err := pandora.NewSender(pandoraSenderConfig)
	if err != nil {
		t.Fatal(err)
	}
	mp := conf.MapConf{}
	mp[KeyFtSaveLogPath] = fttestdir
	mp[KeyFtStrategy] = KeyFtStrategyBackupOnly
	defer os.RemoveAll(fttestdir)
	fts, err := sender.NewFtSender(s, mp, fttestdir)
	assert.NoError(t, err)
	datas := []Data{
		{"ab": "abcccc"},
		{"ab": "E18111:"},
	}
	err = fts.Send(datas)
	_, ok := err.(*StatsError)
	if !ok {
		t.Fatal("ft send return error should .(*StatsError)")
	}
	time.Sleep(5 * time.Second)
	if fts.BackupQueue.Depth() != 1 {
		t.Error("Ft send error exp 1 but got ", fts.BackupQueue.Depth())
	}
}

func TestSkipDeepCopySender(t *testing.T) {
	defer os.RemoveAll("tmp")

	// Skip == false
	{
		fs, err := sender.NewFtSender(&pandora.Sender{}, nil, "tmp")
		assert.Nil(t, err)
		assert.False(t, fs.SkipDeepCopy())
	}

	// Skip == true
	{
		fs, err := sender.NewFtSender(&mock.Sender{}, nil, "tmp")
		assert.Nil(t, err)
		assert.True(t, fs.SkipDeepCopy())
	}
}

func TestPandoraExtraInfo(t *testing.T) {
	pandoraServer, pt := mock_pandora.NewMockPandoraWithPrefix("/v2")
	conf1 := conf.MapConf{
		"force_microsecond":         "false",
		"ft_memory_channel":         "false",
		"ft_strategy":               "backup_only",
		"ignore_invalid_field":      "true",
		"logkit_send_time":          "false",
		"pandora_extra_info":        "true",
		"pandora_ak":                "ak",
		"pandora_auto_convert_date": "true",
		"pandora_gzip":              "true",
		"pandora_host":              "http://127.0.0.1:" + pt,
		"pandora_region":            "nb",
		"pandora_repo_name":         "TestPandoraSenderTime",
		"pandora_schema_free":       "true",
		"pandora_sk":                "sk",
		"runner_name":               "runner.20171117110730",
		"sender_type":               "pandora",
		"name":                      "TestPandoraSenderTime",
		"KeyPandoraSchemaUpdateInterval": "1s",
	}

	innerSender, err := pandora.NewSender(conf1)
	if err != nil {
		t.Fatal(err)
	}
	s, err := sender.NewFtSender(innerSender, conf1, fttestdir)
	defer os.RemoveAll(fttestdir)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "123.2"
	d["hostname"] = "123.2"
	d["hostname0"] = "123.2"
	d["hostname1"] = "123.2"
	d["hostname2"] = "123.2"
	d["osinfo"] = "123.2"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.SendError
	}
	if err != nil {
		t.Error(err)
	}
	resp := pandoraServer.Body
	assert.Equal(t, true, strings.Contains(resp, "core"))
	assert.Equal(t, true, strings.Contains(resp, "x1=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "osinfo=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "hostname=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "hostname0=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "hostname1=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "hostname2=123.2"))

	conf2 := conf.MapConf{
		"force_microsecond":         "false",
		"ft_memory_channel":         "false",
		"ft_strategy":               "backup_only",
		"ignore_invalid_field":      "true",
		"logkit_send_time":          "false",
		"pandora_extra_info":        "false",
		"pandora_ak":                "ak",
		"pandora_auto_convert_date": "true",
		"pandora_gzip":              "true",
		"pandora_host":              "http://127.0.0.1:" + pt,
		"pandora_region":            "nb",
		"pandora_repo_name":         "TestPandoraSenderTime",
		"pandora_schema_free":       "true",
		"pandora_sk":                "sk",
		"runner_name":               "runner.20171117110730",
		"sender_type":               "pandora",
		"name":                      "TestPandoraSenderTime",
		"KeyPandoraSchemaUpdateInterval": "1s",
	}
	innerSender, err = pandora.NewSender(conf2)
	if err != nil {
		t.Fatal(err)
	}

	s, err = sender.NewFtSender(innerSender, conf1, fttestdir)
	d = Data{
		"*x1":        "123.2",
		"x2.dot":     "123.2",
		"@timestamp": "2018-07-18T10:17:36.549054846+08:00",
	}
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.SendError
	}
	if err != nil {
		t.Error(err)
	}
	resp = pandoraServer.Body
	assert.Equal(t, true, strings.Contains(resp, "x1=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "x2_dot=123.2"))
	assert.Equal(t, true, strings.Contains(resp, "timestamp=2018-07-18T10:17:36.549054846+08:00"))
}
