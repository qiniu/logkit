package mgr

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiniu/logkit/cleaner"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"

	"github.com/qiniu/log"
	"github.com/stretchr/testify/assert"
)

func cleanMetaFolder(path string) {
	err := os.Remove(path + "/buf.dat")
	if err != nil {
		log.Println(err)
	}
	err = os.Remove(path + "/buf.meta")
	if err != nil {
		log.Println(err)
	}
	err = os.Remove(path + "/file.meta")
	if err != nil {
		log.Println(err)
	}
}

func Test_Run(t *testing.T) {
	dir := "Test_Run"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logpath := dir + "/logdir"
	logpathLink := dir + "/logdirlink"
	metapath := dir + "/meta_mock_csv"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	absLogpath, err := filepath.Abs(logpath)
	if err != nil {
		t.Fatalf("filepath.Abs %v, %v", logpath, err)
	}
	absLogpathLink, err := filepath.Abs(logpathLink)
	if err != nil {
		t.Fatalf("filepath.Abs %v, %v", logpathLink, err)
	}
	if err := os.Symlink(absLogpath, absLogpathLink); err != nil {
		log.Fatalf("Test_Run error symbol link %v to %v: %v", absLogpathLink, logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	log1 := `hello 123
	xx 1
	`
	log2 := `h 456
	x 789`
	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}
	rinfo := RunnerInfo{
		RunnerName:   "test_runner",
		MaxBatchLen:  1,
		MaxBatchSize: 2048,
	}
	readerConfig := conf.MapConf{
		"log_path":  logpathLink,
		"meta_path": metapath,
		"mode":      "dir",
		"read_from": "oldest",
	}
	meta, err := reader.NewMetaWithConf(readerConfig)
	if err != nil {
		t.Error(err)
	}
	reader, err := reader.NewFileBufReader(readerConfig)
	if err != nil {
		t.Error(err)
	}
	cleanChan := make(chan cleaner.CleanSignal)
	cleaner, err := cleaner.NewCleaner(conf.MapConf{}, meta, cleanChan, readerConfig["log_path"])
	if err != nil {
		t.Error(err)
	}
	parseConf := conf.MapConf{
		"name":         "req_csv",
		"type":         "csv",
		"csv_schema":   "logtype string, xx long",
		"csv_splitter": " ",
	}
	ps := parser.NewParserRegistry()
	pparser, err := ps.NewLogParser(parseConf)
	if err != nil {
		t.Error(err)
	}
	senderConfigs := []conf.MapConf{
		conf.MapConf{
			"name":        "mock_sender",
			"sender_type": "mock",
		},
	}
	var senders []sender.Sender
	raws, err := sender.NewMockSender(senderConfigs[0])
	s, succ := raws.(*sender.MockSender)
	if !succ {
		t.Error("sender should be mock sender")
	}
	if err != nil {
		t.Error(err)
	}
	senders = append(senders, s)

	r, err := NewLogExportRunnerWithService(rinfo, reader, cleaner, pparser, senders, meta)
	if err != nil {
		t.Error(err)
	}

	cleanInfo := CleanInfo{
		enable: false,
		logdir: absLogpath,
	}
	assert.Equal(t, cleanInfo, r.Cleaner())

	go r.Run()
	timer := time.NewTimer(20 * time.Second).C
	for {
		if s.SendCount() >= 4 {
			break
		}
		select {
		case <-timer:
			t.Error("runner didn't stop within ticker time")
			return
		default:
			time.Sleep(time.Second)
		}
	}
	var dts []sender.Data
	rawData := r.senders[0].Name()[len("mock_sender "):]
	err = json.Unmarshal([]byte(rawData), &dts)
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 4 {
		t.Errorf("got sender data not match error,expect 2 but %v", len(dts))
	}
}
