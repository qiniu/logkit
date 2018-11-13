package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/cleaner"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	parserConf "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/qiniu"
	"github.com/qiniu/logkit/reader"
	readerConf "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/router"
	"github.com/qiniu/logkit/sender"
	_ "github.com/qiniu/logkit/sender/builtin"
	senderConf "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/sender/discard"
	"github.com/qiniu/logkit/sender/mock"
	"github.com/qiniu/logkit/sender/pandora"
	"github.com/qiniu/logkit/transforms"
	_ "github.com/qiniu/logkit/transforms/builtin"
	"github.com/qiniu/logkit/transforms/ip"
	"github.com/qiniu/logkit/transforms/mutate"
	"github.com/qiniu/logkit/utils/equeue"
	. "github.com/qiniu/logkit/utils/models"
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
	dir := "Test_RunForErrData"
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logpath := dir + "/logdir"
	logpathLink := dir + "/logdirlink"
	metapath := dir + "/meta_mock_csv"
	if err := os.Mkdir(logpath, DefaultDirPerm); err != nil {
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
	if err := os.Mkdir(metapath, DefaultDirPerm); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	log1 := `hello 123
	xx 1
	`
	log2 := `
`
	log3 := `h 456
	x 789`

	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log3 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log3"), []byte(log3), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}

	exppath1 := filepath.Join(absLogpath, "log1")
	exppath3 := filepath.Join(absLogpath, "log3")
	exppaths := []string{exppath1, exppath1, exppath3, exppath3}
	rinfo := RunnerInfo{
		RunnerName:   "test_runner",
		MaxBatchLen:  1,
		MaxBatchSize: 2048,
	}
	readerConfig := conf.MapConf{
		"log_path":        logpathLink,
		"meta_path":       metapath,
		"mode":            "dir",
		"read_from":       "oldest",
		"datasource_tag":  "testtag",
		"reader_buf_size": "16",
	}
	meta, err := reader.NewMetaWithConf(readerConfig)
	if err != nil {
		t.Error(err)
	}
	isFromWeb := false
	r, err := reader.NewFileBufReader(readerConfig, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	cleanChan := make(chan cleaner.CleanSignal)
	cleanerConfig := conf.MapConf{
		"delete_enable": "true",
	}
	c, err := cleaner.NewCleaner(cleanerConfig, meta, cleanChan, meta.LogPath())
	if err != nil {
		t.Error(err)
	}
	parseConf := conf.MapConf{
		"name":                   "req_csv",
		"type":                   parserConf.TypeCSV,
		"csv_schema":             "logtype string, xx long",
		"csv_splitter":           " ",
		"disable_record_errdata": "true",
	}
	ps := parser.NewRegistry()
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
	raws, err := mock.NewSender(senderConfigs[0])
	s, succ := raws.(*mock.Sender)
	if !succ {
		t.Error("sender should be mock sender")
	}
	if err != nil {
		t.Error(err)
	}
	senders = append(senders, s)

	runner, err := NewLogExportRunnerWithService(rinfo, r, c, pparser, nil, senders, nil, meta)
	if err != nil {
		t.Error(err)
	}

	cleanInfo := CleanInfo{
		enable: true,
		logdir: absLogpath,
	}
	assert.Equal(t, cleanInfo, runner.Cleaner())

	go runner.Run()
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
	var dts []Data
	rawData := runner.senders[0].Name()[len("mock_sender "):]
	err = jsoniter.Unmarshal([]byte(rawData), &dts)
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 4 {
		t.Errorf("got sender data not match error,expect 4 but %v", len(dts))
	}
	for idx, dt := range dts {
		assert.Equal(t, exppaths[idx], dt["testtag"])
	}
}

func Test_RunForEnvTag(t *testing.T) {
	dir := "Test_RunForEnvTag"
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("Test_RunForEnvTag error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	originEnv := os.Getenv("Test_RunForEnvTag")
	defer func() {
		os.Setenv("Test_RunForEnvTag", originEnv)
	}()
	if err := os.Setenv("Test_RunForEnvTag", "{\"Test_RunForEnvTag\":\"env_value\"}"); err != nil {
		t.Fatalf("set env %v to %v error %v", "Test_RunForEnvTag", "env_value", err)
	}
	logpath := dir + "/logdir"
	logpathLink := dir + "/logdirlink"
	metapath := dir + "/meta_mock_csv"
	if err := os.Mkdir(logpath, DefaultDirPerm); err != nil {
		log.Fatalf("Test_RunForEnvTag error mkdir %v %v", logpath, err)
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
	if err := os.Mkdir(metapath, DefaultDirPerm); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	log1 := `hello 123
	xx 1
	`
	log2 := `
`
	log3 := `h 456
	x 789`

	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log3 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log3"), []byte(log3), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}

	rinfo := RunnerInfo{
		RunnerName:   "test_runner",
		MaxBatchLen:  1,
		MaxBatchSize: 2048,
		EnvTag:       "Test_RunForEnvTag",
	}
	readerConfig := conf.MapConf{
		"log_path":        logpathLink,
		"meta_path":       metapath,
		"mode":            "dir",
		"read_from":       "oldest",
		"reader_buf_size": "16",
	}
	meta, err := reader.NewMetaWithConf(readerConfig)
	if err != nil {
		t.Error(err)
	}
	isFromWeb := false
	reader, err := reader.NewFileBufReader(readerConfig, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	cleanChan := make(chan cleaner.CleanSignal)
	cleanerConfig := conf.MapConf{
		"delete_enable": "true",
	}
	cleaner, err := cleaner.NewCleaner(cleanerConfig, meta, cleanChan, meta.LogPath())
	if err != nil {
		t.Error(err)
	}
	parseConf := conf.MapConf{
		"name":                   "req_csv",
		"type":                   "csv",
		"csv_schema":             "logtype string, xx long",
		"csv_splitter":           " ",
		"disable_record_errdata": "true",
	}
	ps := parser.NewRegistry()
	pparser, err := ps.NewLogParser(parseConf)
	if err != nil {
		t.Error(err)
	}
	senderConfigs := []conf.MapConf{
		{
			"name":        "mock_sender",
			"sender_type": "mock",
		},
	}
	var senders []sender.Sender
	raws, err := mock.NewSender(senderConfigs[0])
	s, succ := raws.(*mock.Sender)
	if !succ {
		t.Error("sender should be mock sender")
	}
	if err != nil {
		t.Error(err)
	}
	senders = append(senders, s)

	r, err := NewLogExportRunnerWithService(rinfo, reader, cleaner, pparser, nil, senders, nil, meta)
	if err != nil {
		t.Error(err)
	}

	cleanInfo := CleanInfo{
		enable: true,
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
	var dts []Data
	rawData := r.senders[0].Name()[len("mock_sender "):]
	err = jsoniter.Unmarshal([]byte(rawData), &dts)
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 4 {
		t.Errorf("got sender data not match error,expect 4 but %v", len(dts))
	}
	for _, d := range dts {
		if v, ok := d["Test_RunForEnvTag"]; !ok {
			t.Fatalf("Test_RunForEnvTag error, exp got Test_RunForEnvTag:env_value, but not found")
		} else {
			assert.Equal(t, "env_value", v)
		}
	}
}

func Test_RunForErrData(t *testing.T) {
	dir := "Test_Run"
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logpath := dir + "/logdir"
	logpathLink := dir + "/logdirlink"
	metapath := dir + "/meta_mock_csv"
	if err := os.Mkdir(logpath, DefaultDirPerm); err != nil {
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
	if err := os.Mkdir(metapath, DefaultDirPerm); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	log1 := `hello 123
	xx 1
	`
	log2 := `
`
	log3 := `h 456
	x 789`

	if err := ioutil.WriteFile(filepath.Join(logpath, "log1"), []byte(log1), 0666); err != nil {
		log.Fatalf("write log1 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log2"), []byte(log2), 0666); err != nil {
		log.Fatalf("write log2 fail %v", err)
	}
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log3"), []byte(log3), 0666); err != nil {
		log.Fatalf("write log3 fail %v", err)
	}

	exppath1 := filepath.Join(absLogpath, "log1")
	exppath3 := filepath.Join(absLogpath, "log3")
	exppaths := []string{exppath1, exppath1, "", exppath3, exppath3}
	rinfo := RunnerInfo{
		RunnerName:   "test_runner",
		MaxBatchLen:  1,
		MaxBatchSize: 2048,
	}
	readerConfig := conf.MapConf{
		"log_path":        logpathLink,
		"meta_path":       metapath,
		"mode":            "dir",
		"read_from":       "oldest",
		"datasource_tag":  "testtag",
		"reader_buf_size": "16",
	}
	meta, err := reader.NewMetaWithConf(readerConfig)
	if err != nil {
		t.Error(err)
	}
	isFromWeb := false
	reader, err := reader.NewFileBufReader(readerConfig, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	cleanChan := make(chan cleaner.CleanSignal)
	cleanerConfig := conf.MapConf{
		"delete_enable": "true",
	}
	cleaner, err := cleaner.NewCleaner(cleanerConfig, meta, cleanChan, meta.LogPath())
	if err != nil {
		t.Error(err)
	}
	parseConf := conf.MapConf{
		"name":                   "req_csv",
		"type":                   "csv",
		"csv_schema":             "logtype string, xx long",
		"csv_splitter":           " ",
		"disable_record_errdata": "false",
	}
	ps := parser.NewRegistry()
	pparser, err := ps.NewLogParser(parseConf)
	if err != nil {
		t.Error(err)
	}
	senderConfigs := []conf.MapConf{
		{
			"name":        "mock_sender",
			"sender_type": "mock",
		},
	}
	var senders []sender.Sender
	raws, err := mock.NewSender(senderConfigs[0])
	s, succ := raws.(*mock.Sender)
	if !succ {
		t.Error("sender should be mock sender")
	}
	if err != nil {
		t.Error(err)
	}
	senders = append(senders, s)

	r, err := NewLogExportRunnerWithService(rinfo, reader, cleaner, pparser, nil, senders, nil, meta)
	if err != nil {
		t.Error(err)
	}

	cleanInfo := CleanInfo{
		enable: true,
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
	var dts []Data
	rawData := r.senders[0].Name()[len("mock_sender "):]
	err = jsoniter.Unmarshal([]byte(rawData), &dts)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, len(dts), "got sender data not match")
	for idx, dt := range dts {
		if _, ok := dt[KeyPandoraStash]; ok {
			if dt["testtag"] == nil {
				t.Errorf("data source should be added")
			}
		} else {
			assert.Equal(t, exppaths[idx], dt["testtag"])
		}
	}
}

func Test_Compatible(t *testing.T) {
	rc := RunnerConfig{
		ReaderConfig: conf.MapConf{
			"log_path":       "/path1",
			"meta_path":      "meta",
			"mode":           "dir",
			"read_from":      "oldest",
			"datasource_tag": "testtag",
		},
		ParserConf: conf.MapConf{
			"type": "qiniulog",
		},
	}
	exprc := RunnerConfig{
		ReaderConfig: conf.MapConf{
			"log_path":       "/path1",
			"meta_path":      "meta",
			"mode":           "dir",
			"read_from":      "oldest",
			"datasource_tag": "testtag",
			"head_pattern":   "^" + qiniu.HeadPatthern,
		},
		ParserConf: conf.MapConf{
			"type": "qiniulog",
		},
	}
	rc = Compatible(rc)
	assert.Equal(t, exprc, rc)
	rc2 := RunnerConfig{
		ReaderConfig: conf.MapConf{
			"log_path":       "/path1",
			"meta_path":      "meta",
			"mode":           "dir",
			"read_from":      "oldest",
			"datasource_tag": "testtag",
		},
		ParserConf: conf.MapConf{
			"type":            "qiniulog",
			"qiniulog_prefix": "PREX",
		},
	}
	exprc2 := RunnerConfig{
		ReaderConfig: conf.MapConf{
			"log_path":       "/path1",
			"meta_path":      "meta",
			"mode":           "dir",
			"read_from":      "oldest",
			"datasource_tag": "testtag",
			"head_pattern":   "^PREX " + qiniu.HeadPatthern,
		},
		ParserConf: conf.MapConf{
			"type":            "qiniulog",
			"qiniulog_prefix": "PREX",
		},
	}
	rc2 = Compatible(rc2)
	assert.Equal(t, exprc2, rc2)
}

func Test_QiniulogRun(t *testing.T) {
	dir := "Test_QiniulogRun"
	//clean dir first
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Errorf("Test_QiniulogRun error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logpath := dir + "/logdir"
	logpathLink := dir + "/logdirlink"
	metapath := dir + "/meta_mock_csv"
	if err := os.Mkdir(logpath, DefaultDirPerm); err != nil {
		log.Errorf("Test_Run error mkdir %v %v", logpath, err)
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
	if err := os.Mkdir(metapath, DefaultDirPerm); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	log1 := `2017/01/22 11:16:08.885550 [X-ZsU][INFO] disk.go:123: [REQ_END] 200 0.010k 3.792ms
		[WARN][SLdoIrCDZj7pmZsU] disk.go <job.freezeDeamon> pop() failed: not found
2017/01/22 11:15:54.947217 [2pyKMukqvwSd-ZsU][INFO] disk.go:124: Service: POST 10.200.20.25:9100/user/info, Code: 200, Xlog: AC, Time: 1ms
`
	log2 := `2016/10/20 17:20:30.642666 [ERROR] disk.go:125: github.com/qiniu/logkit/queue/disk.go:241
	1234 3243xsaxs
2016/10/20 17:20:30.642662 [123][WARN] disk.go:241: github.com/qiniu/logkit/queue/disk.go 1
`
	log3 := `2016/10/20 17:20:30.642662 [124][WARN] disk.go:456: xxxxxx`
	expfiles := []string{`[REQ_END] 200 0.010k 3.792ms
		[WARN][SLdoIrCDZj7pmZsU] disk.go <job.freezeDeamon> pop() failed: not found`,
		`Service: POST 10.200.20.25:9100/user/info, Code: 200, Xlog: AC, Time: 1ms`,
		`github.com/qiniu/logkit/queue/disk.go:241
	1234 3243xsaxs`, `github.com/qiniu/logkit/queue/disk.go 1`,
		`xxxxxx`}
	expreqid := []string{"X-ZsU", "2pyKMukqvwSd-ZsU", "", "123", "124"}
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
		"log_path":       logpathLink,
		"meta_path":      metapath,
		"mode":           "dir",
		"read_from":      "oldest",
		"datasource_tag": "testtag",
	}
	parseConf := conf.MapConf{
		"name": "qiniu",
		"type": parserConf.TypeLogv1,
	}
	senderConfigs := []conf.MapConf{
		{
			"name":        "mock_sender",
			"sender_type": "mock",
		},
	}

	rc := RunnerConfig{
		RunnerInfo:    rinfo,
		ReaderConfig:  readerConfig,
		ParserConf:    parseConf,
		SendersConfig: senderConfigs,
		IsInWebFolder: false,
	}
	rc = Compatible(rc)
	meta, err := reader.NewMetaWithConf(rc.ReaderConfig)
	if err != nil {
		t.Error(err)
	}
	r, err := reader.NewFileBufReader(rc.ReaderConfig, rc.IsInWebFolder)
	if err != nil {
		t.Error(err)
	}
	ps := parser.NewRegistry()
	pparser, err := ps.NewLogParser(parseConf)
	if err != nil {
		t.Error(err)
	}

	var senders []sender.Sender
	raws, err := mock.NewSender(senderConfigs[0])
	s, succ := raws.(*mock.Sender)
	if !succ {
		t.Error("sender should be mock sender")
	}
	if err != nil {
		t.Error(err)
	}
	senders = append(senders, s)

	runner, err := NewLogExportRunnerWithService(rinfo, r, nil, pparser, nil, senders, nil, meta)
	if err != nil {
		t.Error(err)
	}

	go runner.Run()
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log3"), []byte(log3), 0666); err != nil {
		log.Fatalf("write log3 fail %v", err)
	}
	time.Sleep(time.Second)
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
	var dts []Data
	rawData := runner.senders[0].Name()[len("mock_sender "):]
	err = jsoniter.Unmarshal([]byte(rawData), &dts)
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 5 {
		t.Errorf("got sender data not match error,expect 5 but %v", len(dts))
	}
	for idx, dt := range dts {
		assert.Equal(t, expfiles[idx], dt["log"], "equl log test")
		if expreqid[idx] == "" {
			assert.Nil(t, dt["reqid"])
		} else {
			assert.Equal(t, expreqid[idx], dt["reqid"], "equal reqid test")
		}
	}
	ls, err := runner.LagStats()
	assert.NoError(t, err)
	assert.Equal(t, &LagInfo{0, "bytes", 0, 0}, ls)
}

func TestCreateTransforms(t *testing.T) {
	config1 := `{
		"name":"test2.csv",
		"reader":{
			"log_path":"./tests/logdir",
			"mode":"dir"
		},
		"parser":{
			"name":"test2_csv_parser",
			"type":"csv",
			"csv_schema":"t1 string"
		},
		"transforms":[{
			"type":"IP",
			"key":  "ip",
			"data_path": "../transforms/ip/test_data/17monipdb.dat"
		}],
		"senders":[{
			"name":"file_sender",
			"sender_type":"file",
			"file_send_path":"./test2/test2_csv_file.txt"
		}]
	}`

	rc := RunnerConfig{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers, _ := createTransformers(rc)
	datas := []Data{{"ip": "111.2.3.4"}}
	exp := []Data{{
		"ip":      "111.2.3.4",
		"Region":  "浙江",
		"City":    "宁波",
		"Country": "中国",
		"Isp":     "N/A"}}
	for k := range transformers {
		datas, err = transformers[k].Transform(datas)
		assert.NoError(t, err)
	}
	assert.Equal(t, exp, datas)
}

func TestReplaceTransforms(t *testing.T) {

	config1 := `{
		"name":"test2.csv",
		"reader":{
			"log_path":"./tests/logdir",
			"mode":"dir"
		},
		"parser":{
			"name":"jsonps",
			"type":"json"
		},
		"transforms":[{
			"type":"replace",
			"stage":"before_parser",
			"old":"\\x",
			"new":"\\\\x"
		}],
		"senders":[{
			"name":"file_sender",
			"sender_type":"file",
			"file_send_path":"./test2/test2_csv_file.txt"
		}]
	}`
	newData := make([]Data, 0)
	rc := RunnerConfig{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers, _ := createTransformers(rc)
	datas := []string{`{"status":"200","request_method":"POST","request_body":"<xml>\x0A","content_type":"text/xml"}`, `{"status":"200","request_method":"POST","request_body":"<xml>x0A","content_type":"text/xml"}`}
	for k := range transformers {
		datas, err = transformers[k].RawTransform(datas)
		assert.NoError(t, err)
		for i := range datas {
			var da Data
			err = jsoniter.Unmarshal([]byte(datas[i]), &da)
			assert.NoError(t, err)
			newData = append(newData, da)
		}
	}
	exp := []Data{
		{
			"status":         "200",
			"request_method": "POST",
			"request_body":   "<xml>\\x0A",
			"content_type":   "text/xml",
		},
		{
			"status":         "200",
			"request_method": "POST",
			"request_body":   "<xml>x0A",
			"content_type":   "text/xml",
		},
	}
	assert.Equal(t, exp, newData)
}

func TestDateTransforms(t *testing.T) {

	config1 := `{
		"name":"test2.csv",
		"reader":{
			"log_path":"./tests/logdir",
			"mode":"dir"
		},
		"parser":{
			"name":"jsonps",
			"type":"json"
		},
		"transforms":[{
			"type":"date",
			"key":"status",
			"offset":1,
			"time_layout_before":"",
			"time_layout_after":"2006-01-02T15:04:05"
		}],
		"senders":[{
			"name":"file_sender",
			"sender_type":"file",
			"file_send_path":"./test2/test2_csv_file.txt"
		}]
	}`
	rc := RunnerConfig{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers, _ := createTransformers(rc)
	datas := []Data{{"status": "02/01/2016--15:04:05"}, {"status": "2006-01-02 15:04:15"}}
	for k := range transformers {
		datas, err = transformers[k].Transform(datas)
	}
	exp := []Data{
		{
			"status": "2016-01-02T16:04:05",
		},
		{
			"status": "2006-01-02T16:04:15",
		},
	}
	assert.Equal(t, exp, datas)
}

func TestSplitAndConvertTransforms(t *testing.T) {

	config1 := `{
		"name":"test2.csv",
		"reader":{
			"log_path":"./tests/logdir",
			"mode":"dir"
		},
		"parser":{
			"name":"jsonps",
			"type":"json"
		},
		"transforms":[{
			"type":"split",
			"key":"status",
			"sep":",",
			"newfield":"newarray"
		},{
			"type":"convert",
			"dsl":"newarray array(long)"
		}],
		"senders":[{
			"name":"file_sender",
			"sender_type":"file",
			"file_send_path":"./test2/test2_csv_file.txt"
		}]
	}`
	rc := RunnerConfig{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers, _ := createTransformers(rc)
	datas := []Data{{"status": "1,2,3"}, {"status": "4,5,6"}}
	for k := range transformers {
		datas, err = transformers[k].Transform(datas)
	}
	exp := []Data{
		{
			"status":   "1,2,3",
			"newarray": []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			"status":   "4,5,6",
			"newarray": []interface{}{int64(4), int64(5), int64(6)},
		},
	}
	assert.Equal(t, exp, datas)
}

func TestGetTrend(t *testing.T) {
	assert.Equal(t, SpeedUp, getTrend(0, 1))
	assert.Equal(t, SpeedDown, getTrend(1, 0))
	assert.Equal(t, SpeedStable, getTrend(1, 1))
}

func TestSpeedTrend(t *testing.T) {
	tests := []struct {
		olds  StatsInfo
		news  StatsInfo
		etime float64
		exp   StatsInfo
	}{
		{
			olds: StatsInfo{
				Success: 1,
				Speed:   1.0,
			},
			news: StatsInfo{
				Success: 2,
			},
			etime: 1.0,
			exp: StatsInfo{
				Success: 2,
				Speed:   1.0,
				Trend:   SpeedStable,
			},
		},
		{
			olds:  StatsInfo{},
			news:  StatsInfo{},
			etime: 0,
			exp: StatsInfo{
				Success: 0,
				Speed:   0,
				Trend:   SpeedStable,
			},
		},
		{
			olds: StatsInfo{
				Success: 1,
				Speed:   1.0,
			},
			news: StatsInfo{
				Success: 10,
			},
			etime: 1.0,
			exp: StatsInfo{
				Success: 10,
				Speed:   9.0,
				Trend:   SpeedUp,
			},
		},
		{
			olds: StatsInfo{
				Success: 10,
				Speed:   10.0,
			},
			news: StatsInfo{
				Success: 11,
			},
			etime: 1.0,
			exp: StatsInfo{
				Success: 11,
				Speed:   1.0,
				Trend:   SpeedDown,
			},
		},
	}
	for _, ti := range tests {
		ti.news.Speed, ti.news.Trend = calcSpeedTrend(ti.olds, ti.news, ti.etime)
		assert.Equal(t, ti.exp, ti.news)
	}
}

func TestCopyStats(t *testing.T) {
	tests := []struct {
		src RunnerStatus
		dst RunnerStatus
		exp RunnerStatus
	}{
		{
			src: RunnerStatus{
				ReadDataSize:  10,
				ReadDataCount: 10,
				SenderStats: map[string]StatsInfo{
					"a": {
						Success: 11,
						Speed:   1.0,
						Trend:   SpeedDown,
					},
					"c": {
						Success: 12,
						Speed:   1.0,
						Trend:   SpeedDown,
					},
				},
				TransformStats: map[string]StatsInfo{
					"x": {
						Success: 2,
						Speed:   5.0,
						Trend:   SpeedDown,
					},
				},
				ReadSpeedKB: 10,
				ReadSpeed:   10,
			},
			exp: RunnerStatus{
				ReadDataSize:  10,
				ReadDataCount: 10,
				SenderStats: map[string]StatsInfo{
					"a": {
						Success: 11,
						Speed:   1.0,
						Trend:   SpeedDown,
					},
					"c": {
						Success: 12,
						Speed:   1.0,
						Trend:   SpeedDown,
					},
				},
				TransformStats: map[string]StatsInfo{
					"x": {
						Success: 2,
						Speed:   5.0,
						Trend:   SpeedDown,
					},
				},
				ReadSpeedKB: 10,
				ReadSpeed:   10,
			},
			dst: RunnerStatus{
				ReadDataSize:  5,
				ReadDataCount: 0,
				SenderStats: map[string]StatsInfo{
					"x": {
						Success: 0,
						Speed:   2.0,
						Trend:   SpeedDown,
					},
					"b": {
						Success: 5,
						Speed:   1.0,
						Trend:   SpeedDown,
					},
				},
				TransformStats: map[string]StatsInfo{
					"s": {
						Success: 21,
						Speed:   50.0,
						Trend:   SpeedUp,
					},
				},
				ReadSpeedKB: 11,
				ReadSpeed:   2,
			},
		},
	}
	for _, ti := range tests {
		ti.dst = (&ti.src).Clone()
		for i, v := range ti.src.SenderStats {
			v.Speed = 0
			v.Success = 0
			ti.src.SenderStats[i] = v
		}
		assert.Equal(t, ti.exp, ti.dst)
	}
}

func TestSyslogRunnerX(t *testing.T) {
	metaDir := "TestSyslogRunner"

	os.Mkdir(metaDir, DefaultDirPerm)
	defer os.RemoveAll(metaDir)

	config := `{
		"name":"TestSyslogRunner",
		"batch_len":1,
		"reader":{
			"mode":"socket",
			"meta_path":"TestSyslogRunner",
			"socket_service_address":"tcp://:5142"
		},
		"parser":{
			"name":"syslog",
			"type":"raw"
		},
		"senders":[{
			"name":"file_sender",
			"sender_type":"file",
			"file_send_path":"./TestSyslogRunner/syslog.txt"
		}]
	}`

	rc := RunnerConfig{}
	err := jsoniter.Unmarshal([]byte(config), &rc)
	assert.NoError(t, err)
	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	go rr.Run()
	time.Sleep(1 * time.Second)
	sysLog, err := syslog.Dial("tcp", "127.0.0.1:5142",
		syslog.LOG_WARNING|syslog.LOG_DAEMON, "demotag")
	if err != nil {
		log.Fatal(err)
	}
	err = sysLog.Emerg("And this is a daemon emergency with demotag.")
	assert.NoError(t, err)
	err = sysLog.Emerg("this is OK")
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	data, err := ioutil.ReadFile("./TestSyslogRunner/syslog.txt")
	assert.NoError(t, err)
	if !strings.Contains(string(data), "this is OK") || !strings.Contains(string(data), "And this is a daemon emergency with demotag.") {
		t.Error("syslog parse error")
	}
}

func TestAddDatasource(t *testing.T) {
	sourceFroms := []string{"a", "b", "c", "d", "e", "f"}
	se := &StatsError{
		DatasourceSkipIndex: []int{0, 3, 5},
	}
	datas := []Data{
		{
			"f1": "2",
		},
		{
			"f2": "1",
		},
		{
			"f3": "3",
		},
	}
	datasourceTagName := "source"
	runnername := "runner1"
	exp := []Data{
		{
			"f1":     "2",
			"source": "b",
		},
		{
			"f2":     "1",
			"source": "c",
		},
		{
			"f3":     "3",
			"source": "e",
		},
	}
	gots := addSourceToData(sourceFroms, se, datas, datasourceTagName, runnername)
	assert.Equal(t, exp, gots)
	se = nil
	exp = []Data{
		{
			"f1":     "2",
			"source": "a",
		},
		{
			"f2":     "1",
			"source": "b",
		},
		{
			"f3":     "3",
			"source": "c",
		},
	}
	datas = []Data{
		{
			"f1": "2",
		},
		{
			"f2": "1",
		},
		{
			"f3": "3",
		},
	}
	gots = addSourceToData(sourceFroms, se, datas, datasourceTagName, runnername)
	assert.Equal(t, exp, gots)

}

func TestAddDatasourceForErrData(t *testing.T) {
	sourceFroms := []string{"a", "b", "c", "d", "e", "f"}
	se := &StatsError{
		DatasourceSkipIndex: []int{0, 3, 5},
	}
	datas := []Data{
		{
			"pandora_stash": "rawdata1",
		},
		{
			"f1": "2",
		},
		{
			"f2": "1",
		},
		{
			"pandora_stash": "rawdata2",
		},
		{
			"f3": "3",
		},
		{
			"pandora_stash": "rawdata3",
		},
	}
	datasourceTagName := "source"
	runnername := "runner1"
	exp := []Data{
		{
			"pandora_stash": "rawdata1",
			"source":        "a",
		},
		{
			"f1":     "2",
			"source": "b",
		},
		{
			"f2":     "1",
			"source": "c",
		},
		{
			"pandora_stash": "rawdata2",
			"source":        "d",
		},
		{
			"f3":     "3",
			"source": "e",
		},
		{
			"pandora_stash": "rawdata3",
			"source":        "f",
		},
	}
	gots := addSourceToData(sourceFroms, se, datas, datasourceTagName, runnername)
	assert.Equal(t, exp, gots)
}

func TestAddDatasourceForRawData(t *testing.T) {
	dir := "TestAddDatasource"
	metaDir := filepath.Join(dir, "meta")
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("TestAddDatasource error mkdir %v %v", dir, err)
	}
	filename := []string{"test1.log", "test2.log", "test3.log", "test4.log"}
	content := []string{"1 fufu 3.14\n", "3 fufu 3.16\n", "hfdjsak,dadiajd,dsaud\n", "4 fufu 3.17\n"}
	var realPaths []string
	for i := range filename {
		logPath := filepath.Join(dir, filename[i])
		readPath, err := filepath.Abs(logPath)
		assert.NoError(t, err)
		realPaths = append(realPaths, readPath)
		err = ioutil.WriteFile(logPath, []byte(content[i]), DefaultDirPerm)
		assert.NoError(t, err)
	}

	defer os.RemoveAll(dir)
	defer os.RemoveAll(metaDir)

	config1 := `{
			"name":"TestAddDatasource",
			"batch_len":4,
			"reader":{
				"mode":"dir",
				"log_path":"./TestAddDatasource/",
				"datasource_tag":"datasource"
			},
			"parser":{
				"name":"testcsv",
				"type":"csv",
				"csv_schema":"a long,b string,c float",
				"csv_splitter":" ",
				"disable_record_errdata":"true",
				"keep_raw_data":"true"
			},
			"senders":[{
				"name":"file_sender",
				"sender_type":"file",
				"file_send_path":"./TestAddDatasource/filesend.csv"
			}]
		}`
	rc := RunnerConfig{}
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	go rr.Run()

	time.Sleep(2 * time.Second)
	data, err := ioutil.ReadFile("./TestAddDatasource/filesend.csv")
	var res []Data
	err = jsoniter.Unmarshal(data, &res)
	if err != nil {
		t.Error(err)
	}
	exp := []Data{
		{
			"c":          float64(3.14),
			"raw_data":   content[0],
			"datasource": realPaths[0],
			"a":          float64(1),
			"b":          "fufu",
		},
		{
			"a":          float64(3),
			"b":          "fufu",
			"c":          float64(3.16),
			"raw_data":   content[1],
			"datasource": realPaths[1],
		},
		{
			"raw_data":   content[2],
			"datasource": realPaths[2],
		},
		{
			"b":          "fufu",
			"c":          float64(3.17),
			"raw_data":   content[3],
			"datasource": realPaths[3],
			"a":          float64(4),
		},
	}
	assert.Equal(t, exp, res)
}

func TestAddDatatags(t *testing.T) {
	dir := "TestAddDatatags"
	metaDir := filepath.Join(dir, "meta")
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("TestAddDatatags error mkdir %v %v", dir, err)
	}
	tagFile := filepath.Join(dir, "tagFile.json")
	err := ioutil.WriteFile(tagFile, []byte(`{  
	   	"Title":"tags",
	    "Author":["john","ada","alice"],
	    "IsTrue":true,
	    "Host":99
	  	}`), DefaultDirPerm)
	assert.NoError(t, err)
	logPath := filepath.Join(dir, "test.log")
	err = ioutil.WriteFile(logPath, []byte(`{"f1": "2","f2": "1","f3": "3"}`), DefaultDirPerm)
	assert.NoError(t, err)

	defer os.RemoveAll(dir)
	defer os.RemoveAll(metaDir)

	config1 := `{
			"name":"TestAddDatatags",
			"batch_len":1,
			"reader":{
				"mode":"file",
				"meta_path":"./TestAddDatatags/meta",
				"file_done":"./TestAddDatatags/meta",
				"log_path":"./TestAddDatatags/test.log",
				"tag_file":"./TestAddDatatags/tagFile.json"
			},
			"parser":{
				"name":"testjson",
				"type":"json"
			},
			"senders":[{
				"name":"file_sender",
				"sender_type":"file",
				"file_send_path":"./TestAddDatatags/filesend.json"
			}]
		}`
	rc := RunnerConfig{}
	err = jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	go rr.Run()

	time.Sleep(2 * time.Second)
	data, err := ioutil.ReadFile("./TestAddDatatags/filesend.json")
	var res []Data
	err = jsoniter.Unmarshal(data, &res)
	if err != nil {
		t.Error(err)
	}
	exp := []Data{
		{
			"f1":     "2",
			"f2":     "1",
			"f3":     "3",
			"Title":  "tags",
			"Author": []interface{}{"john", "ada", "alice"},
			"IsTrue": bool(true),
			"Host":   float64(99),
		},
	}
	assert.Equal(t, exp, res)
}

func TestRunWithExtra(t *testing.T) {
	dir := "TestRunWithExtra"
	metaDir := filepath.Join(dir, "meta")
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("TestRunWithExtra error mkdir %v %v", dir, err)
	}
	logPath := filepath.Join(dir, "test.log")
	err := ioutil.WriteFile(logPath, []byte(`{"f1": "2","f2": "1","f3": "3"}`), DefaultDirPerm)
	assert.NoError(t, err)

	defer os.RemoveAll(dir)
	defer os.RemoveAll(metaDir)

	config1 := `{
			"name":"TestRunWithExtra",
			"batch_len":1,
			"extra_info":true,
			"reader":{
				"mode":"file",
				"meta_path":"./TestRunWithExtra/meta",
				"log_path":"./TestRunWithExtra/test.log"
			},
			"parser":{
				"name":"testjson",
				"type":"json"
			},
			"senders":[{
				"name":"file_sender",
				"sender_type":"file",
				"file_send_path":"./TestRunWithExtra/filesend.json"
			}]
		}`
	rc := RunnerConfig{}
	err = jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)

	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	go rr.Run()

	time.Sleep(2 * time.Second)
	data, err := ioutil.ReadFile("./TestRunWithExtra/filesend.json")
	var res []Data
	err = jsoniter.Unmarshal(data, &res)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 7, len(res[0]))
}

func TestRunWithDataSource(t *testing.T) {
	cur, err := os.Getwd()
	assert.NoError(t, err)
	dir := filepath.Join(cur, "TestRunWithDataSource")
	os.RemoveAll(dir)
	metaDir := filepath.Join(dir, "meta")
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("TestRunWithDataSource error mkdir %v %v", dir, err)
	}
	logPath := filepath.Join(dir, "test.log")
	err = ioutil.WriteFile(logPath, []byte("a\nb\n\n\nc\n"), DefaultDirPerm)
	assert.NoError(t, err)

	defer os.RemoveAll(dir)
	defer os.RemoveAll(metaDir)

	config1 := `{
			"name":"TestRunWithDataSource",
			"batch_len":5,
			"reader":{
				"mode":"file",
				"meta_path":"./TestRunWithDataSource/meta",
				"log_path":"` + logPath + `",
				"datasource_tag":"datasource"
			},
			"parser":{
				"name":"testjson",
				"type":"raw",
				"timestamp":"false"
			},
			"senders":[{
				"name":"file_sender",
				"sender_type":"file",
				"file_send_path":"./TestRunWithDataSource/filesend.json"
			}]
		}`
	rc := RunnerConfig{}
	err = jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	go rr.Run()

	time.Sleep(2 * time.Second)
	data, err := ioutil.ReadFile("./TestRunWithDataSource/filesend.json")
	var res []Data
	err = json.Unmarshal(data, &res)
	if err != nil {
		t.Error(err, string(data))
	}
	exp := []Data{
		{
			"raw":        "a\n",
			"datasource": logPath,
		},
		{
			"raw":        "b\n",
			"datasource": logPath,
		},
		{
			"raw":        "c\n",
			"datasource": logPath,
		},
	}
	assert.Equal(t, exp, res)
}

func TestRunWithDataSourceFial(t *testing.T) {
	cur, err := os.Getwd()
	assert.NoError(t, err)
	dir := filepath.Join(cur, "TestRunWithDataSourceFial")
	metaDir := filepath.Join(dir, "meta")
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("TestRunWithDataSource error mkdir %v %v", dir, err)
	}
	logPath := filepath.Join(dir, "test.log")
	err = ioutil.WriteFile(logPath, []byte("a\n"), DefaultDirPerm)
	assert.NoError(t, err)

	defer os.RemoveAll(dir)
	defer os.RemoveAll(metaDir)

	config1 := `{
			"name":"TestRunWithDataSourceFial",
			"batch_len":1,
			"reader":{
				"mode":"file",
				"log_path":"` + logPath + `",
				"meta_path":"./TestRunWithDataSourceFial/meta",
				"datasource_tag":"datasource"
			},
			"parser":{
				"name":"testjson",
				"type":"json"
			},
			"senders":[{
				"name":"file_sender",
				"sender_type":"file",
				"file_send_path":"./TestRunWithDataSourceFial/filesend.json"
			}]
		}`
	rc := RunnerConfig{}
	err = jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	go rr.Run()

	time.Sleep(2 * time.Second)
	data, err := ioutil.ReadFile("./TestRunWithDataSourceFial/filesend.json")
	var res []Data
	err = json.Unmarshal(data, &res)
	if err != nil {
		t.Error(err, string(data))
	}
	exp := []Data{
		{
			"pandora_stash": "a",
			"datasource":    logPath,
		},
	}
	assert.Equal(t, exp, res)
}

func TestClassifySenderData(t *testing.T) {
	{
		senders := []sender.Sender{&mock.Sender{}, &mock.Sender{}, &mock.Sender{}}
		numSenders := len(senders)
		datas := []Data{
			{
				"a": "a",
				"b": "b",
				"c": "c",
				"d": "d",
			},
			{
				"a": "A",
				"b": "b",
				"c": "c",
				"d": "d",
			},
			{
				"a": "B",
				"b": "b",
				"c": "c",
				"d": "d",
			},
			{
				"a": "C",
				"b": "b",
				"c": "c",
				"d": "d",
			},
		}

		routerConf := router.RouterConfig{
			KeyName:      "a",
			MatchType:    "equal",
			DefaultIndex: 0,
			Routes: map[string]int{
				"a": 2,
				"A": 1,
			},
		}

		r, err := router.NewSenderRouter(routerConf, numSenders)

		senderDataList := classifySenderData(senders, datas, r)
		assert.Equal(t, numSenders, len(senderDataList))
		assert.Equal(t, 2, len(senderDataList[0]))
		assert.Equal(t, 1, len(senderDataList[1]))
		assert.Equal(t, 1, len(senderDataList[2]))

		// 测试没有配置 router 的情况
		routerConf.KeyName = ""
		r, err = router.NewSenderRouter(routerConf, numSenders)
		assert.Nil(t, r)
		assert.NoError(t, err)
		senderDataList = classifySenderData(senders, datas, r)
		assert.Equal(t, numSenders, len(senderDataList))
		assert.Equal(t, 4, len(senderDataList[0]))
		assert.Equal(t, 4, len(senderDataList[1]))
		assert.Equal(t, 4, len(senderDataList[2]))
	}

	// --> 测试 SkipDeepCopySender 检查是否生效 <--

	// 存在数据改动的 sender 后有其它 sender
	{
		senders := []sender.Sender{&mock.Sender{}, &pandora.Sender{}, &mock.Sender{}}
		datas := []Data{
			{
				"a": "a",
				"b": "b",
				"c": "c",
				"d": "d",
			},
		}
		senderDataList := classifySenderData(senders, datas, nil)
		assert.Len(t, senderDataList, len(senders))
		assert.True(t, fmt.Sprintf("%p", datas) == fmt.Sprintf("%p", senderDataList[0]))
		assert.False(t, fmt.Sprintf("%p", datas) == fmt.Sprintf("%p", senderDataList[1]))
		assert.True(t, fmt.Sprintf("%p", datas) == fmt.Sprintf("%p", senderDataList[2]))
	}
	// 存在数据改动的 sender 为最后一个
	{
		senders := []sender.Sender{&mock.Sender{}, &pandora.Sender{}}
		datas := []Data{
			{
				"a": "a",
				"b": "b",
				"c": "c",
				"d": "d",
			},
		}
		senderDataList := classifySenderData(senders, datas, nil)
		assert.Len(t, senderDataList, len(senders))
		assert.True(t, fmt.Sprintf("%p", datas) == fmt.Sprintf("%p", senderDataList[0]))
		assert.True(t, fmt.Sprintf("%p", datas) == fmt.Sprintf("%p", senderDataList[1]))
	}
	// 仅存在数据改动的 sender
	{
		senders := []sender.Sender{&pandora.Sender{}}
		datas := []Data{
			{
				"a": "a",
				"b": "b",
				"c": "c",
				"d": "d",
			},
		}
		senderDataList := classifySenderData(senders, datas, nil)
		assert.Len(t, senderDataList, len(senders))
		assert.True(t, fmt.Sprintf("%p", datas) == fmt.Sprintf("%p", senderDataList[0]))
	}
}

// Reponse from Clearbit API. Size: 2.4kb
var mediumFixture []byte = []byte(`{
  "person": {
    "id": "d50887ca-a6ce-4e59-b89f-14f0b5d03b03",
    "name": {
      "fullName": "Leonid Bugaev",
      "givenName": "Leonid",
      "familyName": "Bugaev"
    },
    "email": "leonsbox@gmail.com",
    "gender": "male",
    "location": "Saint Petersburg, Saint Petersburg, RU",
    "geo": {
      "city": "Saint Petersburg",
      "state": "Saint Petersburg",
      "country": "Russia",
      "lat": 59.9342802,
      "lng": 30.3350986
    },
    "bio": "Senior engineer at Granify.com",
    "site": "http://flickfaver.com",
    "avatar": "https://d1ts43dypk8bqh.cloudfront.net/v1/avatars/d50887ca-a6ce-4e59-b89f-14f0b5d03b03",
    "employment": {
      "name": "www.latera.ru",
      "title": "Software Engineer",
      "domain": "gmail.com"
    },
    "facebook": {
      "handle": "leonid.bugaev"
    },
    "github": {
      "handle": "buger",
      "id": 14009,
      "avatar": "https://avatars.githubusercontent.com/u/14009?v=3",
      "company": "Granify",
      "blog": "http://leonsbox.com",
      "followers": 95,
      "following": 10
    },
    "twitter": {
      "handle": "flickfaver",
      "id": 77004410,
      "bio": null,
      "followers": 2,
      "following": 1,
      "statuses": 5,
      "favorites": 0,
      "location": "",
      "site": "http://flickfaver.com",
      "avatar": null
    },
    "linkedin": {
      "handle": "in/leonidbugaev"
    },
    "googleplus": {
      "handle": null
    },
    "angellist": {
      "handle": "leonid-bugaev",
      "id": 61541,
      "bio": "Senior engineer at Granify.com",
      "blog": "http://buger.github.com",
      "site": "http://buger.github.com",
      "followers": 41,
      "avatar": "https://d1qb2nb5cznatu.cloudfront.net/users/61541-medium_jpg?1405474390"
    },
    "klout": {
      "handle": null,
      "score": null
    },
    "foursquare": {
      "handle": null
    },
    "aboutme": {
      "handle": "leonid.bugaev",
      "bio": null,
      "avatar": null
    },
    "gravatar": {
      "handle": "buger",
      "urls": [
      ],
      "avatar": "http://1.gravatar.com/avatar/f7c8edd577d13b8930d5522f28123510",
      "avatars": [
        {
          "url": "http://1.gravatar.com/avatar/f7c8edd577d13b8930d5522f28123510",
          "type": "thumbnail"
        }
      ]
    },
    "fuzzy": false
  },
  "company": null
}`)

type CBAvatar struct {
	Url string `json:"url"`
}

type CBGravatar struct {
	Avatars []*CBAvatar `json:"avatars"`
}

type CBGithub struct {
	Followers int `json:"followers"`
}

type CBName struct {
	FullName string `json:"fullName"`
}

type CBPerson struct {
	Name     *CBName     `json:"name"`
	Github   *CBGithub   `json:"github"`
	Gravatar *CBGravatar `json:"gravatar"`
}

type MediumPayload struct {
	Person  *CBPerson `json:"person"`
	Company string    `json:"compnay"`
}

func BenchmarkDecodeStdStructMedium(b *testing.B) {
	b.ReportAllocs()
	var data MediumPayload
	for i := 0; i < b.N; i++ {
		jsoniter.Unmarshal(mediumFixture, &data)
	}
}

func BenchmarkEncodeStdStructMedium(b *testing.B) {
	var data MediumPayload
	jsoniter.Unmarshal(mediumFixture, &data)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		jsoniter.Marshal(data)
	}
}

func BenchmarkDecodeJsoniterStructMedium(b *testing.B) {
	b.ReportAllocs()
	var data MediumPayload
	for i := 0; i < b.N; i++ {
		jsoniter.Unmarshal(mediumFixture, &data)
	}
}

func BenchmarkEncodeJsoniterStructMedium(b *testing.B) {
	var data MediumPayload
	jsoniter.Unmarshal(mediumFixture, &data)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		jsoniter.Marshal(data)
	}
}

func BenchmarkEncodeJsoniterCompatibleStructMedium(b *testing.B) {
	var data MediumPayload
	jsoniter.Unmarshal(mediumFixture, &data)
	b.ReportAllocs()
	jsonc := jsoniter.ConfigCompatibleWithStandardLibrary
	for i := 0; i < b.N; i++ {
		jsonc.Marshal(data)
	}
}

/*
BenchmarkDecodeStdStructMedium-4                  	   50000	     39162 ns/op	    1960 B/op	      99 allocs/op
BenchmarkEncodeStdStructMedium-4                  	 1000000	      2106 ns/op	     712 B/op	       5 allocs/op
BenchmarkDecodeJsoniterStructMedium-4             	  200000	      7676 ns/op	     320 B/op	      36 allocs/op
BenchmarkEncodeJsoniterStructMedium-4             	 1000000	      1046 ns/op	     240 B/op	       3 allocs/op
BenchmarkEncodeJsoniterCompatibleStructMedium-4   	 1000000	      1023 ns/op	     240 B/op	       3 allocs/op
PASS
性能明显提升
*/

func TestMergeEnvTags(t *testing.T) {
	key := "TestMergeEnvTags"
	os.Setenv(key, `{"a":"hello"}`)
	defer os.Unsetenv(key)
	tags := MergeEnvTags(key, nil)
	assert.Equal(t, map[string]interface{}{"a": "hello"}, tags)

	os.Setenv(key, `{"b":"123","c":"nihao"}`)
	tags = MergeEnvTags(key, tags)
	assert.Equal(t, map[string]interface{}{"a": "hello", "b": "123", "c": "nihao"}, tags)

}

func TestMergeExtraInfoTags(t *testing.T) {
	meta, err := reader.NewMetaWithConf(conf.MapConf{
		ExtraInfo:          "true",
		readerConf.KeyMode: readerConf.ModeMySQL,
	})
	assert.NoError(t, err)
	tags := MergeExtraInfoTags(meta, nil)
	assert.Equal(t, 4, len(tags))
	//再次写入，应该不会产生变化。
	tags = MergeExtraInfoTags(meta, tags)
	assert.Equal(t, 4, len(tags))
}

func TestTailxCleaner(t *testing.T) {
	cur, err := os.Getwd()
	assert.NoError(t, err)
	dir := filepath.Join(cur, "TestTailxCleaner")
	metaDir := filepath.Join(dir, "meta")
	os.RemoveAll(dir)
	if err := os.Mkdir(dir, DefaultDirPerm); err != nil {
		log.Fatalf("TestTailxCleaner error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)

	dira := filepath.Join(dir, "a")
	os.MkdirAll(dira, DefaultDirPerm)
	logPatha := filepath.Join(dira, "a.log")
	assert.NoError(t, ioutil.WriteFile(logPatha, []byte("a\n"), 0666))

	dirb := filepath.Join(dir, "b")
	os.MkdirAll(dirb, DefaultDirPerm)
	logPathb := filepath.Join(dirb, "b.log")
	assert.NoError(t, ioutil.WriteFile(logPathb, []byte("b\n"), 0666))

	readfile := filepath.Join(dir, "*", "*.log")
	config := `
{
  "name": "TestTailxCleaner",
  "batch_size": 2097152,
  "batch_interval": 1,
  "reader": {
    "expire": "24h",
    "log_path": "` + readfile + `",
	"meta_path":"` + metaDir + `",
    "mode": "tailx",
    "read_from": "oldest",
    "stat_interval": "1s"
  },
  "cleaner": {
    "delete_enable": "true",
    "delete_interval": "1",
    "reserve_file_number": "1",
    "reserve_file_size": "2048"
  },
  "parser": {
    "disable_record_errdata": "false",
    "timestamp": "true",
    "type": "raw"
  },
  "senders": [
    {
      "sender_type": "discard"
    }
  ]
}`

	rc := RunnerConfig{}
	assert.NoError(t, jsoniter.Unmarshal([]byte(config), &rc))
	cleanChan := make(chan cleaner.CleanSignal)
	rr, err := NewLogExportRunner(rc, cleanChan, reader.NewRegistry(), parser.NewRegistry(), sender.NewRegistry())
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	go rr.Run()

	time.Sleep(2 * time.Second)

	logPatha1 := filepath.Join(dira, "a.log.1")
	assert.NoError(t, os.Rename(logPatha, logPatha1))

	assert.NoError(t, ioutil.WriteFile(logPatha, []byte("bbbb\n"), 0666))

	time.Sleep(5 * time.Second)

	logPatha2 := filepath.Join(dira, "a.log.2")
	assert.NoError(t, os.Rename(logPatha, logPatha2))

	assert.NoError(t, ioutil.WriteFile(logPatha, []byte("cccc\n"), 0666))

	time.Sleep(2 * time.Second)

	assert.NotNil(t, rr.Cleaner())

	var ret, dft int
DONE:
	for {
		select {
		case sig := <-cleanChan:
			ret++
			assert.Equal(t, "a.log.1", sig.Filename)
			assert.NoError(t, os.Remove(filepath.Join(sig.Logdir, sig.Filename)))
			assert.Equal(t, readerConf.ModeTailx, sig.ReadMode)
			break DONE
		default:
			dft++

		}
		time.Sleep(50 * time.Millisecond)
		if dft > 100 {
			break
		}
	}
	assert.Equal(t, 1, ret)
}

func Test_setSenderConfig(t *testing.T) {
	senderConfig := conf.MapConf{
		senderConf.KeySenderType: senderConf.TypePandora,
	}

	serverConfigs := []map[string]interface{}{
		{
			transforms.KeyType:     ip.Name,
			transforms.TransformAt: ip.Server,
		},
	}
	actualConfig, err := setPandoraServerConfig(senderConfig, serverConfigs)
	assert.NoError(t, err)
	assert.Equal(t, "", actualConfig[senderConf.KeyPandoraAutoCreate])

	serverConfigs = []map[string]interface{}{
		{
			transforms.KeyType:     ip.Name,
			transforms.TransformAt: ip.Server,
			"key": "ip",
		},
	}
	actualConfig, err = setPandoraServerConfig(senderConfig, serverConfigs)
	assert.NoError(t, err)
	assert.Equal(t, "ip ip", actualConfig[senderConf.KeyPandoraAutoCreate])

	senderConfig = conf.MapConf{
		senderConf.KeySenderType: senderConf.TypePandora,
	}
	serverConfigs = []map[string]interface{}{
		{
			transforms.KeyType:     ip.Name,
			transforms.TransformAt: ip.Local,
			"key": "a.b",
		},
	}
	actualConfig, err = setPandoraServerConfig(senderConfig, serverConfigs)
	assert.NoError(t, err)
	assert.Equal(t, "", actualConfig[senderConf.KeyPandoraAutoCreate])

	serverConfigs = []map[string]interface{}{
		{
			transforms.KeyType: "other",
		},
	}
	actualConfig, err = setPandoraServerConfig(senderConfig, serverConfigs)
	assert.NoError(t, err)
	assert.Equal(t, "", actualConfig[senderConf.KeyPandoraAutoCreate])

	serverConfigs = []map[string]interface{}{
		{
			transforms.KeyType:     ip.Name,
			transforms.TransformAt: ip.Server,
			"key": "ip.ip",
		},
	}
	actualConfig, err = setPandoraServerConfig(senderConfig, serverConfigs)
	assert.Error(t, err)
}

func Test_removeServerIPSchema(t *testing.T) {
	tests := []struct {
		autoCreate string
		key        string
		expect     string
	}{
		{
			autoCreate: "a ip,a ip",
			key:        "a",
			expect:     "",
		},
		{
			autoCreate: "pandora_stash string,a ip,b string",
			key:        "a",
			expect:     "pandora_stash string,b string",
		},
		{
			autoCreate: "",
			key:        "a",
			expect:     "",
		},
		{
			autoCreate: "a ip,b string",
			key:        "a",
			expect:     "b string",
		},
		{
			autoCreate: "a ip",
			key:        "a",
			expect:     "",
		},
	}
	for _, test := range tests {
		res := removeServerIPSchema(test.autoCreate, test.key)
		assert.Equal(t, test.expect, res)
	}
}

//之前：5000	    242788 ns/op	  126474 B/op	     758 allocs/op
//现在：5000	    266301 ns/op	  145645 B/op	    1572 allocs/op
// 需要优化
func BenchmarkStatusRestore(b *testing.B) {
	logkitConf := conf.MapConf{
		readerConf.KeyMetaPath: "testmeta",
		readerConf.KeyMode:     readerConf.ModeMongo,
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	if err != nil {
		b.Fatal(err)
	}
	r1 := &LogExportRunner{
		meta:         meta,
		rs:           &RunnerStatus{},
		lastRs:       &RunnerStatus{},
		historyError: &ErrorsList{},
	}
	r2 := &LogExportRunner{
		meta:         meta,
		rs:           &RunnerStatus{},
		lastRs:       &RunnerStatus{},
		historyError: &ErrorsList{},
	}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r1.StatusRestore()
		r2.StatusRestore()
	}
}

func randinsert(l *equeue.ErrorQueue, num int) {
	for i := 0; i < num; i++ {
		l.Put(equeue.ErrorInfo{
			Error: fmt.Sprintf("err %v", rand.Intn(100)),
			Count: int64(rand.Intn(100) + 1),
		})
	}
}

func TestBackupRestoreHistory(t *testing.T) {
	logkitConf := conf.MapConf{
		readerConf.KeyMetaPath: "meta",
		readerConf.KeyMode:     readerConf.ModeMongo,
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("meta")

	rq := equeue.New(10)
	randinsert(rq, 12)
	pq := equeue.New(10)
	randinsert(pq, 12)
	tq := equeue.New(10)
	randinsert(tq, 12)
	sq := equeue.New(10)
	randinsert(sq, 12)

	s1, _ := discard.NewSender(conf.MapConf{"name": "s1"})
	r1 := &LogExportRunner{
		meta:    meta,
		rsMutex: new(sync.RWMutex),
		rs: &RunnerStatus{
			TransformStats: map[string]StatsInfo{"pick-0": {Success: 1}},
			SenderStats:    map[string]StatsInfo{"s1": {Success: 1}},
		},
		historyError: &ErrorsList{
			ReadErrors:  rq,
			ParseErrors: pq,
			TransformErrors: map[string]*equeue.ErrorQueue{
				"pick-0": tq,
			},
			SendErrors: map[string]*equeue.ErrorQueue{
				"s1": sq,
			},
		},
		lastRs:       &RunnerStatus{},
		transformers: []transforms.Transformer{&mutate.Pick{}},
		senders:      []sender.Sender{s1},
	}

	r1.StatusBackup()

	r2 := &LogExportRunner{
		meta: meta,
		rs: &RunnerStatus{
			TransformStats: map[string]StatsInfo{},
			SenderStats:    map[string]StatsInfo{},
		},
		historyError: &ErrorsList{},
		lastRs:       &RunnerStatus{},
		transformers: []transforms.Transformer{&mutate.Pick{}},
		senders:      []sender.Sender{s1},
	}
	r2.StatusRestore()

	//保证restore与前面一致
	assert.Equal(t, r1.historyError.ReadErrors.List(), r2.historyError.ReadErrors.List())
	assert.Equal(t, r1.historyError.ParseErrors.List(), r2.historyError.ParseErrors.List())
	for k, v := range r1.historyError.TransformErrors {
		assert.Equal(t, v.List(), r2.historyError.TransformErrors[k].List())
	}
	for k, v := range r1.historyError.SendErrors {
		assert.Equal(t, v.List(), r2.historyError.SendErrors[k].List())
	}
}
