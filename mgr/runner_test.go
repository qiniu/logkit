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
	_ "github.com/qiniu/logkit/transforms/all"

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
		"log_path":       logpathLink,
		"meta_path":      metapath,
		"mode":           "dir",
		"read_from":      "oldest",
		"datasource_tag": "testtag",
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

	r, err := NewLogExportRunnerWithService(rinfo, reader, cleaner, pparser, nil, senders, meta)
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
	for _, dt := range dts {
		assert.Equal(t, absLogpath, dt["testtag"])
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
			"head_pattern":   "^" + qiniulogHeadPatthern,
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
			"head_pattern":   "^PREX " + qiniulogHeadPatthern,
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
	log1 := `2017/01/22 11:16:08.885550 [X-ZsU][INFO] disk.go:123: [REQ_END] 200 0.010k 3.792ms
		[WARN][SLdoIrCDZj7pmZsU] disk.go <job.freezeDeamon> pop() failed: not found
2017/01/22 11:15:54.947217 [2pyKMukqvwSd-ZsU][INFO] disk.go:124: Service: POST 10.200.20.25:9100/user/info, Code: 200, Xlog: AC, Time: 1ms
`
	log2 := `2016/10/20 17:20:30.642666 [ERROR] disk.go:125: github.com/qiniu/logkit/queue/disk.go:241
	1234 3243xsaxs
2016/10/20 17:20:30.642662 [123][WARN] disk.go:241: github.com/qiniu/logkit/queue/disk.go 1
`
	log3 := `2016/10/20 17:20:30.642662 [124][WARN] disk.go xxxxxx`
	expfiles := []string{`[REQ_END] 200 0.010k 3.792ms \t\t[WARN][SLdoIrCDZj7pmZsU] disk.go <job.freezeDeamon> pop() failed: not found`,
		`Service: POST 10.200.20.25:9100/user/info, Code: 200, Xlog: AC, Time: 1ms`,
		`github.com/qiniu/logkit/queue/disk.go:241 \t1234 3243xsaxs`, `github.com/qiniu/logkit/queue/disk.go 1`}
	expreqid := []string{"X-ZsU", "2pyKMukqvwSd-ZsU", "", "123"}
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
		"type": "qiniulog",
	}
	senderConfigs := []conf.MapConf{
		conf.MapConf{
			"name":        "mock_sender",
			"sender_type": "mock",
		},
	}

	rc := RunnerConfig{
		RunnerInfo:   rinfo,
		ReaderConfig: readerConfig,
		ParserConf:   parseConf,
		SenderConfig: senderConfigs,
	}
	rc = Compatible(rc)
	meta, err := reader.NewMetaWithConf(rc.ReaderConfig)
	if err != nil {
		t.Error(err)
	}
	reader, err := reader.NewFileBufReader(rc.ReaderConfig)
	if err != nil {
		t.Error(err)
	}
	ps := parser.NewParserRegistry()
	pparser, err := ps.NewLogParser(parseConf)
	if err != nil {
		t.Error(err)
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

	r, err := NewLogExportRunnerWithService(rinfo, reader, nil, pparser, nil, senders, meta)
	if err != nil {
		t.Error(err)
	}

	go r.Run()
	time.Sleep(time.Second)
	if err := ioutil.WriteFile(filepath.Join(logpath, "log3"), []byte(log3), 0666); err != nil {
		log.Fatalf("write log3 fail %v", err)
	}
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
		t.Errorf("got sender data not match error,expect 4 but %v", len(dts))
	}
	for idx, dt := range dts {
		assert.Equal(t, expfiles[idx], dt["log"], "equl log test")
		assert.Equal(t, expreqid[idx], dt["reqid"], "equal reqid test")
	}
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
			"data_path": "../transforms/ip/17monipdb.dat"
		}],
		"senders":[{
			"name":"file_sender",
			"sender_type":"file",
			"file_send_path":"./test2/test2_csv_file.txt"
		}]
	}`

	rc := RunnerConfig{}
	err := json.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers := createTransformers(rc)
	datas := []sender.Data{{"ip": "111.2.3.4"}}
	exp := []sender.Data{{
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
	newData := make([]sender.Data, 0)
	rc := RunnerConfig{}
	err := json.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers := createTransformers(rc)
	datas := []string{`{"status":"200","request_method":"POST","request_body":"<xml>\x0A","content_type":"text/xml"}`, `{"status":"200","request_method":"POST","request_body":"<xml>x0A","content_type":"text/xml"}`}
	for k := range transformers {
		datas, err = transformers[k].RawTransform(datas)
		assert.NoError(t, err)
		for i := range datas {
			var da sender.Data
			err = json.Unmarshal([]byte(datas[i]), &da)
			assert.NoError(t, err)
			newData = append(newData, da)
		}
	}
	exp := []sender.Data{
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
