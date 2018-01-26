package mgr

import (
	"io/ioutil"
	"log/syslog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/cleaner"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	_ "github.com/qiniu/logkit/transforms/all"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
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

	r, err := NewLogExportRunnerWithService(rinfo, reader, cleaner, pparser, nil, senders, nil, meta)
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
	var dts []Data
	rawData := r.senders[0].Name()[len("mock_sender "):]
	err = jsoniter.Unmarshal([]byte(rawData), &dts)
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 4 {
		t.Errorf("got sender data not match error,expect 2 but %v", len(dts))
	}
	for _, dt := range dts {
		assert.Equal(t, absLogpath, filepath.Dir(dt["testtag"].(string)))
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
		RunnerInfo:    rinfo,
		ReaderConfig:  readerConfig,
		ParserConf:    parseConf,
		SenderConfig:  senderConfigs,
		IsInWebFolder: false,
	}
	rc = Compatible(rc)
	meta, err := reader.NewMetaWithConf(rc.ReaderConfig)
	if err != nil {
		t.Error(err)
	}
	reader, err := reader.NewFileBufReader(rc.ReaderConfig, rc.IsInWebFolder)
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

	r, err := NewLogExportRunnerWithService(rinfo, reader, nil, pparser, nil, senders, nil, meta)
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
	var dts []Data
	rawData := r.senders[0].Name()[len("mock_sender "):]
	err = jsoniter.Unmarshal([]byte(rawData), &dts)
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
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	transformers := createTransformers(rc)
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
	transformers := createTransformers(rc)
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
	transformers := createTransformers(rc)
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
	transformers := createTransformers(rc)
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
	assert.Equal(t, SpeedStable, getTrend(0.02, 0))
}

func TestSpeedTrend(t *testing.T) {
	tests := []struct {
		olds  utils.StatsInfo
		news  utils.StatsInfo
		etime float64
		exp   utils.StatsInfo
	}{
		{
			olds: utils.StatsInfo{
				Success: 1,
				Speed:   1.0,
			},
			news: utils.StatsInfo{
				Success: 2,
			},
			etime: 1.0,
			exp: utils.StatsInfo{
				Success: 2,
				Speed:   1.0,
				Trend:   SpeedStable,
			},
		},
		{
			olds:  utils.StatsInfo{},
			news:  utils.StatsInfo{},
			etime: 0,
			exp: utils.StatsInfo{
				Success: 0,
				Speed:   0,
				Trend:   SpeedStable,
			},
		},
		{
			olds: utils.StatsInfo{
				Success: 1,
				Speed:   1.0,
			},
			news: utils.StatsInfo{
				Success: 10,
			},
			etime: 1.0,
			exp: utils.StatsInfo{
				Success: 10,
				Speed:   9.0,
				Trend:   SpeedUp,
			},
		},
		{
			olds: utils.StatsInfo{
				Success: 10,
				Speed:   10.0,
			},
			news: utils.StatsInfo{
				Success: 11,
			},
			etime: 1.0,
			exp: utils.StatsInfo{
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
				SenderStats: map[string]utils.StatsInfo{
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
				TransformStats: map[string]utils.StatsInfo{
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
				SenderStats: map[string]utils.StatsInfo{
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
				TransformStats: map[string]utils.StatsInfo{
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
				SenderStats: map[string]utils.StatsInfo{
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
				TransformStats: map[string]utils.StatsInfo{
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
		copyRunnerStatus(&ti.dst, &ti.src)
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

	os.Mkdir(metaDir, 0755)
	defer os.RemoveAll(metaDir)

	config1 := `{
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
	err := jsoniter.Unmarshal([]byte(config1), &rc)
	assert.NoError(t, err)
	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), parser.NewParserRegistry(), sender.NewSenderRegistry())
	assert.NoError(t, err)
	go rr.Run()
	sysLog, err := syslog.Dial("tcp", "localhost:5142",
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
	se := &utils.StatsError{
		ErrorIndex: []int{0, 3, 5},
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
}

func TestAddDatatags(t *testing.T) {
	dir := "TestAddDatatags"
	metaDir := filepath.Join(dir, "meta")
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	tagFile := filepath.Join(dir, "tagFile.json")
	err := ioutil.WriteFile(tagFile, []byte(`{  
	   	"Title":"tags",
	    "Author":["john","ada","alice"],
	    "IsTrue":true,
	    "Host":99
	  	}`), 0755)
	assert.NoError(t, err)
	logPath := filepath.Join(dir, "test.log")
	err = ioutil.WriteFile(logPath, []byte(`{"f1": "2","f2": "1","f3": "3"}`), 0755)
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

	rr, err := NewCustomRunner(rc, make(chan cleaner.CleanSignal), parser.NewParserRegistry(), sender.NewSenderRegistry())
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

func TestClassifySenderData(t *testing.T) {
	senderCnt := 3
	datas := []Data{
		Data{
			"a": "a",
			"b": "b",
			"c": "c",
			"d": "d",
		},
		Data{
			"a": "A",
			"b": "b",
			"c": "c",
			"d": "d",
		},
		Data{
			"a": "B",
			"b": "b",
			"c": "c",
			"d": "d",
		},
		Data{
			"a": "C",
			"b": "b",
			"c": "c",
			"d": "d",
		},
	}

	routerConf := sender.RouterConfig{
		KeyName:      "a",
		MatchType:    "equal",
		DefaultIndex: 0,
		Routes: map[string]int{
			"a": 2,
			"A": 1,
		},
	}

	r, err := sender.NewSenderRouter(routerConf, senderCnt)

	senderDataList := classifySenderData(datas, r, senderCnt)
	assert.Equal(t, senderCnt, len(senderDataList))
	assert.Equal(t, 2, len(senderDataList[0]))
	assert.Equal(t, 1, len(senderDataList[1]))
	assert.Equal(t, 1, len(senderDataList[2]))

	// 测试没有配置 router 的情况
	routerConf.KeyName = ""
	r, err = sender.NewSenderRouter(routerConf, senderCnt)
	assert.Nil(t, r)
	assert.NoError(t, err)
	senderDataList = classifySenderData(datas, r, senderCnt)
	assert.Equal(t, senderCnt, len(senderDataList))
	assert.Equal(t, 4, len(senderDataList[0]))
	assert.Equal(t, 4, len(senderDataList[1]))
	assert.Equal(t, 4, len(senderDataList[2]))
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
