package mgr

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/logkit/utils"
)

func Test_generateStatsShell(t *testing.T) {
	err := generateStatsShell(":4001", "/logkit")
	if err != nil {
		t.Errorf("Test_generateStatsShell fail %v", err)
	}
	_, err = os.Stat(StatsShell)
	if err != nil {
		t.Error(StatsShell + " not found")
	}
	os.Remove(StatsShell)
}

var testRestConf = `{
    "name":"test1.csv",
    "batch_len": 1,
    "batch_size": 20,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./Test_Rest/logdir",
        "meta_path":"./Test_Rest/meta_mock_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
    "parser":{
        "name":         "req_csv",
		"type":         "csv",
		"csv_schema":   "logtype string, xx long",
		"csv_splitter": " "
    },
    "senders":[{
		"name":           "file_sender",
		"sender_type":    "file",
		"file_send_path": "./Test_Rest/filesenderdata"
    }]
}`

func Test_Rest(t *testing.T) {
	dir := "Test_Rest"
	if err := os.Mkdir(dir, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", dir, err)
	}
	defer os.RemoveAll(dir)
	logpath := dir + "/logdir"
	metapath := dir + "/meta_mock_csv"
	logconfs := dir + "/confs"
	if err := os.Mkdir(logpath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logpath, err)
	}
	if err := os.Mkdir(metapath, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", metapath, err)
	}
	if err := os.Mkdir(logconfs, 0755); err != nil {
		log.Fatalf("Test_Run error mkdir %v %v", logconfs, err)
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
	err := ioutil.WriteFile(logconfs+"/test1.conf", []byte(testRestConf), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	var conf ManagerConfig
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		dir + "/confs",
	}
	err = m.Watch(confs)
	if err != nil {
		t.Error(err)
	}
	rs := NewRestService(m)
	defer func() {
		rs.Stop()
		os.Remove(StatsShell)
	}()
	time.Sleep(5 * time.Second)
	cmd := exec.Command("./stats")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	rss := make(map[string]RunnerStatus)
	err = json.Unmarshal([]byte(out.String()), &rss)
	if err != nil {
		t.Error(err, out.String())
	}
	rp, err := filepath.Abs(logpath)
	if err != nil {
		t.Error(err)
	}
	exp := map[string]RunnerStatus{
		"test1.csv": RunnerStatus{
			Name:    "test1.csv",
			Logpath: rp,
			Lag: RunnerLag{
				Size:  0,
				Files: 0,
			},
			ParserStats: utils.StatsInfo{
				Errors:  0,
				Success: 4,
			},
			SenderStats: map[string]utils.StatsInfo{
				"file_sender": utils.StatsInfo{
					Errors:  0,
					Success: 4,
				},
			},
		},
	}
	if !reflect.DeepEqual(rss, exp) {
		t.Errorf("get status error exp %v but got %v", exp, rss)
	}
}
