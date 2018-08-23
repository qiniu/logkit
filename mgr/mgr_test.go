package mgr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var test1 = `{
    "name":"test1.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./tests/logdir",
        "meta_path":"./test1/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
     "cleaner":{
		"cleaner_name":"test1",
        "delete_enable":"true",
        "delete_interval":"1",
        "reserve_file_number":"1",
        "reserve_file_size":"1"
    },
    "parser":{
        "name":"csv_parser", 
        "type":"csv",
        "csv_splitter":" ",
        "csv_schema":"t1 string"
    },
    "senders":[{
        "name":"file_sender",
        "sender_type":"file",
        "fault_tolerant":"true",
        "ft_save_log_path":"./test1/ft",
        "ft_sync_every":"2000",
        "ft_write_limit":"10",
        "file_send_path":"./test1/test_csv_file.txt"
    }]
}
`

var test2 = `
{
    "name":"test2.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./tests/logdir",
        "meta_path":"./test2/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
     "cleaner":{
		"cleaner_name":"test2",
        "delete_enable":"true",
        "delete_interval":"1",
        "reserve_file_number":"1",
        "reserve_file_size":"1"
    },
    "parser":{
        "name":"test2_csv_parser", 
        "type":"csv",
        "csv_splitter":" ",
        "csv_schema":"t1 string"
    },
    "senders":[{
        "name":"file_sender",
        "sender_type":"file",
        "fault_tolerant":"true",
        "ft_save_log_path":"./test2/ft",
        "ft_sync_every":"2000",
        "ft_write_limit":"10",
        "file_send_path":"./test2/test2_csv_file.txt"
    }]
}
`

var test3 = `{
    "name":"test3.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./tests/logdir",
        "meta_path":"./test3/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
	 "cleaner":{
		"cleaner_name":"test3",
        "delete_enable":"false",
        "delete_interval":"1",
        "reserve_file_number":"1",
        "reserve_file_size":"10240"
    },
    "parser":{
        "name":"csv_parser", 
        "type":"csv",
        "csv_splitter":" ",
        "csv_schema":"t1 string"
    },
    "senders":[{
        "name":"file_sender",
        "sender_type":"file",
        "fault_tolerant":"true",
        "ft_save_log_path":"./test3/ft",
        "ft_sync_every":"2000",
        "ft_write_limit":"10",
        "file_send_path":"./test3/test_csv_file.txt"
    }]
}
`
var test4 = `{
    "name":"test4.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./tests/logdir",
        "meta_path":"./test4/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
	 "cleaner":{
		"cleaner_name":"test4",
        "delete_enable":"true",
        "delete_interval":"1",
        "reserve_file_number":"1",
        "reserve_file_size":"1"
    },
    "parser":{
        "name":"csv_parser", 
        "type":"csv",
        "csv_splitter":" ",
        "csv_schema":"t1 string"
    },
    "senders":[{
        "name":"file_sender",
        "sender_type":"file",
        "fault_tolerant":"true",
        "ft_save_log_path":"./test4/ft",
        "ft_sync_every":"2000",
        "ft_write_limit":"10",
        "file_send_path":"./test4/test_csv_file.txt"
    }]
}
`
var test5 = `{
    "name":"test5.csv",
    "batch_len": 3,
    "batch_size": 2097152,
    "batch_interval": 60,
    "batch_try_times": 3, 
    "reader":{
        "log_path":"./tests2/logdir2",
        "meta_path":"./test5/meta_req_csv",
        "mode":"dir",
        "read_from":"oldest",
        "ignore_hidden":"true"
    },
	 "cleaner":{
		"cleaner_name":"test5",
        "delete_enable":"false",
        "delete_interval":"1",
        "reserve_file_number":"1",
        "reserve_file_size":"1"
    },
    "parser":{
        "name":"csv_parser", 
        "type":"csv",
        "csv_splitter":" ",
        "csv_schema":"t1 string"
    },
    "senders":[{
        "name":"file_sender",
        "sender_type":"file",
        "fault_tolerant":"true",
        "ft_save_log_path":"./test5/ft",
        "ft_sync_every":"2000",
        "ft_write_limit":"10",
        "file_send_path":"./test5/test_csv_file.txt"
    }]
}
`

func tryTest(tryTimes int, tryfunc func() bool) bool {
	for {
		tryTimes--
		if tryfunc() {
			return true
		}
		if tryTimes <= 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func createFile(name string, size int64) error {
	var bytes []byte
	for int64(len(bytes)) < size {
		bytes = append(bytes, []byte("abc\n")...)
	}
	return ioutil.WriteFile(name, bytes, 0666)
}
func createTestFile(metapath, logdir, logfile string) error {
	done1 := filepath.Join(metapath, "file.done.2016-10-01")
	logs := logfile + "\n"
	err := ioutil.WriteFile(done1, []byte(logs), 0666)
	if err != nil {
		return err
	}
	return nil
}

func getfilename(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	gots := make([]string, 0)
	for _, f := range files {
		gots = append(gots, f.Name())
	}
	return gots, nil
}

func Test_Watch(t *testing.T) {
	logfile := "./tests/logdir/log1"
	logdir := "./tests/logdir"
	if err := os.MkdirAll("./tests/confs1", 0777); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./tests")

	if err := os.MkdirAll("./tests/confs2", 0777); err != nil {
		t.Error(err)
	}
	if err := os.MkdirAll(logdir, 0777); err != nil {
		t.Error(err)
	}
	err := createFile(logfile, 20000000)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./tests/confs1/test1.conf", []byte(test1), 0666)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./tests/confs2/test2.conf", []byte(test2), 0666)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./tests/confs2/test3", []byte(test2), 0666)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Second)
	var conf ManagerConfig
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{
		"./tests/confs1",
		"./tests/confs2",
	}
	realdir, err := filepath.Abs(logdir)
	if err != nil {
		t.Error(err)
	}
	log.Println(">>>>>>>>", logdir)
	err = m.Watch(confs)
	if err != nil {
		t.Error(err)
	}
	if len(m.watchers) != 2 {
		t.Errorf("watchers exp 2 but got %v", len(m.watchers))
	}
	time.Sleep(5 * time.Second) //因为使用了异步add runners 有可能还没执行完。
	var runnerLength int
	m.runnerLock.Lock()
	runnerLength = len(m.runners)
	m.runnerLock.Unlock()
	if runnerLength != 2 {
		t.Fatalf("runners exp 2 but got %v", runnerLength)
	}
	time.Sleep(time.Second)

	// 再写入一个文件，变成三个runner
	err = ioutil.WriteFile("./tests/confs2/test3.conf", []byte(test3), os.ModePerm)
	if err != nil {
		t.Error(err)
	}

	if !tryTest(10, func() bool {
		m.runnerLock.RLock()
		runnerLength = len(m.runners)
		m.runnerLock.RUnlock()
		return runnerLength == 3
	}) {
		t.Fatalf("runners exp 3 after add test3.conf but got %v", runnerLength)
	}
	if !tryTest(10, func() bool {
		m.cleanLock.RLock()
		defer m.cleanLock.RUnlock()
		return m.cleanQueues[realdir].cleanerCount == 2
	}) {
		t.Fatalf("cleanerCount exp 2 after add test3.conf  but got %v", m.cleanQueues[realdir].cleanerCount)
	}

	time.Sleep(2 * time.Second)
	// 再写入一个文件，变成四个runner
	err = ioutil.WriteFile("./tests/confs1/test4.conf", []byte(test4), 0666)
	if err != nil {
		t.Error(err)
	}

	if !tryTest(10, func() bool {
		m.runnerLock.Lock()
		runnerLength = len(m.runners)
		m.runnerLock.Unlock()
		return runnerLength == 4
	}) {
		t.Fatalf("runners exp 4 after add test4.conf but got %v", runnerLength)
	}
	if !tryTest(10, func() bool { return m.cleanQueues[realdir].cleanerCount == 3 }) {
		t.Fatalf("cleanerCount exp 3 after add test4.conf but got %v", m.cleanQueues[realdir].cleanerCount)
	}

	// 此时四个runner有三个cleaner，都是针对同一个logdir
	if err = createTestFile("./test1/meta_req_csv", logdir, logfile); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./test1")

	if err = createTestFile("./test2/meta_req_csv", logdir, logfile); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./test2")

	if err = createTestFile("./test3/meta_req_csv", logdir, logfile); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./test3")

	// 三个地方有file done，但是依旧不能删。
	time.Sleep(3 * time.Second)
	exps := []string{"log1"}
	gots, err := getfilename(logdir)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gots, exps) {
		t.Errorf("test cleaner error exps %v but got %v, after add 3 filedones", exps, gots)
	}
	// 此时增加第四个filedone，所有cleaner都到位
	if err = createTestFile("./test4/meta_req_csv", logdir, logfile); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./test4")

	time.Sleep(2 * time.Second)
	exps = make([]string, 0)
	gots, err = getfilename(logdir)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gots, exps) {
		t.Errorf("test cleaner error exps %v but got %v, after add all filedones", exps, gots)
	}
	time.Sleep(2 * time.Second)
	// 移除一个文件，变成三个runner
	os.Remove("./tests/confs1/test4.conf")
	if !tryTest(10, func() bool {
		m.runnerLock.Lock()
		runnerLength = len(m.runners)
		m.runnerLock.Unlock()
		return runnerLength == 3
	}) {
		t.Fatalf("runners exp 3 after remove test4.conf but got %v", runnerLength)
	}
	if m.cleanQueues[realdir].cleanerCount != 2 {
		t.Errorf("cleanerCount exp 2 after remove test4.conf but got %v", m.cleanQueues[realdir].cleanerCount)
	}

	time.Sleep(2 * time.Second)
	// 移除一个文件，变回两个runner
	os.Remove("./tests/confs2/test3.conf")
	if !tryTest(10, func() bool {
		m.runnerLock.Lock()
		runnerLength = len(m.runners)
		m.runnerLock.Unlock()
		return runnerLength == 2
	}) {
		t.Fatalf("runners exp 2 after remove test3.conf but got %v", runnerLength)
	}
	if m.cleanQueues[realdir].cleanerCount != 2 {
		t.Errorf("cleanerCount exp 2 after remove test3.conf but got %v", m.cleanQueues[realdir].cleanerCount)
	}
	m.Stop()
}

func Test_Watch_LogDir(t *testing.T) {
	logfile2 := "./tests2/logdir2/log1"
	logdir2 := "./tests2/logdir2"
	if err := os.MkdirAll("./tests2/confs1", 0777); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./tests2")
	os.Setenv("DIR_NOT_EXIST_SLEEP_TIME", "8")
	defer func() {
		os.Setenv("DIR_NOT_EXIST_SLEEP_TIME", DIR_NOT_EXIST_SLEEP_TIME)
	}()
	var conf ManagerConfig
	m, err := NewManager(conf)
	if err != nil {
		t.Fatal(err)
	}
	confs := []string{"./tests2/confs1"}
	err = m.Watch(confs)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("./tests2/confs1/test5.conf", []byte(test5), 0666)
	if err != nil {
		t.Error(err)
	}
	confPathAbs, err := filepath.Abs("./tests2/confs1/test5.conf")
	m.runnerLock.Lock()
	_, ok := m.runners[confPathAbs]
	m.runnerLock.Unlock()
	if ok {
		t.Fatal("not exp, the runner should be not exsit")
	}

	time.Sleep(1 * time.Second)
	//创建logdir目录
	if err := os.MkdirAll(logdir2, 0777); err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("./test5")
	err = createFile(logfile2, 20000)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(10 * time.Second)
	m.runnerLock.Lock()
	_, ok = m.runners[confPathAbs]
	m.runnerLock.Unlock()
	assert.Equal(t, true, ok, fmt.Sprintf("runner of %v exp but not exsit in runners %v", confPathAbs, m.runners))
	m.Stop()
}
