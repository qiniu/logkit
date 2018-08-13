package cleaner

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

func runCleanChan(c <-chan CleanSignal, t *testing.T) {
	go func() {
		for cs := range c {
			if err := os.Remove(filepath.Join(cs.Logdir, cs.Filename)); err != nil {
				t.Error(err)
			}
		}
	}()
	return
}

func Test_CheckBelong(t *testing.T) {
	logfiles := "Test_CheckBelong"
	err := os.Mkdir(logfiles, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logfiles)
	c := conf.MapConf{}
	c[reader.KeyMetaPath] = logfiles
	c[KeyCleanEnable] = "true"
	c[reader.KeyLogPath] = logfiles
	c[reader.KeyMode] = "dir"
	meta, err := reader.NewMetaWithConf(c)
	if err != nil {
		t.Error(err)
	}
	cs := make(chan CleanSignal)
	runCleanChan(cs, t)
	cl, err := NewCleaner(c, meta, cs, logfiles)
	if err != nil {
		t.Fatal(err)
	}

	if cl.checkBelong(filepath.Join(logfiles, "xx")) != true {
		t.Error("check belong error,exp true but got flase")
	}

	if cl.checkBelong("xx") != false {
		t.Error("check belong error,exp false but got true")
	}
}

type TestReader struct{}

func (tr *TestReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(i & 0xff)
	}
	return len(p), nil
}

func createFile(name string, size int64, rd *TestReader) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}

	_, err = io.CopyN(f, rd, size)
	if err != nil {
		return err
	}
	return nil
}

func GetTestFiles(path string) error {
	donefiles := path
	err := os.Mkdir(donefiles, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		return err
	}
	done1 := filepath.Join(donefiles, "file.done.2016-10-01")
	done2 := filepath.Join(donefiles, "file.done.2016-10-02")
	log1 := donefiles + "/log1"
	log2 := donefiles + "/log2"
	logs := log1 + "\n" + log2 + "\n"
	err = ioutil.WriteFile(done1, []byte(logs), 0666)
	if err != nil {
		return err
	}
	log3 := donefiles + "/log3"
	err = ioutil.WriteFile(done2, []byte(log3), 0666)
	if err != nil {
		return err
	}
	tr := &TestReader{}
	err = createFile(log1, 1000000, tr)
	if err != nil {
		return err
	}
	err = createFile(log2, 10000000, tr)
	if err != nil {
		return err
	}
	err = createFile(log3, 10000000, tr)
	if err != nil {
		return err
	}
	return nil
}

func Test_clean(t *testing.T) {
	donefiles := "Test_clean"
	c := conf.MapConf{}
	c[reader.KeyMetaPath] = donefiles
	c[reader.KeyLogPath] = donefiles + "/" + "log"
	c[reader.KeyMode] = "dir"
	c[KeyCleanEnable] = "true"
	c[KeyReserveFileSize] = "15"
	c[KeyReserveFileNumber] = "1"
	c[KeyCleanInterval] = "1"
	meta, err := reader.NewMetaWithConf(c)
	if err != nil {
		t.Error(err)
	}
	cs := make(chan CleanSignal)
	runCleanChan(cs, t)
	cl, err := NewCleaner(c, meta, cs, donefiles)
	if err != nil {
		t.Fatal(err)
	}
	err = GetTestFiles(donefiles)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(donefiles)
	cl.reserveNumber = 1
	err = cl.Clean()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	files, err := ioutil.ReadDir(donefiles)
	if err != nil {
		t.Error(err)
	}
	var gots []string
	dfile := filepath.Base(meta.DeleteFile())
	exps := []string{dfile, "file.done.2016-10-02", "log3"}
	for _, f := range files {
		gots = append(gots, f.Name())
	}
	if !reflect.DeepEqual(gots, exps) {
		t.Fatalf("test clean error exps %v got %v", exps, gots)
	}
	err = os.RemoveAll(donefiles)
	if err != nil {
		t.Error(err)
	}
	err = GetTestFiles(donefiles)
	if err != nil {
		t.Fatal(err)
	}
	cl.reserveSize = 9 * MB
	err = cl.Clean()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
	files, err = ioutil.ReadDir(donefiles)
	if err != nil {
		t.Error(err)
	}
	gots = make([]string, 0)
	for _, f := range files {
		gots = append(gots, f.Name())
	}
	exps = []string{dfile}
	if !reflect.DeepEqual(gots, exps) {
		t.Fatalf("test clean error exps %v got %v", exps, gots)
	}
	sd, err := ioutil.ReadFile(meta.DeleteFile())
	if err != nil {
		t.Error(err)
	}
	expstr := "Test_clean/log3\nTest_clean/log2\nTest_clean/log1\n"
	if string(sd) != expstr {
		t.Errorf("exps %v got %v", expstr, string(sd))
	}
	err = os.RemoveAll(donefiles)
	if err != nil {
		t.Error(err)
	}

	err = GetTestFiles(donefiles)
	if err != nil {
		t.Fatal(err)
	}
	cl.reserveNumber = 10
	cl.reserveSize = 10 * 1024 * 1024
	err = cl.Clean()
	time.Sleep(3 * time.Second)
	if err != nil {
		t.Error(err)
	}
	files, err = ioutil.ReadDir(donefiles)
	if err != nil {
		t.Error(err)
	}
	gots = make([]string, 0)
	for _, f := range files {
		gots = append(gots, f.Name())
	}
	exps = []string{dfile, "file.done.2016-10-02", "log3"}
	if !reflect.DeepEqual(gots, exps) {
		t.Fatalf("test clean error exps %v got %v", exps, gots)
	}
}
