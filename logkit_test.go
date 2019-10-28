package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiniu/log"
	"github.com/stretchr/testify/assert"
)

func Test_getValidPath(t *testing.T) {
	t.Parallel()
	confs := []string{
		"test1",
		"./test1",
		"../logkit/test1",
		"./test2",
	}
	paths := getValidPath(confs)
	if len(paths) != 2 {
		t.Errorf("get path error exp 2 but got %v %v", len(paths), paths)
	}
	for _, v := range paths {
		os.Remove(v)
	}
}

func Test_RotateClean(t *testing.T) {
	t.Parallel()
	dirp := "Test_RotateClean"
	os.MkdirAll(dirp, 0755)
	defer os.RemoveAll(dirp)
	ch1 := make(chan struct{}, 0)
	go loopCleanLogkitLog(dirp, "logkit.log-*", 3, "10ms", ch1)
	ch2 := make(chan struct{}, 0)
	go loopRotateLogs(filepath.Join(dirp, "logkit.log"), 10, 10*time.Nanosecond, ch2)
	exitchan := make(chan struct{}, 0)
	go func() {
		i := 0
		for {
			select {
			case <-exitchan:
				return
			default:
				//通过这一行打印日志到文件，生成日志文件，误删
				log.Info("test output log ", i)
			}
			i++
		}
	}()
	time.Sleep(2 * time.Second)
	exitchan <- struct{}{}
	ch2 <- struct{}{}
	time.Sleep(time.Millisecond)
	ch1 <- struct{}{}
	nn, _ := ioutil.ReadDir(dirp)
	assert.Equal(t, 3, len(nn))
}

func Test_cleanlogkitlog(t *testing.T) {
	t.Parallel()
	dirp := "Test_cleanlogkitlog"
	os.MkdirAll(dirp, 0755)
	defer os.RemoveAll(dirp)
	ioutil.WriteFile(filepath.Join(dirp, "first.log"), []byte("first"), 0666)
	time.Sleep(10 * time.Millisecond)
	ioutil.WriteFile(filepath.Join(dirp, "second.log"), []byte("second"), 0666)
	time.Sleep(10 * time.Millisecond)
	ioutil.WriteFile(filepath.Join(dirp, "third.log"), []byte("third"), 0666)
	time.Sleep(10 * time.Millisecond)
	ioutil.WriteFile(filepath.Join(dirp, "forth.log"), []byte("forth"), 0666)
	time.Sleep(10 * time.Millisecond)
	cleanLogkitLog(dirp, "*.log", 3)
	nn, _ := ioutil.ReadDir(dirp)
	assert.Equal(t, 3, len(nn))
	for _, v := range nn {
		if v.Name() == "first.log" {
			t.Fatal("should not have first")
		}
	}
}
