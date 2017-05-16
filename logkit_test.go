package main

import (
	"os"
	"testing"
	"time"
)

func Test_getValidPath(t *testing.T) {
	confs := []string{
		"test1",
		"./test1",
		"../logkit/test1",
		"./test2",
	}
	paths := getValidPath(confs)
	if len(paths) != 2 {
		t.Errorf("get paths error exp 2 but got %v %v", len(paths), paths)
	}
	for _, v := range paths {
		os.Remove(v)
	}
}

func Test_main(t *testing.T) {
	ch := make(chan struct{}, 0)
	go loopCleanLogkitLog("", "", 0, ch)
	time.Sleep(time.Second * 3)
	ch <- struct{}{}
}
