package reader

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

func NewFileAutoReader(meta *Meta, conf conf.MapConf) (reader Reader, err error) {

	path, err := conf.GetString(KeyLogPath)
	if err != nil {
		return
	}
	logpath, mode, errStat := matchMode(path)
	if errStat != nil {
		err = errStat
		return
	}
	switch mode {
	case ModeTailx:
		reader, err = NewMultiReader(meta, conf)
	case ModeDir:
		reader, err = NewFileDirReader(meta, conf)
	case ModeFile:
		reader, err = NewSingleFileReader(meta, conf)
	default:
		err = fmt.Errorf("can not find property mode for this logpath %v", logpath)
	}
	return
}

func matchMode(logpath string) (path, mode string, err error) {
	_, after := filepath.Split(logpath)
	if after == "" {
		logpath = filepath.Dir(logpath)
	}
	path = logpath
	//path with * matching tailx mode
	matchTailx := strings.Contains(logpath, "*")
	if matchTailx == true {
		mode = ModeTailx
		return
	}
	//for logpath this path to make judgments
	fileInfo, err := os.Stat(logpath)
	if err != nil {
		return
	}
	if fileInfo.IsDir() == true {
		if shoudUseModeDir(path) {
			mode = ModeDir
		} else {
			mode = ModeTailx
			path = filepath.Join(path, "*")
		}
		return
	}
	return
}

func shoudUseModeDir(logpath string) bool {
	files, err := ioutil.ReadDir(logpath)
	if err != nil {
		log.Warn("read dir %v error %v", logpath, err)
		return true
	}
	for _, f := range files {
		if f.ModTime().Add(24 * time.Hour).Before(time.Now()) {
			return true
		}
	}
	return false
}
