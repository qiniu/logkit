package autofile

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/tailx"
)

func init() {
	reader.RegisterConstructor(reader.ModeFileAuto, NewReader)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (r reader.Reader, err error) {
	path, err := conf.GetString(reader.KeyLogPath)
	if err != nil {
		return
	}
	logpath, mode, errStat := matchMode(path)
	if errStat != nil {
		err = errStat
		return
	}
	switch mode {
	case reader.ModeTailx:
		return tailx.NewReader(meta, conf)
	case reader.ModeDir:
		return reader.NewFileDirReader(meta, conf)
	case reader.ModeFile:
		return reader.NewSingleFileReader(meta, conf)
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
		mode = reader.ModeTailx
		return
	}
	//for logpath this path to make judgments
	fileInfo, err := os.Stat(logpath)
	if err != nil {
		return
	}
	if fileInfo.IsDir() == true {
		if shoudUseModeDir(path) {
			mode = reader.ModeDir
		} else {
			mode = reader.ModeTailx
			path = filepath.Join(path, "*")
		}
		return
	}
	mode = reader.ModeFile
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
