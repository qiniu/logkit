package autofile

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/bufreader"
	"github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/reader/dirx"
	"github.com/qiniu/logkit/reader/tailx"
)

func init() {
	reader.RegisterConstructor(config.ModeFileAuto, NewReader)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (r reader.Reader, err error) {
	path, err := conf.GetString(config.KeyLogPath)
	if err != nil {
		return
	}
	logpath, mode, errStat := matchMode(path)
	if errStat != nil {
		err = errStat
		return
	}
	switch mode {
	case config.ModeTailx:
		conf[config.KeyLogPath] = logpath
		return tailx.NewReader(meta, conf)
	case config.ModeDir:
		return bufreader.NewFileDirReader(meta, conf)
	case config.ModeFile:
		return bufreader.NewSingleFileReader(meta, conf)
	case config.ModeDirx:
		conf[config.KeyLogPath] = logpath
		return dirx.NewReader(meta, conf)
	default:
		err = errors.New("can not find property mode for this path " + logpath)
	}
	return
}

func matchMode(logpath string) (path, mode string, err error) {
	_, after := filepath.Split(logpath)
	if after == "" {
		logpath = filepath.Dir(logpath)
	}
	path = logpath
	if strings.HasSuffix(logpath, ".tar.gz") || strings.HasSuffix(logpath, ".tar") || strings.HasSuffix(logpath, ".zip") {
		mode = config.ModeDirx
		return
	}
	if strings.HasSuffix(logpath, ".gz") {
		mode = config.ModeTailx
		return
	}

	//path with * matching tailx mode
	matchTailx := strings.Contains(logpath, "*")
	if matchTailx == true {
		mode = config.ModeTailx
		return
	}
	//for logpath this path to make judgments
	fileInfo, err := os.Stat(logpath)
	if err != nil {
		return
	}
	if fileInfo.IsDir() == true {
		if shouldUseModeDir(path) {
			mode = config.ModeDir
		} else {
			mode = config.ModeTailx
			path = filepath.Join(path, "*")
		}
		return
	}
	mode = config.ModeFile
	return
}

// 如果目录中有文件且最后修改时间在tailx的expire之前，则认为是dir模式
func shouldUseModeDir(logpath string) bool {
	files, err := ioutil.ReadDir(logpath)
	if err != nil {
		log.Warn("read dir error ", logpath, err)
		return true
	}
	for _, f := range files {
		// 必须和tailx中expire时间一致
		if f.ModTime().Add(24 * time.Hour).Before(time.Now()) {
			return true
		}
	}
	return false
}
