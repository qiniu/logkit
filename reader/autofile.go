package reader

import (
	"strings"
	"io/ioutil"
	"path"
	
	"github.com/qiniu/logkit/conf"
)

const (
	winsign = "\\"
	comsign = "/"
)

func NewFileAutoReader(conf conf.MapConf, meta *Meta, isFromWeb bool) (multiReader *MultiReader, bufReader *BufReader, err error) {
		logpath, err := conf.GetString(KeyLogPath)
		bufSize, _ := conf.GetIntOr(KeyBufSize, defaultBufSize)
		whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
		var fr FileReader
	    //windows path for replacement
		logpath = strings.Replace(logpath,winsign,comsign,-1)
		// for example: The path is "/usr/logkit/"
		_ , after := path.Split(logpath)
		if after == "" {
			logpath = path.Dir(logpath)
		}
		//path with * matching tailx mode
		isSub := strings.Contains(logpath, "*")
		if isSub == true {
			meta.mode = ModeTailx
			expireDur, _ := conf.GetStringOr(KeyExpire, "24h")
			stateIntervalDur, _ := conf.GetStringOr(KeyStatInterval, "3m")
			maxOpenFiles, _ := conf.GetIntOr(KeyMaxOpenFiles, 256)
			multiReader, err = NewMultiReader(meta, logpath, whence, expireDur, stateIntervalDur, maxOpenFiles)
		}else{
			//for "/usr/logkit" this path to make judgments
			dirStr := path.Dir(logpath)
			lastStr := path.Base(logpath)
			files,_ := ioutil.ReadDir(dirStr)
			for _, file := range files {
				if file.Name() == lastStr {
					if file.IsDir(){
						meta.mode = ModeDir
						ignoreHidden, _ := conf.GetBoolOr(KeyIgnoreHiddenFile, true)
						ignoreFileSuffix, _ := conf.GetStringListOr(KeyIgnoreFileSuffix, defaultIgnoreFileSuffix)
						validFilesRegex, _ := conf.GetStringOr(KeyValidFilePattern, "*")
						fr, err = NewSeqFile(meta, logpath, ignoreHidden, ignoreFileSuffix, validFilesRegex, whence)
						if err != nil {
							return
						}
						bufReader, err = NewReaderSize(fr, meta, bufSize)
					}else{
						meta.mode = ModeFile
						fr, err = NewSingleFile(meta, logpath, whence, isFromWeb)
						if err != nil {
							return
						}
						bufReader, err = NewReaderSize(fr, meta, bufSize)
					}
					break;
				}
			}
		}
		return
}

