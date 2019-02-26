package dirx

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/bufreader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.Reader       = &Reader{}
	_ Resetable           = &Reader{}
)

func init() {
	reader.RegisterConstructor(ModeDirx, NewReader)
}

type message struct {
	result      string
	logpath     string
	currentFile string
}

type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32

	stopChan chan struct{}
	msgChan  chan message
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	headRegexp  *regexp.Regexp
	currentFile string
	dirReaders  *dirReaders

	// 以下为传入参数
	logPathPattern       string
	ignoreLogPathPattern string
	statInterval         time.Duration
	expire               time.Duration
	submetaExpire        time.Duration
	expireDelete         bool
	deleteDirs           chan string
	maxOpenFiles         int
	ignoreHidden         bool
	skipFirstLine        bool
	newFileNewLine       bool
	ignoreFileSuffixes   []string
	validFilesRegex      string
	whence               string
	bufferSize           int

	expireMap map[string]int64

	notFirstTime  bool
	readSameInode bool
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	logPathPattern, err := conf.GetString(KeyLogPath)
	if err != nil {
		return nil, err
	}

	ignoreLogPathPattern, _ := conf.GetStringOr(KeyIgnoreLogPath, "")

	statIntervalDur, _ := conf.GetStringOr(KeyStatInterval, "3m")
	statInterval, err := time.ParseDuration(statIntervalDur)
	if err != nil {
		return nil, err
	}

	expireDur, _ := conf.GetStringOr(KeyExpire, "0s")
	expire, err := time.ParseDuration(expireDur)
	if err != nil {
		return nil, err
	}

	submetaExpireDur, _ := conf.GetStringOr(KeySubmetaExpire, "720h")
	submetaExpire, err := time.ParseDuration(submetaExpireDur)
	if err != nil {
		return nil, err
	}
	// submetaExpire 为 0 时，不清理元数据
	if IsSubmetaExpireValid(submetaExpire, expire) {
		return nil, fmt.Errorf("%q valus is less than %q", KeySubmetaExpire, KeyExpire)
	}

	expireDelete, _ := conf.GetBoolOr(KeyExpireDelete, false)

	maxOpenFiles, _ := conf.GetIntOr(KeyMaxOpenFiles, 256)
	ignoreHidden, _ := conf.GetBoolOr(KeyIgnoreHiddenFile, true) // 默认不读取隐藏文件
	skipFirstLine, _ := conf.GetBoolOr(KeySkipFileFirstLine, false)
	newFileNewLine, _ := conf.GetBoolOr(KeyNewFileNewLine, false)
	ignoreFileSuffixes, _ := conf.GetStringListOr(KeyIgnoreFileSuffix, DefaultIgnoreFileSuffixes)
	validFilesRegex, _ := conf.GetStringOr(KeyValidFilePattern, "*")
	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	bufferSize, _ := conf.GetIntOr(KeyBufSize, bufreader.DefaultBufSize)
	readSameInode, _ := conf.GetBoolOr(KeyReadSameInode, false)

	_, _, bufsize, err := meta.ReadBufMeta()
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] %v recover from meta error %v, ignore...", meta.RunnerName, logPathPattern, err)
		} else {
			log.Warnf("Runner[%v] %v recover from meta error %v, ignore...", meta.RunnerName, logPathPattern, err)
		}
		bufsize = 0
	}

	cachedLines := make(map[string]string)
	buf := make([]byte, bufsize)
	if bufsize > 0 {
		if _, err = meta.ReadBuf(buf); err != nil {
			if os.IsNotExist(err) {
				log.Debugf("Runner[%v] read buf file[%v] failed: %v, ignore...", meta.RunnerName, meta.BufFile(), err)
			} else {
				log.Warnf("Runner[%v] read buf file[%v] failed: %v, ignore...", meta.RunnerName, meta.BufFile(), err)
			}
		} else {
			err = jsoniter.Unmarshal(buf, &cachedLines)
			if err != nil {
				log.Warnf("Runner[%v] unmarshal read buf cache failed: %v, ignore...", meta.RunnerName, err)
			}
		}
	}

	deleteDirs := make(chan string, 10)
	return &Reader{
		meta:                 meta,
		status:               StatusInit,
		stopChan:             make(chan struct{}),
		msgChan:              make(chan message),
		errChan:              make(chan error),
		dirReaders:           newDirReaders(meta, expire, cachedLines, expireDelete, deleteDirs),
		logPathPattern:       strings.TrimSuffix(logPathPattern, "/"),
		ignoreLogPathPattern: strings.TrimSuffix(ignoreLogPathPattern, "/"),
		statInterval:         statInterval,
		expire:               expire,
		submetaExpire:        submetaExpire,
		expireDelete:         expireDelete,
		deleteDirs:           deleteDirs,
		maxOpenFiles:         maxOpenFiles,
		ignoreHidden:         ignoreHidden,
		skipFirstLine:        skipFirstLine,
		newFileNewLine:       newFileNewLine,
		ignoreFileSuffixes:   ignoreFileSuffixes,
		validFilesRegex:      validFilesRegex,
		whence:               whence,
		bufferSize:           bufferSize,
		readSameInode:        readSameInode,
		expireMap:            make(map[string]int64),
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *Reader) Name() string {
	return "DirxReader: " + r.logPathPattern
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	reg, err := reader.HeadPatternMode(mode, v)
	if err != nil {
		return fmt.Errorf("get head pattern mode: %v", err)
	}
	if reg != nil {
		r.headRegexp = reg
	}
	return nil
}

func (r *Reader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) statLogPath() {
	// 达到最大打开文件数时不再追踪
	if r.dirReaders.Num() >= r.maxOpenFiles {
		log.Warnf("Runner[%v] has met 'maxOpenFiles' limit %v, stat new log ignored", r.meta.RunnerName, r.maxOpenFiles)
		return
	}

	matches, err := filepath.Glob(r.logPathPattern)
	if err != nil {
		errMsg := fmt.Sprintf("Runner[%v] stat log path failed: %v", r.meta.RunnerName, err)
		log.Error(errMsg)
		r.setStatsError(errMsg)
		return
	}
	if len(matches) == 0 {
		log.Debugf("Runner[%v] no match found after stated log path %q", r.meta.RunnerName, r.logPathPattern)
		return
	}
	log.Debugf("Runner[%v] %d matches found after stated log path %q: %v", r.meta.RunnerName, len(matches), r.logPathPattern, matches)

	var unmatchMap = make(map[string]bool)
	if r.ignoreLogPathPattern != "" {
		unmatches, err := filepath.Glob(r.ignoreLogPathPattern)
		if err != nil {
			log.Errorf("Runner[%v] stat ignoreLogPathPattern error %v", r.meta.RunnerName, err)
			r.setStatsError("Runner[" + r.meta.RunnerName + "] stat ignoreLogPathPattern error " + err.Error())
			return
		}
		for _, unmatch := range unmatches {
			unmatchMap[unmatch] = true
		}
		if len(unmatches) > 0 {
			log.Debugf("Runner[%v] %d unmatches found after stated ignore log path %q: %v", r.meta.RunnerName, len(unmatches), r.ignoreLogPathPattern, unmatches)
		}
	}

	var newPaths []string
	for _, m := range matches {
		if unmatchMap[m] {
			continue
		}
		logPath, fi, err := GetRealPath(m)
		if err != nil {
			log.Warnf("Runner[%v] file pattern %v match %v stat failed: %v, ignored this match", r.meta.RunnerName, r.logPathPattern, m, err)
			continue
		}
		if !fi.IsDir() && !(strings.HasSuffix(logPath, ".tar") || strings.HasSuffix(logPath, ".tar.gz")) {
			log.Debugf("Runner[%v] %v is a file, mode[dirx] only supports stat directory, ignored this match", r.meta.RunnerName, m)
			continue
		}
		if r.dirReaders.HasReader(logPath) {
			log.Debugf("Runner[%v] %q is collecting, ignored this path", r.meta.RunnerName, logPath)
			continue
		}

		// 过期的文件不追踪，除非之前追踪的并且有日志没读完
		if !r.dirReaders.hasCachedLine(logPath) && HasDirExpired(logPath, r.dirReaders.expire) {
			if r.whence == WhenceNewest {
				fileMap, err := utils.GetFiles(r.meta.RunnerName, logPath)
				if err != nil {
					log.Errorf("Runner[%v] get log path %q failed %v, ignored this time", r.meta.RunnerName, logPath, err)
				} else {
					utils.UpdateExpireMap(r.meta.RunnerName, fileMap, r.expireMap)
				}
			}
			log.Debugf("Runner[%v] log path %q has expired, ignored this time", r.meta.RunnerName, logPath)
			continue
		}
		dr, err := r.dirReaders.NewReader(newReaderOptions{
			Meta:               r.meta,
			OriginalPath:       m,
			LogPath:            logPath,
			IgnoreHidden:       r.ignoreHidden,
			SkipFirstLine:      r.skipFirstLine,
			NewFileNewLine:     r.newFileNewLine,
			IgnoreFileSuffixes: r.ignoreFileSuffixes,
			ValidFilesRegex:    r.validFilesRegex,
			Whence:             r.whence,
			BufferSize:         r.bufferSize,
			MsgChan:            r.msgChan,
			ErrChan:            r.errChan,
			ReadSameInode:      r.readSameInode,
			expireMap:          r.expireMap,
		}, r.notFirstTime)
		if err != nil {
			if err == ErrAlreadyExist {
				continue
			}
			err = fmt.Errorf("create new reader for log path %q failed: %v", logPath, err)
			r.sendError(err)
			log.Errorf("Runner[%v] %v, ignored this path", r.meta.RunnerName, err)
			continue
		}

		if r.headRegexp != nil {
			err = dr.br.SetMode(ReadModeHeadPatternRegexp, r.headRegexp)
			if err != nil {
				errMsg := fmt.Sprintf("Runner[%v] set mode for log path %q failed: %v", r.meta.RunnerName, logPath, err)
				log.Error(errMsg)
				r.setStatsError(errMsg)
			}
		}
		newPaths = append(newPaths, logPath)

		if r.hasStopped() || r.isStopping() {
			log.Warnf("Runner[%v] created new reader for log path %q but daemon reader has stopped/is stopping, will not run at this time", r.meta.RunnerName, logPath)
			continue
		}

		go dr.Run()
	}
	if !r.notFirstTime {
		r.notFirstTime = true
	}
	if len(newPaths) > 0 {
		log.Infof("Runner[%v] stat log path found %d new log paths: %v", r.meta.RunnerName, len(newPaths), newPaths)
	} else {
		log.Debugf("Runner[%v] stat log path has not found any new log path", r.meta.RunnerName)
	}
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	go func() {
		ticker := time.NewTicker(r.statInterval)
		defer ticker.Stop()
		for {
			r.dirReaders.checkExpiredDirs()
			utils.CheckNotExistFile(r.meta.RunnerName, r.expireMap)
			r.statLogPath()

			select {
			case <-r.stopChan:
				atomic.StoreInt32(&r.status, StatusStopped)
				log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
				return
			case <-ticker.C:
			}
		}
	}()

	if r.expireDelete {
		go func() {
			for {
				select {
				case <-r.stopChan:
					return
				case path := <-r.deleteDirs:
					err := os.RemoveAll(path)
					if err != nil {
						log.Errorf("Runner[%v] %q delete expire and read done dir %s, err: %v", r.meta.RunnerName, r.Name(), path, err)
					} else {
						log.Infof("Runner[%v] %q delete expire and read done dir %s finished", r.meta.RunnerName, r.Name(), path)
					}
				}
			}
		}()
	}

	if IsSubMetaExpire(r.submetaExpire, r.expire) {
		go func() {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				r.meta.CheckExpiredSubMetas(r.submetaExpire)

				select {
				case <-r.stopChan:
					return
				case <-ticker.C:
				}
			}
		}()
	}

	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return r.currentFile
}

// Note: 对 currentFile 的操作非线程安全，需由上层逻辑保证同步调用 ReadLine
func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case msg := <-r.msgChan:
		r.currentFile = msg.currentFile
		return msg.result, nil
	case <-timer.C:
		return "", r.readError()
	}

	return "", nil
}

func (r *Reader) Status() StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()

	for _, dr := range r.dirReaders.getReaders() {
		st := dr.Status()
		if st.LastError != "" {
			r.stats.LastError += "\n<" + dr.originalPath + ">: " + st.LastError
		}
	}
	return r.stats
}

// SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复
func (r *Reader) SyncMeta() {
	data, err := r.dirReaders.SyncMeta()
	if err != nil {
		log.Errorf("Runner[%v] reader %q sync meta failed: %v", r.meta.RunnerName, r.Name(), err)
		return
	}

	err = r.meta.WriteBuf(data, 0, 0, len(data))
	if err != nil {
		log.Errorf("Runner[%v] write meta[%v] failed: %v", r.meta.RunnerName, string(data), err)
		return
	}

	if IsSubMetaExpire(r.submetaExpire, r.expire) {
		r.meta.CleanExpiredSubMetas(r.submetaExpire)
	}
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	r.dirReaders.Close()
	r.SyncMeta()

	// 在所有 dirReader 关闭完成后再关闭管道
	close(r.msgChan)
	close(r.errChan)
	return nil
}

func (r *Reader) Reset() error {
	r.expireMap = make(map[string]int64)
	return r.dirReaders.Reset()
}

func (r *Reader) readError() error {
	select {
	case err := <-r.errChan:
		return err
	default:
		return nil
	}
}
