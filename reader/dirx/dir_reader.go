package dirx

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

type dirReader struct {
	status        int32 // Note: 原子操作
	inactive      int32 // Note: 原子操作，当 inactive>0 时才会被 expire 回收
	br            *reader.BufReader
	runnerName    string
	originalPath  string // 实际的路径可能和配置传递进来的路径有所不同
	logPath       string
	readLock      sync.RWMutex
	readcache     string
	numEmptyLines int

	msgChan chan<- message
	errChan chan<- error

	stats     StatsInfo
	statsLock sync.RWMutex
}

func (dr *dirReader) setStatsError(err string) {
	dr.statsLock.Lock()
	defer dr.statsLock.Unlock()
	dr.stats.LastError = err
}

func (dr *dirReader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("Runner[%v] eader of log path %q has recovered from %v", dr.runnerName, dr.originalPath, err)
		}
	}()
	dr.errChan <- err
}

func (dr *dirReader) Run() {
	if !atomic.CompareAndSwapInt32(&dr.status, reader.StatusInit, reader.StatusRunning) {
		log.Errorf("Runner[%v] log path[%v] reader is not in init state before running, exiting", dr.runnerName, dr.originalPath)
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var err error
	for {
		if atomic.LoadInt32(&dr.status) == reader.StatusStopping || atomic.LoadInt32(&dr.status) == reader.StatusStopped {
			atomic.CompareAndSwapInt32(&dr.status, reader.StatusStopping, reader.StatusStopped)
			log.Warnf("Runner[%v] log path[%v] reader has stopped", dr.runnerName, dr.originalPath)
			return
		}

		if len(dr.readcache) == 0 {
			dr.readLock.Lock()
			dr.readcache, err = dr.br.ReadLine()
			dr.readLock.Unlock()
			if err != nil && err != io.EOF && err != os.ErrClosed {
				log.Warnf("Runner[%v] log path[%v] reader read failed: %v", dr.runnerName, dr.originalPath, err)
				dr.setStatsError(err.Error())
				dr.sendError(err)
				time.Sleep(2 * time.Second)
				continue
			}

			if len(dr.readcache) == 0 {
				dr.numEmptyLines++
				// 文件 EOF，同时没有任何内容，代表不是第一次 EOF，休息时间设置长一些
				if err == io.EOF {
					atomic.StoreInt32(&dr.inactive, 1)
					log.Debugf("Runner[%v] log path[%v] reader met EOF and becomes inactive now, sleep 5 seconds", dr.runnerName, dr.originalPath)
					time.Sleep(5 * time.Second)
					continue
				}

				// 大约一小时没读到内容，设置为 inactive
				if dr.numEmptyLines > 60*60 {
					atomic.StoreInt32(&dr.inactive, 1)
				}

				// 读取的结果为空，无论如何都 sleep 1s
				time.Sleep(time.Second)
				continue
			}
		}

		log.Debugf("Runner[%v] %v >>>>>> read cache[%v] line cache [%v]", dr.runnerName, dr.originalPath, dr.readcache, string(dr.br.FormMutiLine()))
		repeat := 0
		for {
			if len(dr.readcache) == 0 {
				break
			}
			repeat++
			if repeat%3000 == 0 {
				log.Errorf("Runner[%v] log path[%v] reader has timed out 3000 times with read cache: %v", dr.runnerName, dr.originalPath, dr.readcache)
			}

			atomic.StoreInt32(&dr.inactive, 0)
			dr.numEmptyLines = 0

			// 做这一层检查是为了快速结束和确保在上层 reader 已经关闭的情况下不会继续向 dr.msgChan 发送数据（因为可能已经被关闭）
			if atomic.LoadInt32(&dr.status) == reader.StatusStopping || atomic.LoadInt32(&dr.status) == reader.StatusStopped {
				log.Debugf("Runner[%v] log path[%v] reader has stopped when waits to send data", dr.runnerName, dr.originalPath)
				atomic.CompareAndSwapInt32(&dr.status, reader.StatusStopping, reader.StatusStopped)
				return
			}

			select {
			case dr.msgChan <- message{result: dr.readcache, logpath: dr.originalPath}:
				dr.readLock.Lock()
				dr.readcache = ""
				dr.readLock.Unlock()
			case <-ticker.C:
			}
		}
	}
}

// HasDirExpired 当指定目录内的所有文件都超过指定过期时间后返回 true，否则返回 false
func HasDirExpired(dir string, expire time.Duration) bool {
	// 如果目录本身已经不存在，则直接认为过期
	if !utils.IsExist(dir) {
		return true
	}

	// 如果过期时间为 0，则永不过期
	if expire.Nanoseconds() == 0 {
		return false
	}

	var latestModTime time.Time
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Errorf("Failed to walk directory file[%v]: %v", path, err)
			return nil
		}

		if info.ModTime().After(latestModTime) {
			latestModTime = info.ModTime()
		}

		return nil
	}); err != nil {
		log.Errorf("Failed to walk directory[%v]: %v", dir, err)
		return false
	}
	return latestModTime.Add(expire).Before(time.Now())
}

func (dr *dirReader) HasExpired(expire time.Duration) bool {
	// 在非 inactive 的情况下，数据尚未读完，有必要先继续处理
	return atomic.LoadInt32(&dr.inactive) > 0 && HasDirExpired(dr.logPath, expire)
}

func (dr *dirReader) Status() StatsInfo {
	dr.statsLock.RLock()
	defer dr.statsLock.RUnlock()
	return dr.stats
}

// 除了 sync 自己的 bufReader，还要 sync 一行 linecache
func (dr *dirReader) SyncMeta() string {
	dr.br.SyncMeta()

	dr.readLock.RLock()
	defer dr.readLock.RUnlock()
	return dr.readcache
}

func (dr *dirReader) Close() error {
	defer log.Warnf("Runner[%v] log path[%v] reader has closed", dr.runnerName, dr.originalPath)
	err := dr.br.Close()
	if atomic.CompareAndSwapInt32(&dr.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] log path[%v] reader is closing", dr.runnerName, dr.originalPath)
	} else {
		return err
	}

	waitedTimes := 0
	// 等待结束
	for atomic.LoadInt32(&dr.status) != reader.StatusStopped {
		waitedTimes++
		// 超过 300 个 10ms，即 3s 就强行退出
		if waitedTimes > 300 {
			log.Errorf("Runner[%v] log path[%v] reader was not closed after 3s, force closing it", dr.runnerName, dr.originalPath)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// dirReaders 用于管理一系列针对目录的读取器
type dirReaders struct {
	lock        sync.RWMutex
	readers     map[string]*dirReader // logPath -> dirReader
	cachedLines map[string]string     // logPath -> data

	// 以下为传入参数
	meta   *reader.Meta
	expire time.Duration
}

func newDirReaders(meta *reader.Meta, expire time.Duration, cachedLines map[string]string) *dirReaders {
	return &dirReaders{
		readers:     make(map[string]*dirReader),
		cachedLines: cachedLines,
		meta:        meta,
		expire:      expire,
	}
}

// Num 返回当前拥有的读取器数量
func (drs *dirReaders) Num() int {
	drs.lock.RLock()
	defer drs.lock.RUnlock()
	return len(drs.readers)
}

// HasReader 根据 logPath 判断是否已经有相应的读取器
func (drs *dirReaders) HasReader(logPath string) bool {
	drs.lock.RLock()
	defer drs.lock.RUnlock()

	_, ok := drs.readers[logPath]
	return ok
}

type newReaderOptions struct {
	Meta               *reader.Meta
	OriginalPath       string
	LogPath            string
	IgnoreHidden       bool
	SkipFirstLine      bool
	NewFileNewLine     bool
	IgnoreFileSuffixes []string
	ValidFilesRegex    string
	Whence             string
	BufferSize         int

	MsgChan chan<- message
	ErrChan chan<- error
}

func (drs *dirReaders) NewReader(opts newReaderOptions) (*dirReader, error) {
	rpath := strings.Replace(opts.LogPath, string(os.PathSeparator), "_", -1)
	subMetaPath := filepath.Join(opts.Meta.Dir, rpath)
	subMeta, err := reader.NewMeta(subMetaPath, subMetaPath, opts.LogPath, reader.ModeDir, opts.Meta.TagFile, reader.DefautFileRetention)
	if err != nil {
		return nil, fmt.Errorf("new meta: %v", err)
	}
	subMeta.Readlimit = opts.Meta.Readlimit

	fr, err := reader.NewSeqFile(subMeta, opts.LogPath, opts.IgnoreHidden, opts.NewFileNewLine, opts.IgnoreFileSuffixes, opts.ValidFilesRegex, opts.Whence)
	if err != nil {
		return nil, fmt.Errorf("new sequence file: %v", err)
	}
	fr.SkipFileFirstLine = opts.SkipFirstLine
	br, err := reader.NewReaderSize(fr, subMeta, opts.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("new buffer reader: %v", err)
	}

	dr := &dirReader{
		status:       reader.StatusInit,
		inactive:     1,
		br:           br,
		runnerName:   opts.Meta.RunnerName,
		originalPath: opts.OriginalPath,
		logPath:      opts.LogPath,
		msgChan:      opts.MsgChan,
		errChan:      opts.ErrChan,
	}

	drs.lock.Lock()
	defer drs.lock.Unlock()

	dr.readcache = drs.cachedLines[opts.LogPath]
	opts.Meta.AddSubMeta(opts.LogPath, subMeta)
	drs.readers[opts.LogPath] = dr
	return dr, nil
}

func (drs *dirReaders) hasCachedLine(logPath string) bool {
	drs.lock.RLock()
	defer drs.lock.RUnlock()

	return len(drs.cachedLines[logPath]) > 0
}

// checkExpiredDirs 方法用于检查和关闭过期目录的读取器，并清理相关元数据
func (drs *dirReaders) checkExpiredDirs() {
	drs.lock.Lock()
	defer drs.lock.Unlock()

	var expiredDirs []string
	for logPath, dr := range drs.readers {
		if dr.HasExpired(drs.expire) {
			if err := dr.Close(); err != nil {
				log.Errorf("Failed to close log path[%v] reader: %v", logPath, err)
			}
			delete(drs.readers, logPath)
			delete(drs.cachedLines, logPath)
			drs.meta.RemoveSubMeta(logPath)
			expiredDirs = append(expiredDirs, logPath)
		}
	}
	if len(expiredDirs) > 0 {
		log.Infof("Runner[%v] has expired log paths: %v", drs.meta.RunnerName, expiredDirs)
	} else {
		log.Debugf("Runner[%v] no log path has expired", drs.meta.RunnerName)
	}
}

func (drs *dirReaders) getReaders() []*dirReader {
	drs.lock.RLock()
	defer drs.lock.RUnlock()

	readers := make([]*dirReader, 0, drs.Num())
	for _, dr := range drs.readers {
		readers = append(readers, dr)
	}
	return readers
}

func (drs *dirReaders) SyncMeta() ([]byte, error) {
	for _, dr := range drs.getReaders() {
		readcache := dr.SyncMeta()
		if len(readcache) == 0 {
			continue
		}

		drs.lock.Lock()
		drs.cachedLines[dr.logPath] = readcache
		drs.lock.Unlock()
	}
	drs.lock.RLock()
	data, err := jsoniter.Marshal(drs.cachedLines)
	drs.lock.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("marshal cached lines: %v", err)
	}
	return data, nil
}

func (drs *dirReaders) Close() {
	var wg sync.WaitGroup
	for _, dr := range drs.getReaders() {
		wg.Add(1)
		go func(r *dirReader) {
			defer wg.Done()
			if err := r.Close(); err != nil {
				log.Errorf("Runner[%v] close log path[%v] reader failed: %v", drs.meta.RunnerName, r.originalPath, err)
			}
		}(dr)
	}
	wg.Wait()
}

func (drs *dirReaders) Reset() error {
	errMsgs := make([]string, 0)
	if err := drs.meta.Reset(); err != nil {
		errMsgs = append(errMsgs, err.Error())
	}
	for _, ar := range drs.getReaders() {
		if ar.br != nil {
			if err := ar.br.Meta.Reset(); err != nil {
				errMsgs = append(errMsgs, err.Error())
			}
		}
	}
	if len(errMsgs) > 0 {
		return errors.New(strings.Join(errMsgs, "\n"))
	}
	return nil
}
