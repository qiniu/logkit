package reader

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils"
)

type MultiReader struct {
	started     bool
	status      int32
	fileReaders map[string]*ActiveReader
	armapmux    sync.Mutex
	startmux    sync.Mutex
	curFile     string
	headRegexp  *regexp.Regexp
	cacheMap    map[string]string

	msgChan chan Result

	//以下为传入参数
	meta           *Meta
	logPathPattern string
	expire         time.Duration
	statInterval   time.Duration
	maxOpenFiles   int
	whence         string

	stats     utils.StatsInfo
	statsLock sync.RWMutex
}

type ActiveReader struct {
	cacheLineMux sync.RWMutex
	br           *BufReader
	realpath     string
	originpath   string
	readcache    string
	msgchan      chan<- Result
	status       int32
	inactive     int32 //当inactive>0 时才会被expire回收
	runnerName   string

	stats     utils.StatsInfo
	statsLock sync.RWMutex
}

type Result struct {
	result  string
	logpath string
}

func NewActiveReader(originPath, realPath, whence string, meta *Meta, msgChan chan<- Result) (ar *ActiveReader, err error) {
	rpath := strings.Replace(realPath, string(os.PathSeparator), "/", -1)
	subMetaPath := filepath.Join(meta.dir, rpath)
	subMeta, err := NewMeta(subMetaPath, subMetaPath, realPath, ModeFile, meta.tagFile, defautFileRetention)
	if err != nil {
		return nil, err
	}
	subMeta.readlimit = meta.readlimit
	isFromWeb := false
	fr, err := NewSingleFile(subMeta, realPath, whence, isFromWeb)
	if err != nil {
		return
	}
	bf, err := NewReaderSize(fr, subMeta, defaultBufSize)
	if err != nil {
		return
	}
	ar = &ActiveReader{
		cacheLineMux: sync.RWMutex{},
		br:           bf,
		realpath:     realPath,
		originpath:   originPath,
		msgchan:      msgChan,
		inactive:     1,
		runnerName:   meta.RunnerName,
		status:       StatusInit,
		statsLock:    sync.RWMutex{},
	}
	return
}

func (ar *ActiveReader) Run() {
	if !atomic.CompareAndSwapInt32(&ar.status, StatusInit, StatusRunning) {
		log.Errorf("Runner[%v] ActiveReader %s was not in StatusInit before Running,exit it...", ar.runnerName, ar.originpath)
		return
	}
	var err error
	timer := time.NewTicker(time.Second)
	for {
		if atomic.LoadInt32(&ar.status) == StatusStopped || atomic.LoadInt32(&ar.status) == StatusStopping {
			atomic.CompareAndSwapInt32(&ar.status, StatusStopping, StatusStopped)
			log.Warnf("Runner[%v] ActiveReader %s was stopped", ar.runnerName, ar.originpath)
			return
		}
		if ar.readcache == "" {
			ar.cacheLineMux.Lock()
			ar.readcache, err = ar.br.ReadLine()
			ar.cacheLineMux.Unlock()
			if err != nil && err != io.EOF {
				log.Warnf("Runner[%v] ActiveReader %s read error: %v", ar.runnerName, ar.originpath, err)
				ar.setStatsError(err.Error())
				time.Sleep(3 * time.Second)
				continue
			}
			//文件EOF，同时没有任何内容，代表不是第一次EOF，休息时间设置长一些
			if ar.readcache == "" && err == io.EOF {
				atomic.StoreInt32(&ar.inactive, 1)
				log.Debugf("Runner[%v] %v meet EOF, ActiveReader was inactive now, sleep 5 seconds", ar.runnerName, ar.originpath)
				time.Sleep(5 * time.Second)
				continue
			}
		}
		log.Debugf("Runner[%v] %v >>>>>>readcache <%v> linecache <%v>", ar.runnerName, ar.originpath, ar.readcache, ar.br.lineCache)
		repeat := 0
		for {
			if ar.readcache == "" {
				break
			}
			repeat++
			if repeat%3000 == 0 {
				log.Errorf("Runner[%v] %v ActiveReader has timeout 3000 times with readcache %v", ar.runnerName, ar.originpath, ar.readcache)
			}

			atomic.StoreInt32(&ar.inactive, 0)
			//做这一层结构为了快速结束
			if atomic.LoadInt32(&ar.status) == StatusStopped || atomic.LoadInt32(&ar.status) == StatusStopping {
				log.Debugf("Runner[%v] %v ActiveReader was stopped when waiting to send data", ar.runnerName, ar.originpath)
				atomic.CompareAndSwapInt32(&ar.status, StatusStopping, StatusStopped)
				return
			}
			select {
			case ar.msgchan <- Result{result: ar.readcache, logpath: ar.originpath}:
				ar.cacheLineMux.Lock()
				ar.readcache = ""
				ar.cacheLineMux.Unlock()
			case <-timer.C:
			}
		}
	}
}
func (ar *ActiveReader) Close() error {
	defer log.Warnf("Runner[%v] ActiveReader %s was closed", ar.runnerName, ar.originpath)
	err := ar.br.Close()

	if atomic.CompareAndSwapInt32(&ar.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] ActiveReader %s was closing", ar.runnerName, ar.originpath)
	} else {
		return err
	}

	cnt := 0
	// 等待结束
	for atomic.LoadInt32(&ar.status) != StatusStopped {
		cnt++
		//超过300个10ms，即3s，就强行退出
		if cnt > 300 {
			log.Errorf("Runner[%v] ActiveReader %s was not closed after 3s, force closing it", ar.runnerName, ar.originpath)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

func (ar *ActiveReader) setStatsError(err string) {
	ar.statsLock.Lock()
	defer ar.statsLock.Unlock()
	ar.stats.Errors++
	ar.stats.LastError = err
}

func (ar *ActiveReader) Status() utils.StatsInfo {
	ar.statsLock.RLock()
	defer ar.statsLock.RUnlock()
	return ar.stats
}

//除了sync自己的bufreader，还要sync一行linecache
func (ar *ActiveReader) SyncMeta() string {
	ar.cacheLineMux.Lock()
	defer ar.cacheLineMux.Unlock()
	ar.br.SyncMeta()
	return ar.readcache
}

func (ar *ActiveReader) expired(expireDur time.Duration) bool {
	fi, err := os.Stat(ar.realpath)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		log.Errorf("Runner[%v] stat log %v error %v,will not expire it...", ar.runnerName, ar.originpath, err)
		return false
	}
	if fi.ModTime().Add(expireDur).Before(time.Now()) && atomic.LoadInt32(&ar.inactive) > 0 {
		return true
	}
	return false
}

func NewMultiReader(meta *Meta, logPathPattern, whence, expireDur, statIntervalDur string, maxOpenFiles int) (mr *MultiReader, err error) {
	expire, err := time.ParseDuration(expireDur)
	if err != nil {
		return nil, err
	}
	statInterval, err := time.ParseDuration(statIntervalDur)
	if err != nil {
		return nil, err
	}
	_, _, bufsize, err := meta.ReadBufMeta()
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] %v recover from meta error %v, ignore...", meta.RunnerName, logPathPattern, err)
		} else {
			log.Warnf("Runner[%v] %v recover from meta error %v, ignore...", meta.RunnerName, logPathPattern, err)
		}
		bufsize = 0
		err = nil
	}

	mr = &MultiReader{
		meta:           meta,
		logPathPattern: logPathPattern,
		whence:         whence,
		expire:         expire,
		statInterval:   statInterval,
		maxOpenFiles:   maxOpenFiles,
		started:        false,
		startmux:       sync.Mutex{},
		status:         StatusInit,
		fileReaders:    make(map[string]*ActiveReader), //armapmux
		cacheMap:       make(map[string]string),        //armapmux
		armapmux:       sync.Mutex{},
		msgChan:        make(chan Result),
		statsLock:      sync.RWMutex{},
	}
	buf := make([]byte, bufsize)
	if bufsize > 0 {
		_, err = meta.ReadBuf(buf)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("Runner[%v] %v read buf error %v, ignore...", mr.meta.RunnerName, mr.Name(), err)
			} else {
				log.Warnf("Runner[%v] %v read buf error %v, ignore...", mr.meta.RunnerName, mr.Name(), err)
			}
		} else {
			err = jsoniter.Unmarshal(buf, &mr.cacheMap)
			if err != nil {
				log.Warnf("Runner[%v] %v Unmarshal read buf error %v, ignore...", mr.meta.RunnerName, mr.Name(), err)
			}
		}
		err = nil
	}
	return
}

//Expire 函数关闭过期的文件，再更新
func (mr *MultiReader) Expire() {
	var paths []string
	if atomic.LoadInt32(&mr.status) == StatusStopped {
		return
	}
	mr.armapmux.Lock()
	defer mr.armapmux.Unlock()
	if atomic.LoadInt32(&mr.status) == StatusStopped {
		return
	}
	for path, ar := range mr.fileReaders {
		if ar.expired(mr.expire) {
			ar.Close()
			delete(mr.fileReaders, path)
			delete(mr.cacheMap, path)
			paths = append(paths, path)
		}
	}
	if len(paths) > 0 {
		log.Infof("Runner[%v] expired logpath: %v", mr.meta.RunnerName, strings.Join(paths, ", "))
	}
}

func (mr *MultiReader) SetMode(mode string, value interface{}) (err error) {
	reg, err := HeadPatternMode(mode, value)
	if err != nil {
		return fmt.Errorf("%v setmode error %v", mr.Name(), err)
	}
	if reg != nil {
		mr.headRegexp = reg
	}
	return
}

func (mr *MultiReader) StatLogPath() {
	//达到最大打开文件数，不再追踪
	if len(mr.fileReaders) >= mr.maxOpenFiles {
		log.Warnf("Runner[%v] %v meet maxOpenFiles limit %v, ignore Stat new log...", mr.meta.RunnerName, mr.Name(), mr.maxOpenFiles)
		return
	}
	matches, err := filepath.Glob(mr.logPathPattern)
	if err != nil {
		log.Errorf("Runner[%v] stat logPathPattern error %v", mr.meta.RunnerName, err)
		mr.setStatsError("Runner[" + mr.meta.RunnerName + "] stat logPathPattern error " + err.Error())
		return
	}
	if len(matches) > 0 {
		log.Debugf("Runner[%v] StatLogPath %v find matches: %v", mr.meta.RunnerName, mr.logPathPattern, strings.Join(matches, ", "))
	}
	var newaddsPath []string
	for _, mc := range matches {
		rp, fi, err := utils.GetRealPath(mc)
		if err != nil {
			log.Errorf("Runner[%v] file pattern %v match %v stat error %v, ignore this match...", mr.meta.RunnerName, mr.logPathPattern, mc, err)
			continue
		}
		mr.armapmux.Lock()
		_, ok := mr.fileReaders[rp]
		mr.armapmux.Unlock()
		if ok {
			log.Debugf("Runner[%v] <%v> is collecting, ignore...", mr.meta.RunnerName, rp)
			continue
		}
		mr.armapmux.Lock()
		cacheline := mr.cacheMap[rp]
		mr.armapmux.Unlock()
		//过期的文件不追踪，除非之前追踪的并且有日志没读完
		if cacheline == "" && fi.ModTime().Add(mr.expire).Before(time.Now()) {
			log.Debugf("Runner[%v] <%v> is expired, ignore...", mr.meta.RunnerName, mc)
			continue
		}
		ar, err := NewActiveReader(mc, rp, mr.whence, mr.meta, mr.msgChan)
		if err != nil {
			log.Errorf("Runner[%v] NewActiveReader for matches %v error %v, ignore this match...", mr.meta.RunnerName, rp, err)
			continue
		}
		ar.readcache = cacheline
		if mr.headRegexp != nil {
			err = ar.br.SetMode(ReadModeHeadPatternRegexp, mr.headRegexp)
			if err != nil {
				log.Errorf("Runner[%v] NewActiveReader for matches %v SetMode error %v", mr.meta.RunnerName, rp, err)
				mr.setStatsError("Runner[" + mr.meta.RunnerName + "] NewActiveReader for matches " + rp + " SetMode error " + err.Error())
			}
		}
		newaddsPath = append(newaddsPath, rp)
		mr.armapmux.Lock()
		if atomic.LoadInt32(&mr.status) != StatusStopped {
			mr.fileReaders[rp] = ar
		} else {
			log.Warnf("Runner[%v] %v NewActiveReader but reader was stopped, ignore this...", mr.meta.RunnerName, mc)
		}
		mr.armapmux.Unlock()
		if atomic.LoadInt32(&mr.status) != StatusStopped {
			go ar.Run()
		} else {
			log.Warnf("Runner[%v] %v NewActiveReader but reader was stopped, will not running...", mr.meta.RunnerName, mc)
		}
	}
	if len(newaddsPath) > 0 {
		log.Infof("Runner[%v] StatLogPath find new logpath: %v", mr.meta.RunnerName, strings.Join(newaddsPath, ", "))
	}
}

func (mr *MultiReader) getActiveReaders() []*ActiveReader {
	mr.armapmux.Lock()
	defer mr.armapmux.Unlock()
	var ars []*ActiveReader
	for _, ar := range mr.fileReaders {
		ars = append(ars, ar)
	}
	return ars
}

func (mr *MultiReader) Name() string {
	return "MultiReader:" + mr.logPathPattern
}

func (mr *MultiReader) Source() string {
	return mr.curFile
}

func (mr *MultiReader) setStatsError(err string) {
	mr.statsLock.Lock()
	defer mr.statsLock.Unlock()
	mr.stats.LastError = err
}

func (mr *MultiReader) Status() utils.StatsInfo {
	mr.statsLock.RLock()
	defer mr.statsLock.RUnlock()

	ars := mr.getActiveReaders()
	for _, ar := range ars {
		st := ar.Status()
		if st.LastError != "" {
			mr.stats.LastError += "\n<" + ar.originpath + ">: " + st.LastError
		}
	}
	return mr.stats
}

func (mr *MultiReader) Close() (err error) {
	atomic.StoreInt32(&mr.status, StatusStopped)
	// 停10ms为了管道中的数据传递完毕，确认reader run函数已经结束不会再读取，保证syncMeta的正确性
	time.Sleep(10 * time.Millisecond)
	mr.SyncMeta()
	ars := mr.getActiveReaders()
	var wg sync.WaitGroup
	for _, ar := range ars {
		wg.Add(1)
		go func(mar *ActiveReader) {
			defer wg.Done()
			xerr := mar.Close()
			if xerr != nil {
				log.Errorf("Runner[%v] Close ActiveReader %v error %v", mr.meta.RunnerName, ar.originpath, xerr)
			}
		}(ar)
	}
	wg.Wait()
	//在所有 active readers都关闭后再close msgChan
	close(mr.msgChan)
	return
}

/*
	Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
	处理StatIntervel以及Expire两大循环任务
*/
func (mr *MultiReader) Start() {
	mr.startmux.Lock()
	defer mr.startmux.Unlock()
	if mr.started {
		return
	}
	go mr.run()
	mr.started = true
	log.Infof("%v MultiReader stat file deamon started", mr.Name())
}

func (mr *MultiReader) run() {
	for {
		if atomic.LoadInt32(&mr.status) == StatusStopped {
			log.Warnf("%v stopped from running", mr.Name())
			return
		}
		mr.Expire()
		mr.StatLogPath()
		time.Sleep(mr.statInterval)
	}
}

func (mr *MultiReader) ReadLine() (data string, err error) {
	if !mr.started {
		mr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case result := <-mr.msgChan:
		mr.curFile = result.logpath
		data = result.result
	case <-timer.C:
	}
	timer.Stop()
	return
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (mr *MultiReader) SyncMeta() {
	ars := mr.getActiveReaders()
	for _, ar := range ars {
		readcache := ar.SyncMeta()
		mr.armapmux.Lock()
		mr.cacheMap[ar.realpath] = readcache
		mr.armapmux.Unlock()
	}
	mr.armapmux.Lock()
	buf, err := jsoniter.Marshal(mr.cacheMap)
	mr.armapmux.Unlock()
	if err != nil {
		log.Errorf("%v sync meta error %v, cacheMap %v", mr.Name(), err, mr.cacheMap)
		return
	}
	err = mr.meta.WriteBuf(buf, 0, 0, len(buf))
	if err != nil {
		log.Errorf("%v sync meta WriteBuf error %v, buf %v", mr.Name(), err, string(buf))
		return
	}
	return
}
