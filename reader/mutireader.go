package reader

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils"
)

type MultiReader struct {
	started     bool
	status      int32
	fileReaders map[string]*ActiveReader
	scs         []reflect.SelectCase //在updateSelectCase中更新
	scs2File    []string             //在updateSelectCase中更新
	armapmux    sync.Mutex
	scsmux      sync.Mutex
	startmux    sync.Mutex
	curFile     string
	headRegexp  *regexp.Regexp
	cacheMap    map[string]string

	//以下为传入参数
	meta           *Meta
	logPathPattern string
	expire         time.Duration
	statInterval   time.Duration
	maxOpenFiles   int
	whence         string
}

type ActiveReader struct {
	br        *BufReader
	logpath   string
	readcache string
	msgchan   chan string
	status    int32
	inactive  bool //当inactive为true时才会被expire回收
}

func NewActiveReader(logPath, whence string, meta *Meta) (ar *ActiveReader, err error) {
	rpath := strings.Replace(logPath, string(os.PathSeparator), "_", -1)
	subMetaPath := filepath.Join(meta.dir, rpath)
	subMeta, err := NewMeta(subMetaPath, subMetaPath, logPath, ModeFile, defautFileRetention)
	if err != nil {
		return nil, err
	}
	fr, err := NewSingleFile(subMeta, logPath, whence)
	if err != nil {
		return
	}
	bf, err := NewReaderSize(fr, subMeta, defaultBufSize)
	if err != nil {
		return
	}
	ar = &ActiveReader{
		br:       bf,
		logpath:  logPath,
		msgchan:  make(chan string),
		inactive: false,
	}
	return
}

func (ar *ActiveReader) Run() {
	var err error
	defer close(ar.msgchan)
	timer := time.NewTicker(50 * time.Millisecond)
	for {
		if atomic.LoadInt32(&ar.status) == StatusStopped {
			return
		}
		if ar.readcache == "" {
			ar.readcache, err = ar.br.ReadLine()
			if err != nil && err != io.EOF {
				log.Warnf("ActiveReader %s read error: %v", ar.logpath, err)
				time.Sleep(3 * time.Second)
				continue
			}
			//文件EOF，同时没有任何内容，代表不是第一次EOF，休息时间设置长一些
			if ar.readcache == "" && err == io.EOF {
				ar.inactive = true
				log.Debugf("%v ActiveReader was inactive, sleep 10 seconds", ar.logpath)
				time.Sleep(10 * time.Second)
				continue
			}
		}
		log.Debugf(">>>>>>readcache <%v> linecache <%v>", ar.readcache, ar.br.lineCache)
		for {
			if ar.readcache == "" {
				break
			}
			ar.inactive = false
			//做这一层结构为了快速结束
			if atomic.LoadInt32(&ar.status) == StatusStopped {
				log.Debugf("%v ActiveReader was exited when waiting to send data", ar.logpath)
				return
			}
			select {
			case ar.msgchan <- ar.readcache:
				ar.readcache = ""
			case <-timer.C:
			}
		}
	}
}
func (ar *ActiveReader) Close() error {
	atomic.StoreInt32(&ar.status, StatusStopped)
	err := ar.br.Close()
	return err
}

//除了sync自己的bufreader，还要sync一行linecache
func (ar *ActiveReader) SyncMeta() {
	ar.br.SyncMeta()
}

func (ar *ActiveReader) expired(expireDur time.Duration) bool {
	fi, err := os.Stat(ar.logpath)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		log.Errorf("stat log %v error %v", ar.logpath, err)
	}
	if fi.ModTime().Add(expireDur).Before(time.Now()) && ar.inactive {
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
			log.Debugf("%v recover from meta error %v, ignore...", logPathPattern, err)
		} else {
			log.Warnf("%v recover from meta error %v, ignore...", logPathPattern, err)
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
		scs:            make([]reflect.SelectCase, 0), //scsmux
		scs2File:       make([]string, 0),             //scsmux
		scsmux:         sync.Mutex{},
	}
	buf := make([]byte, bufsize)
	if bufsize > 0 {
		_, err = meta.ReadBuf(buf)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("%v read buf error %v, ignore...", mr.Name(), err)
			} else {
				log.Warnf("%v read buf error %v, ignore...", mr.Name(), err)
			}
		} else {
			err = json.Unmarshal(buf, &mr.cacheMap)
			if err != nil {
				log.Warnf("%v Unmarshal read buf error %v, ignore...", mr.Name(), err)
			}
		}
		err = nil
	}
	return
}

//Expire 函数关闭过期的文件，再更新
func (mr *MultiReader) Expire() {
	var paths []string
	mr.armapmux.Lock()
	for path, ar := range mr.fileReaders {
		if ar.expired(mr.expire) {
			go ar.Close()
			delete(mr.fileReaders, path)
			delete(mr.cacheMap, path)
			paths = append(paths, path)
		}
	}
	mr.armapmux.Unlock()

	if len(paths) > 0 {
		log.Infof("Expire reader find expired logpath: %v", strings.Join(paths, ", "))
		mr.updateSelectCase()
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
		log.Warnf("%v meet maxOpenFiles limit %v, ignore Stat new log...", mr.Name(), mr.maxOpenFiles)
		return
	}
	matches, err := filepath.Glob(mr.logPathPattern)
	if err != nil {
		log.Errorf("stat logPathPattern error %v", err)
		return
	}
	var newaddsPath []string
	for _, mc := range matches {
		rp, fi, err := utils.GetRealPath(mc)
		if err != nil {
			log.Errorf("file pattern %v match %v stat error %v, ignore this match...", mr.logPathPattern, mc, err)
			continue
		}
		mr.armapmux.Lock()
		_, ok := mr.fileReaders[rp]
		mr.armapmux.Unlock()
		if ok {
			log.Debugf("<%v> is collecting, ignore...", rp)
			continue
		}
		mr.armapmux.Lock()
		cacheline := mr.cacheMap[rp]
		mr.armapmux.Unlock()
		//过期的文件不追踪，除非之前追踪的并且有日志没读完
		if cacheline == "" && fi.ModTime().Add(mr.expire).Before(time.Now()) {
			continue
		}
		ar, err := NewActiveReader(rp, mr.whence, mr.meta)
		if err != nil {
			log.Errorf("NewActiveReader for matches %v error %v, ignore this match...", rp, err)
			continue
		}
		ar.readcache = cacheline
		if mr.headRegexp != nil {
			err = ar.br.SetMode(ReadModeHeadPatternRegexp, mr.headRegexp)
			if err != nil {
				log.Errorf("NewActiveReader for matches %v SetMode error %v", rp, err)
			}
		}
		newaddsPath = append(newaddsPath, rp)
		go ar.Run()
		mr.armapmux.Lock()
		mr.fileReaders[rp] = ar
		mr.armapmux.Unlock()
	}
	if len(newaddsPath) > 0 {
		log.Infof("StatLogPath find new logpath: %v", strings.Join(newaddsPath, ", "))
		mr.updateSelectCase()
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

func (mr *MultiReader) updateSelectCase() {
	ars := mr.getActiveReaders()
	var newScs []reflect.SelectCase
	var newScs2files []string
	for _, ar := range ars {
		newScs = append(newScs, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ar.msgchan),
		})
		newScs2files = append(newScs2files, ar.logpath)
	}
	mr.scsmux.Lock()
	mr.scs = newScs
	mr.scs2File = newScs2files
	mr.scsmux.Unlock()
}

func (mr *MultiReader) Name() string {
	return "MultiReader:" + mr.logPathPattern
}

func (mr *MultiReader) Source() string {
	return mr.curFile
}

func (mr *MultiReader) Close() (err error) {
	ars := mr.getActiveReaders()
	for _, ar := range ars {
		err = ar.Close()
		if err != nil {
			log.Errorf("Close ActiveReader %v error %v", ar.logpath, err)
		}
	}
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
	log.Printf("%v MultiReader stat file deamon started", mr.Name())
}

func (mr *MultiReader) run() {
	for {
		if atomic.LoadInt32(&mr.status) == StatusStoping {
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
	if len(mr.scs) <= 0 {
		return
	}
	mr.scsmux.Lock()
	defer mr.scsmux.Unlock()
	//double check
	if len(mr.scs) <= 0 {
		return
	}
	//reflect.Select blocks until at least one of the cases can proceed
	chosen, value, ok := reflect.Select(mr.scs)
	if ok {
		mr.curFile = mr.scs2File[chosen]
		data = value.String()
	}
	return
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (mr *MultiReader) SyncMeta() {
	ars := mr.getActiveReaders()
	for _, ar := range ars {
		ar.SyncMeta()
		mr.armapmux.Lock()
		mr.cacheMap[ar.logpath] = ar.readcache
		mr.armapmux.Unlock()
	}
	mr.armapmux.Lock()
	buf, err := json.Marshal(mr.cacheMap)
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
