package reader

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils"
)

type MultiReader struct {
	started         bool
	status          int32
	fileReaders     map[uint64]*ActiveReader
	scs             []reflect.SelectCase //在updateSelectCase中更新
	scs2Inode       []uint64             //在updateSelectCase中更新
	mux             sync.Mutex
	curDataLogInode uint64
	headRegexp      *regexp.Regexp
	cacheMap        map[string]string

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
		status:         StatusInit,
		fileReaders:    make(map[uint64]*ActiveReader),
		scs:            make([]reflect.SelectCase, 0),
		scs2Inode:      make([]uint64, 0),
		mux:            sync.Mutex{},
		cacheMap:       make(map[string]string),
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

//Expire函数关闭过期的文件，先要remove SelectCase，再close
func (mr *MultiReader) Expire() {
	var removes []uint64
	for inode, ar := range mr.fileReaders {
		fi, err := os.Stat(ar.logpath)
		if err != nil {
			if os.IsNotExist(err) {
				removes = append(removes, inode)
			} else {
				log.Errorf("stat log %v error %v", ar.logpath, err)
			}
			continue
		}
		if fi.ModTime().Add(mr.expire).Before(time.Now()) && ar.inactive {
			removes = append(removes, inode)
		}
	}
	mr.removeSelectCase(removes)
	for _, inode := range removes {
		ar, ok := mr.fileReaders[inode]
		if ok {
			ar.Close()
		}
		delete(mr.fileReaders, inode)
		delete(mr.cacheMap, strconv.FormatUint(inode, 10))
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
	var newaddsInode []uint64
	for _, m := range matches {
		m, fi, err := utils.GetRealPath(m)
		if err != nil {
			log.Errorf("file pattern %v match %v stat error %v, ignore this match...", mr.logPathPattern, m, err)
			continue
		}
		inode := getInode(fi)
		if _, ok := mr.fileReaders[inode]; ok {
			log.Debugf("%v %vis exist,ignore...", inode, m)
			continue
		}
		cacheline := mr.cacheMap[strconv.FormatUint(inode, 10)]
		//过期的文件不追踪，除非之前追踪的并且有日志没读完
		if cacheline == "" && fi.ModTime().Add(mr.expire).Before(time.Now()) {
			continue
		}
		ar, err := NewActiveReader(m, mr.whence, mr.meta)
		if err != nil {
			log.Errorf("NewActiveReader for matches %v error %v, ignore this match...", m, err)
			continue
		}
		ar.readcache = cacheline
		newaddsPath = append(newaddsPath, m)
		newaddsInode = append(newaddsInode, inode)
		if mr.headRegexp != nil {
			err = ar.br.SetMode(ReadModeHeadPatternRegexp, mr.headRegexp)
			if err != nil {
				log.Errorf("NewActiveReader for matches %v SetMode error %v", m, err)
			}
		}
		go ar.Run()
		mr.fileReaders[inode] = ar
	}
	if len(newaddsInode) > 0 {
		log.Debugf("StatLogPath find new logpath: %v; %v", strings.Join(newaddsPath, ", "), newaddsInode)
		mr.addSelectCase(newaddsInode)
	}
}

func (mr *MultiReader) addSelectCase(updates []uint64) {
	mr.mux.Lock()
	defer mr.mux.Unlock()
	for _, inode := range updates {
		ar, ok := mr.fileReaders[inode]
		if !ok {
			log.Errorf("addSelectCase inode %v, but not found in fileReaders map", inode)
			continue
		}
		mr.scs = append(mr.scs, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ar.msgchan),
		})
		mr.scs2Inode = append(mr.scs2Inode, inode)
	}
}

func (mr *MultiReader) removeSelectCase(updates []uint64) {
	checkIn := func(cs []uint64, c uint64) bool {
		for _, x := range cs {
			if c == x {
				return true
			}
		}
		return false
	}
	var reservedIdx []int
	for i, ix := range mr.scs2Inode {
		if !checkIn(updates, ix) {
			reservedIdx = append(reservedIdx, i)
		}
	}

	newScs := make([]reflect.SelectCase, 0)
	newScs2Inode := make([]uint64, 0)
	for _, i := range reservedIdx {
		if i >= len(mr.scs) {
			log.Errorf("removeSelectCase but found index %v is out of range %v ", i, len(mr.scs))
			continue
		}
		newScs = append(newScs, mr.scs[i])
		newScs2Inode = append(newScs2Inode, mr.scs2Inode[i])
	}
	mr.mux.Lock()
	defer mr.mux.Unlock()
	mr.scs = newScs
	mr.scs2Inode = newScs2Inode
}

func (mr *MultiReader) Name() string {
	return "MultiReader:" + mr.logPathPattern
}

func (mr *MultiReader) Source() string {
	if mr.curDataLogInode == 0 {
		return ""
	}
	if ar, ok := mr.fileReaders[mr.curDataLogInode]; ok {
		return ar.logpath
	}
	log.Errorf("%v not exist in tailx multireader", mr.curDataLogInode)
	return ""
}

func (mr *MultiReader) Close() (err error) {
	for _, ar := range mr.fileReaders {
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
	mr.mux.Lock()
	defer mr.mux.Unlock()
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
		time.Sleep(time.Second)
		return
	}

	//reflect.Select blocks until at least one of the cases can proceed
	chosen, value, ok := reflect.Select(mr.scs)
	if ok {
		mr.curDataLogInode = mr.scs2Inode[chosen]
		data = value.String()
	}
	return
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (mr *MultiReader) SyncMeta() {
	for inode, ar := range mr.fileReaders {
		ar.SyncMeta()
		mr.cacheMap[strconv.FormatUint(inode, 10)] = ar.readcache
	}
	buf, err := json.Marshal(mr.cacheMap)
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
