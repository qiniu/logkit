package seqfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/rateio"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

// SeqFile 按最终修改时间依次读取文件的Reader类型
type SeqFile struct {
	meta *reader.Meta
	mux  sync.Mutex

	name             string
	dir              string   // 文件目录
	currFile         string   // 当前处理文件名
	lastFile         string   //上一个处理的文件名
	f                *os.File // 当前处理文件
	ratereader       io.ReadCloser
	inode            uint64   // 当前文件inode
	offset           int64    // 当前处理文件offset
	ignoreHidden     bool     // 忽略隐藏文件
	ignoreFileSuffix []string // 忽略文件后缀

	newFileAsNewLine bool //新文件自动加换行符
	newLineNotAdded  bool //文件最后的部分正好填满buffer，导致\n符号加不上，此时要用这个变量

	newLineBytesSourceIndex []reader.SourceIndex //新文件被读取时的bytes位置
	justOpenedNewFile       bool                 //新文件刚刚打开

	validFilePattern  string // 合法的文件名正则表达式
	stopped           int32  // 停止标志位
	SkipFileFirstLine bool   //跳过新文件的第一行，常用于带title的csv文件，title与实际格式不同
	hasSkiped         bool

	inodeDone      map[string]bool //记录filename_inode是否已经读过
	inodeSensitive bool            // 是否以inode信息作为 inodeDone 和 expireMap 的key值

	lastSyncPath   string
	lastSyncOffset int64

	expireMap map[string]int64

	ReadSameInode bool //记录已经读过的filename_inode是否继续读
}

func getStartFile(path, whence string, meta *reader.Meta, sf *SeqFile) (f *os.File, dir, currFile string, offset int64, err error) {
	var pfi os.FileInfo
	dir, pfi, err = GetRealPath(path)
	if err != nil || pfi == nil {
		log.Errorf("%s - utils.GetRealPath failed, err:%v", path, err)
		return
	}
	if !pfi.IsDir() {
		err = fmt.Errorf("%s -the path is not directory", dir)
		log.Error(err)
		return
	}
	currFile, offset, err = meta.ReadOffset()
	if err != nil {
		switch whence {
		case config.WhenceOldest:
			currFile, offset, err = oldestFile(dir, sf.getIgnoreCondition())
		case config.WhenceNewest:
			currFile, offset, err = newestFile(dir, sf.getIgnoreCondition())
		default:
			err = errors.New("reader_whence parameter does not support: " + whence)
			return
		}
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
				return
			}
			err = fmt.Errorf("%s -cannot open oldest file err:%v", dir, err)
			return
		}
	} else {
		log.Debugf("%v restore meta success", dir)
	}
	f, err = os.Open(currFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		err = fmt.Errorf("%s -cannot open currfile file err:%v", currFile, err)
		return
	}
	return
}

func NewSeqFile(meta *reader.Meta, path string, ignoreHidden, newFileNewLine bool, suffixes []string, validFileRegex, whence string, expireMap map[string]int64, inodeSensitive bool) (sf *SeqFile, err error) {
	sf = &SeqFile{
		ignoreFileSuffix: suffixes,
		ignoreHidden:     ignoreHidden,
		validFilePattern: validFileRegex,
		mux:              sync.Mutex{},
		newFileAsNewLine: newFileNewLine,
		meta:             meta,
		inodeDone:        make(map[string]bool),
		expireMap:        expireMap,
		inodeSensitive:   inodeSensitive,
	}
	//原来的for循环替换成单次执行，启动的时候出错就直接报错给用户即可，不需要等待重试。
	f, dir, currFile, offset, err := getStartFile(path, whence, meta, sf)
	if err != nil {
		if strings.Contains(err.Error(), os.ErrPermission.Error()) {
			return nil, err
		}
		log.Warnf("Runner[%v] NewSeqFile reader getStartFile from dir %v error %v, will find during read...", sf.meta.RunnerName, path, err)
		err = nil
		dir = path
	}

	if f != nil {
		offset = sf.getOffset(f, offset, false)
		_, err = f.Seek(offset, io.SeekStart)
		if err != nil {
			f.Close()
			return nil, err
		}
		sf.inode, err = utilsos.GetIdentifyIDByPath(currFile)
		if err != nil {
			return nil, err
		}
		sf.f = f
		if meta.Readlimit > 0 {
			sf.ratereader = rateio.NewRateReader(f, meta.Readlimit)
		} else {
			sf.ratereader = f
		}
		sf.offset = offset
	} else {
		sf.inode = 0
		sf.f = nil
		sf.offset = 0
	}
	sf.inodeDone = meta.GetDoneFileInode(sf.inodeSensitive)
	sf.dir = dir
	sf.currFile = currFile
	sf.name = "SeqFile:" + dir
	return sf, nil
}

func (sf *SeqFile) getIgnoreCondition() func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {

		if reader.IgnoreHidden(fi.Name(), sf.ignoreHidden) {
			return false
		}
		if reader.IgnoreFileSuffixes(fi.Name(), sf.ignoreFileSuffix) {
			return false
		}
		match := reader.ValidFileRegex(fi.Name(), sf.validFilePattern)
		if !match {
			log.Debugf(" when read dir %s, get no valid file in pattern %v", sf.dir, sf.validFilePattern)
		}
		return match
	}
}

func newestFile(logdir string, condition func(os.FileInfo) bool) (currFile string, offset int64, err error) {
	fi, err := reader.GetMaxFile(logdir, condition, ModTimeLater)
	if err != nil {
		return
	}
	currFile = filepath.Join(logdir, fi.Name())
	f, err := os.Open(currFile)
	if err != nil {
		return
	}
	offset, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}
	return currFile, offset, nil

}

func oldestFile(logdir string, condition func(os.FileInfo) bool) (currFile string, offset int64, err error) {
	fi, err := reader.GetMinFile(logdir, condition, ModTimeLater)
	if err != nil {
		return
	}
	return filepath.Join(logdir, fi.Name()), 0, err
}

func (sf *SeqFile) Name() string {
	return sf.name
}

func (sf *SeqFile) Source() string {
	return sf.currFile
}

func (sf *SeqFile) Close() (err error) {
	atomic.AddInt32(&sf.stopped, 1)
	sf.mux.Lock()
	defer sf.mux.Unlock()
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.f == nil {
		return
	}

	err = sf.f.Close()
	if err != nil && err == os.ErrClosed {
		return err
	}

	return nil
}

// 这个函数目前只针对stale NFS file handle的情况，重新打开文件
func (sf *SeqFile) reopenForESTALE() error {
	log.Warnf("reopening stale NFS file handle for %q", sf.currFile)

	f, err := os.Open(sf.currFile)
	if os.IsNotExist(err) {
		return err
	}
	if err != nil {
		return fmt.Errorf("%s -cannot reopen currfile file for ESTALE err:%v", sf.currFile, err)
	}

	_, err = f.Seek(sf.offset, io.SeekStart)
	if err != nil {
		f.Close()
		return err
	}
	sf.f.Close()
	sf.f = f
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, sf.meta.Readlimit)
	} else {
		sf.ratereader = f
	}
	ninode, err := utilsos.GetIdentifyIDByFile(f)
	if err != nil {
		//为了不影响程序运行
		log.Errorf("Runner[%v] %v getinode error %v, use old inode", sf.meta.RunnerName, sf.dir, err)
	} else {
		sf.inode = ninode
	}
	return nil
}

func (sf *SeqFile) NewSourceIndex() []reader.SourceIndex {
	return sf.newLineBytesSourceIndex
}

func (sf *SeqFile) handleUnexpectErr(err error) {
	if err == io.ErrUnexpectedEOF || err == os.ErrClosed || err == os.ErrNotExist {
		sf.f = nil
		return
	}
}

func (sf *SeqFile) Read(p []byte) (n int, err error) {
	sf.newLineBytesSourceIndex = []reader.SourceIndex{}
	var nextFileRetry int
	sf.mux.Lock()
	defer sf.mux.Unlock()
	n = 0
	for n < len(p) {
		if sf.newLineNotAdded {
			p[n] = '\n'
			n++
			sf.newLineNotAdded = false
		}
		var n1 int
		if sf.f == nil {
			if atomic.LoadInt32(&sf.stopped) > 0 {
				return 0, errors.New("reader " + sf.Name() + " has been exited")
			}
			err = sf.newOpen()
			if err != nil {
				if !os.IsNotExist(err) {
					log.Warnf("Runner[%v] %v new open error %v", sf.meta.RunnerName, sf.dir, err)
				}
				//此处出错了就应该直接return，不然容易陷入死循环，让外面的runner去sleep
				return
			}
			sf.hasSkiped = false
		}
		n1, err = sf.ratereader.Read(p[n:])
		if err != nil && strings.Contains(err.Error(), "stale NFS file handle") {
			nerr := sf.reopenForESTALE()
			if nerr != nil {
				log.Errorf("Runner[%v] %v meet eror %v reopen error %v", sf.meta.RunnerName, sf.dir, err, nerr)
				return
			}
			continue
		}
		if n1 > 0 && sf.justOpenedNewFile {
			sf.justOpenedNewFile = false
			sf.newLineBytesSourceIndex = append(sf.newLineBytesSourceIndex, reader.SourceIndex{
				Source: sf.lastFile,
				Index:  n,
			})
		}
		sf.offset += int64(n1)
		n += n1
		if err != nil {
			if err != io.EOF {
				sf.handleUnexpectErr(err)
				return n, err
			}
			fi, err1 := sf.nextFile()
			if os.IsNotExist(err1) {
				if nextFileRetry >= 1 {
					if !sf.inodeSensitive {
						if err = sf.reopenCurrentFile(); err != nil {
							return n, err
						}
					}
					return n, io.EOF
				}
				// dir removed or file rotated
				log.Debugf("Runner[%v] %s - nextFile: %v", sf.meta.RunnerName, sf.dir, err1)
				time.Sleep(reader.WaitNoSuchFile)
				nextFileRetry++
				continue
			}
			if err1 != nil {
				log.Debugf("Runner[%v] %s - nextFile(file exist) but: %v", sf.meta.RunnerName, sf.dir, err1)
				return n, err1
			}
			if fi != nil {
				if sf.newFileAsNewLine {
					if n < len(p) {
						p[n] = '\n'
						n++
					} else {
						sf.newLineNotAdded = true
					}
				}
				log.Infof("Runner[%v] %s - nextFile: %s", sf.meta.RunnerName, sf.dir, fi.Name())
				err2 := sf.open(fi)
				if err2 != nil {
					return n, err2
				}
				sf.justOpenedNewFile = true
				//已经获得了下一个文件，没有EOF
				err = nil
			} else {
				time.Sleep(time.Millisecond * 500)
				return 0, io.EOF
			}
		}
	}
	return
}

func (sf *SeqFile) reopenCurrentFile() (err error) {
	fc := sf.f
	sf.f = nil
	err = fc.Close()
	if err != nil && err != syscall.EINVAL {
		log.Warnf("Runner[%s] %s - %s f.Close: %v", sf.meta.RunnerName, sf.dir, sf.currFile, err)
		return
	}

	f, err := os.Open(sf.currFile)
	if err != nil {
		log.Warnf("Runner[%v] os.Open %s: %v", sf.meta.RunnerName, sf.currFile, err)
		return err
	}
	sf.f = f
	//开新的之前关掉老的
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, sf.meta.Readlimit)
	} else {
		sf.ratereader = f
	}
	_, err = f.Seek(sf.offset, io.SeekStart)
	if err != nil {
		log.Errorf("Runner[%s] file: %s seek offset: %d failed: %v", sf.meta.RunnerName, f.Name(), sf.offset, err)
		return err
	}
	sf.inode, err = utilsos.GetIdentifyIDByPath(sf.currFile)
	if err != nil {
		return err
	}
	return
}

func (sf *SeqFile) getNextFileCondition() (condition func(os.FileInfo) bool, err error) {
	condition = sf.getIgnoreCondition()
	if sf.currFile == "" {
		var pfi os.FileInfo
		var dir string
		dir, pfi, err = GetRealPath(sf.dir)
		if err != nil || pfi == nil {
			log.Errorf("%s - utils.GetRealPath failed, err:%v", sf.dir, err)
			return
		}
		sf.dir = dir
		if !pfi.IsDir() {
			err = fmt.Errorf("%s -the path is not directory", dir)
			log.Errorf("Reader[%v]: error %v", sf.name, err)
			return
		}
		return
	}
	currFi, err := os.Stat(sf.currFile)
	if err != nil {
		if !os.IsNotExist(err) {
			// 日志读取错误
			log.Errorf("Runner[%v] stat current file error %v, need retry", sf.meta.RunnerName, err)
			return
		}
		err = nil
		// 当前读取的文件已经被删除
		log.Debugf("Runner[%v] stat current file [%v] error %v, start to find the oldest file", sf.meta.RunnerName, sf.currFile, err)
		return
	}
	newerThanCurrFile := func(f os.FileInfo) bool {
		return f.ModTime().UnixNano() >= currFi.ModTime().UnixNano()
	}

	isNewFile := func(f os.FileInfo) bool {
		inode, err := utilsos.GetIdentifyIDByPath(filepath.Join(sf.dir, f.Name()))
		if err != nil {
			log.Errorf("get %v %v inode err %v", sf.dir, f.Name(), err)
			return false
		}
		//与当前的是同一个文件
		if inode == sf.inode {
			return false
		}
		if sf.ReadSameInode {
			return true
		}

		if len(sf.inodeDone) < 1 {
			return true
		}
		var ok bool
		if sf.inodeSensitive {
			_, ok = sf.inodeDone[reader.JoinFileInode(f.Name(), strconv.FormatUint(inode, 10))]
		} else {
			_, ok = sf.inodeDone[filepath.Base(f.Name())]
		}
		return !ok
	}

	condition = reader.AndCondition(reader.AndCondition(newerThanCurrFile, sf.getIgnoreCondition()), isNewFile)
	return
}

func (sf *SeqFile) nextFile() (fi os.FileInfo, err error) {
	condition, err := sf.getNextFileCondition()
	if err != nil {
		return
	}
	fi, err = reader.GetMinFile(sf.dir, condition, ModTimeLater)
	if err != nil {
		log.Debugf("Runner[%v] getMinFile error %v", sf.meta.RunnerName, err)
		return nil, err
	}
	if sf.isNewFile(fi, filepath.Join(sf.dir, fi.Name())) {
		return fi, nil
	}
	log.Warnf("Runner[%v] %v is not new file", sf.meta.RunnerName, fi.Name())
	return nil, nil
}

func (sf *SeqFile) isNewFile(newFileInfo os.FileInfo, filePath string) bool {
	if newFileInfo == nil {
		return false
	}
	newInode, err := utilsos.GetIdentifyIDByPath(filePath)
	if err != nil {
		log.Error(err)
		return false
	}
	newName := newFileInfo.Name()
	newFsize := newFileInfo.Size()
	if newInode != 0 && sf.inode != 0 && newInode == sf.inode {
		return false
	}
	if newInode != sf.inode {
		log.Debugf("Runner[%v] %s - newInode: %d, l.inode: %d", sf.meta.RunnerName, sf.dir, newInode, sf.inode)
		return true
	}
	if newFsize < sf.offset {
		log.Debugf("Runner[%v] %s - newFsize: %d, l.offset: %d", sf.meta.RunnerName, sf.dir, newFsize, sf.offset)
		return true
	}
	fname := filepath.Base(sf.currFile)
	if newName != fname {
		log.Debugf("Runner[%s] %s - newName: %s, l.fname: %s", sf.meta.RunnerName, sf.dir, newName, fname)
		return true
	}
	return false
}

func (sf *SeqFile) newOpen() (err error) {
	fi, err1 := sf.nextFile()
	if os.IsNotExist(err1) {
		log.Debugf("can not find any file in dir %s - nextFile: %v", sf.dir, err1)
		return err1
	}
	if err1 != nil {
		return fmt.Errorf("read file in dir %s error - nextFile: %v", sf.dir, err1)
	}
	if fi == nil {
		return fmt.Errorf("nextfile info in dir %v is nil", sf.dir)
	}
	fname := fi.Name()
	sf.lastFile = sf.currFile
	sf.currFile = filepath.Join(sf.dir, fname)
	f, err := os.Open(sf.currFile)
	if os.IsNotExist(err) {
		return fmt.Errorf("os.Open %s: %v", fname, err)
	}
	if err != nil {
		return fmt.Errorf("os.Open %s: %v", fname, err)
	}
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, sf.meta.Readlimit)
	} else {
		sf.ratereader = f
	}
	sf.offset = sf.getOffset(f, 0, true)
	sf.f = f

	sf.inode, err = utilsos.GetIdentifyIDByPath(sf.currFile)
	if err != nil {
		return
	}
	return
}

func (sf *SeqFile) open(fi os.FileInfo) (err error) {
	if fi == nil {
		return
	}
	fc := sf.f
	sf.f = nil
	err = fc.Close()
	if err != nil && err != syscall.EINVAL {
		log.Warnf("Runner[%s] %s - %s f.Close: %v", sf.meta.RunnerName, sf.dir, sf.currFile, err)
		return
	}

	doneFile := sf.currFile
	doneFileInode := sf.inode
	sf.lastFile = doneFile
	fname := fi.Name()
	sf.currFile = filepath.Join(sf.dir, fname)
	f, err := os.Open(sf.currFile)
	if err != nil {
		log.Warnf("Runner[%v] os.Open %s: %v", sf.meta.RunnerName, fname, err)
		return err
	}
	sf.f = f
	//开新的之前关掉老的
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, sf.meta.Readlimit)
	} else {
		sf.ratereader = f
	}
	sf.offset = sf.getOffset(f, 0, true)
	sf.inode, err = utilsos.GetIdentifyIDByPath(sf.currFile)
	if err != nil {
		return err
	}
	log.Infof("Runner[%v] %s - start tail new file: %s", sf.meta.RunnerName, sf.dir, fname)
	if sf.inodeDone == nil {
		sf.inodeDone = make(map[string]bool)
	}
	if sf.inodeSensitive {
		sf.inodeDone[reader.JoinFileInode(doneFile, strconv.FormatUint(doneFileInode, 10))] = true
	} else {
		sf.inodeDone[filepath.Base(doneFile)] = true
	}
	tryTime := 0
	for {
		err = sf.meta.AppendDoneFileInode(doneFile, doneFileInode)
		if err != nil {
			if tryTime > 3 {
				log.Errorf("Runner[%v] cannot write done file %s, err:%v, ignore this noefi", sf.meta.RunnerName, doneFile, err)
				break
			}
			log.Errorf("Runner[%v] cannot write done file %s, err:%v, will retry after 3s", sf.meta.RunnerName, doneFile, err)
			time.Sleep(3 * time.Second)
			tryTime++
			continue
		}
		break
	}
	return
}

func (sf *SeqFile) SyncMeta() (err error) {
	sf.mux.Lock()
	defer sf.mux.Unlock()
	if sf.lastSyncOffset == sf.offset && sf.lastSyncPath == sf.currFile {
		log.Debugf("Runner[%v] %v was just syncd %v %v ignore it...", sf.meta.RunnerName, sf.Name(), sf.lastSyncPath, sf.lastSyncOffset)
		return nil
	}
	sf.lastSyncOffset = sf.offset
	sf.lastSyncPath = sf.currFile
	return sf.meta.WriteOffset(sf.currFile, sf.offset)
}

func (sf *SeqFile) Lag() (rl *LagInfo, err error) {
	sf.mux.Lock()
	rl = &LagInfo{Size: -sf.offset, SizeUnit: "bytes"}
	logReading := filepath.Base(sf.currFile)
	sf.mux.Unlock()

	inode, err := utilsos.GetIdentifyIDByPath(sf.currFile)
	if os.IsNotExist(err) || (inode != 0 && inode != sf.inode) {
		rl.Size = 0
		err = nil
	}
	if err != nil {
		rl.Size = 0
		return rl, err
	}

	logs, err := ReadDirByTime(sf.dir)
	if err != nil {
		return rl, fmt.Errorf("ReadDirByTime err %v, can't get stats", err)
	}
	condition := sf.getIgnoreCondition()
	for _, l := range logs {
		if l.IsDir() {
			continue
		}
		if condition == nil || !condition(l) {
			continue
		}
		rl.Size += l.Size()
		if l.Name() == logReading {
			break
		}
	}

	return rl, nil
}

func (sf *SeqFile) IsNewOpen() bool {
	if sf.SkipFileFirstLine {
		return !sf.hasSkiped
	}
	return false
}

func (sf *SeqFile) SetSkipped() {
	sf.hasSkiped = true
}

type LineSkipper interface {
	IsNewOpen() bool
	SetSkipped()
}

func (sf *SeqFile) getOffset(f *os.File, offset int64, seek bool) int64 {
	if len(sf.expireMap) == 0 || offset != 0 || f == nil {
		return offset
	}

	if sf.meta.IsExist() {
		deleteNotExist(filepath.Dir(f.Name()), sf.expireMap, sf.inodeSensitive)
		return offset
	}

	fileName := f.Name()
	inode, err := utilsos.GetIdentifyIDByPath(fileName)
	if err != nil {
		log.Errorf("Runner[%s] NewSeqFile get file %s inode error %v, ignore...", sf.meta.RunnerName, fileName, err)
		return offset
	}
	inodeStr := strconv.FormatUint(inode, 10)
	if sf.inodeSensitive {
		offset = sf.expireMap[inodeStr+"_"+fileName]
	} else {
		offset = sf.expireMap[fileName]
	}
	if seek {
		_, err = f.Seek(sf.offset, io.SeekStart)
		if err != nil {
			log.Errorf("Runner[%s] file: %s seek offset: %d failed: %v", sf.meta.RunnerName, f.Name(), sf.offset, err)
		}
	}
	return offset
}

func deleteNotExist(dir string, expireMap map[string]int64, inodeSensitive bool) {
	if dir == "" {
		return
	}
	var arr []string
	for inodeFile := range expireMap {
		if inodeSensitive {
			arr = strings.SplitN(inodeFile, "_", 2)
			if len(arr) < 2 {
				continue
			}
			if filepath.Dir(arr[1]) != dir {
				continue
			}
		} else {
			if filepath.Dir(inodeFile) != dir {
				continue
			}
		}
		delete(expireMap, inodeFile)
	}
}

var (
	_ LineSkipper              = new(SeqFile)
	_ reader.NewSourceRecorder = new(SeqFile)
)
