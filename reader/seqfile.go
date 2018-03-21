package reader

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/qiniu/logkit/rateio"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"

	"github.com/qiniu/log"
)

// FileMode 读取单个文件模式
const FileMode = "file"

// DirMode 按时间顺序顺次读取文件夹下所有文件的模式
const DirMode = "dir"

const deafultFilePerm = 0600

// SeqFile 按最终修改时间依次读取文件的Reader类型
type SeqFile struct {
	meta *Meta
	mux  sync.Mutex

	dir              string   // 文件目录
	currFile         string   // 当前处理文件名
	f                *os.File // 当前处理文件
	ratereader       io.ReadCloser
	inode            uint64   // 当前文件inode
	offset           int64    // 当前处理文件offset
	ignoreHidden     bool     // 忽略隐藏文件
	ignoreFileSuffix []string // 忽略文件后缀
	newFileAsNewLine bool     //新文件自动加换行符
	validFilePattern string   // 合法的文件名正则表达式
	stopped          int32    // 停止标志位

	lastSyncPath   string
	lastSyncOffset int64
}

func getStartFile(path, whence string, meta *Meta, sf *SeqFile) (f *os.File, dir, currFile string, offset int64, err error) {
	var pfi os.FileInfo
	dir, pfi, err = GetRealPath(path)
	if err != nil || pfi == nil {
		log.Errorf("%s - utils.GetRealPath failed, err:%v", path, err)
		return
	}
	if !pfi.IsDir() {
		log.Errorf("%s -the path is not directory", dir)
		return
	}
	currFile, offset, err = meta.ReadOffset()
	if err != nil {
		switch whence {
		case WhenceOldest:
			currFile, offset, err = oldestFile(dir, sf.getIgnoreCondition())
		case WhenceNewest:
			currFile, offset, err = newestFile(dir, sf.getIgnoreCondition())
		default:
			err = errors.New("reader_whence paramter does not support: " + whence)
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

func NewSeqFile(meta *Meta, path string, ignoreHidden, newFileNewLine bool, suffixes []string, validFileRegex, whence string) (sf *SeqFile, err error) {
	sf = &SeqFile{
		ignoreFileSuffix: suffixes,
		ignoreHidden:     ignoreHidden,
		validFilePattern: validFileRegex,
		mux:              sync.Mutex{},
		newFileAsNewLine: newFileNewLine,
	}
	//原来的for循环替换成单次执行，启动的时候出错就直接报错给用户即可，不需要等待重试。
	f, dir, currFile, offset, err := getStartFile(path, whence, meta, sf)
	if err != nil {
		return
	}
	if f != nil {
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
		sf.ratereader = rateio.NewRateReader(f, meta.readlimit)
		sf.offset = offset
	} else {
		sf.inode = 0
		sf.f = nil
		sf.offset = 0
	}
	sf.meta = meta
	sf.dir = dir
	sf.currFile = currFile
	return sf, nil
}

func (sf *SeqFile) getIgnoreCondition() func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {

		if sf.ignoreHidden {
			if strings.HasPrefix(fi.Name(), ".") {
				return false
			}
		}
		for _, s := range sf.ignoreFileSuffix {
			if strings.HasSuffix(fi.Name(), s) {
				return false
			}
		}
		match, err := filepath.Match(sf.validFilePattern, fi.Name())
		if err != nil {
			log.Errorf("when read dir %s, get not valid file pattern. Error->%v", sf.dir, err)
			return false
		}
		if !match {
			log.Debugf(" when read dir %s, get no valid file in pattern %v", sf.dir, sf.validFilePattern)
		}
		return match
	}
}

func newestFile(logdir string, condition func(os.FileInfo) bool) (currFile string, offset int64, err error) {
	fi, err := getMaxFile(logdir, condition, modTimeLater)
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
	fi, err := getMinFile(logdir, condition, modTimeLater)
	if err != nil {
		return
	}
	return filepath.Join(logdir, fi.Name()), 0, err
}

func (sf *SeqFile) Name() string {
	return "SeqFile:" + sf.dir
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
	return sf.f.Close()
}

// 这个函数目前只针对stale NFS file handle的情况，重新打开文件
func (sf *SeqFile) reopenForESTALE() error {
	f, err := os.Open(sf.currFile)
	if err != nil {
		return fmt.Errorf("%s -cannot reopen currfile file err:%v", sf.currFile, err)
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
	sf.ratereader = rateio.NewRateReader(f, sf.meta.readlimit)
	ninode, err := utilsos.GetIdentifyIDByFile(f)
	if err != nil {
		//为了不影响程序运行
		log.Errorf("Runner[%v] %v getinode error %v, use old inode", sf.meta.RunnerName, sf.dir, err)
		err = nil
	} else {
		sf.inode = ninode
	}
	return nil
}

func (sf *SeqFile) Read(p []byte) (n int, err error) {
	var nextFileRetry int
	sf.mux.Lock()
	defer sf.mux.Unlock()
	n = 0
	for n < len(p) {
		var n1 int
		if sf.f == nil {
			if atomic.LoadInt32(&sf.stopped) > 0 {
				return 0, errors.New("reader " + sf.Name() + " has been exited")
			}
			err = sf.newOpen()
			if err != nil {
				log.Warnf("Runner[%v] %v new open error %v, sleep 3s and retry", sf.meta.RunnerName, sf.dir, err)
				time.Sleep(3 * time.Second)
				continue
			}
		}
		n1, err = sf.ratereader.Read(p[n:])
		if err != nil && strings.Contains(err.Error(), "stale NFS file handle") {
			nerr := sf.reopenForESTALE()
			if nerr != nil {
				log.Errorf("Runner[%v] %v meet eror %v reopen error %v", sf.meta.RunnerName, sf.dir, err, nerr)
				time.Sleep(time.Second)
			}
			continue
		}
		sf.offset += int64(n1)
		n += n1
		if err != nil {
			if err != io.EOF {
				return n, err
			}
			fi, err1 := sf.nextFile()
			if os.IsNotExist(err1) {
				if nextFileRetry >= 3 {
					return n, io.EOF
				}
				// dir removed or file rotated
				log.Debugf("Runner[%v] %s - nextFile: %v", sf.meta.RunnerName, sf.dir, err1)
				time.Sleep(WaitNoSuchFile)
				nextFileRetry++
				continue
			}
			if err1 != nil {
				log.Debugf("Runner[%v] %s - nextFile(file exist) but: %v", sf.meta.RunnerName, sf.dir, err1)
				return n, err1
			}
			if fi != nil {
				if sf.newFileAsNewLine {
					p[n] = '\n'
					n++
				}
				log.Infof("Runner[%v] %s - nextFile: %s", sf.meta.RunnerName, sf.dir, fi.Name())
				err2 := sf.open(fi)
				if err2 != nil {
					return n, err2
				}
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

func (sf *SeqFile) nextFile() (fi os.FileInfo, err error) {
	currFi, err := os.Stat(sf.currFile)
	var condition func(os.FileInfo) bool
	if err != nil {
		if !os.IsNotExist(err) {
			// 日志读取错误
			log.Errorf("Runner[%v] stat current file error %v, need retry", sf.meta.RunnerName, err)
			return
		}
		// 当前读取的文件已经被删除
		log.Warnf("Runner[%v] stat current file [%v] error %v, start to find the oldest file", sf.meta.RunnerName, sf.currFile, err)
		condition = sf.getIgnoreCondition()
	} else {
		newerThanCurrFile := func(f os.FileInfo) bool {
			return modTimeLater(f, currFi)
		}
		condition = andCondition(newerThanCurrFile, sf.getIgnoreCondition())
	}
	fi, err = getMinFile(sf.dir, condition, modTimeLater)
	if err != nil {
		log.Debugf("Runner[%v] getMinFile error %v", sf.meta.RunnerName, err)
		return nil, err
	}
	if sf.isNewFile(fi, filepath.Join(sf.dir, fi.Name())) {
		return fi, nil
	} else {
		log.Warnf("Runner[%v] %v is not new file", sf.meta.RunnerName, fi.Name())
	}
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
		log.Debugf("Runner[%v] %s - newName: %d, l.fname: %d", sf.meta.RunnerName, sf.dir, newName, fname)
		return true
	}
	return false
}

func (sf *SeqFile) newOpen() (err error) {
	fi, err1 := sf.nextFile()
	if os.IsNotExist(err1) {
		return fmt.Errorf("did'n find any file in dir %s - nextFile: %v", sf.dir, err1)
	}
	if err1 != nil {
		return fmt.Errorf("read file in dir %s error - nextFile: %v", sf.dir, err1)
	}
	if fi == nil {
		return fmt.Errorf("nextfile info in dir %v is nil", sf.dir)
	}
	fname := fi.Name()
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
	sf.ratereader = rateio.NewRateReader(f, sf.meta.readlimit)
	sf.f = f
	sf.offset = 0
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
	err = sf.f.Close()
	if err != nil && err != syscall.EINVAL {
		log.Warnf("Runner[%v] %s - %s f.Close: %v", sf.meta.RunnerName, sf.dir, sf.currFile)
		return
	}

	doneFile := sf.currFile
	fname := fi.Name()
	sf.currFile = filepath.Join(sf.dir, fname)
	for {
		f, err := os.Open(sf.currFile)
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] os.Open %s: %v", sf.meta.RunnerName, fname, err)
			time.Sleep(WaitNoSuchFile)
			continue
		}
		if err != nil {
			log.Warnf("Runner[%v] os.Open %s: %v", sf.meta.RunnerName, fname, err)
			return err
		}
		sf.f = f
		//开新的之前关掉老的
		if sf.ratereader != nil {
			sf.ratereader.Close()
		}
		sf.ratereader = rateio.NewRateReader(f, sf.meta.readlimit)
		sf.offset = 0
		sf.inode, err = utilsos.GetIdentifyIDByPath(sf.currFile)
		if err != nil {
			return err
		}
		log.Infof("Runner[%v] %s - start tail new file: %s", sf.meta.RunnerName, sf.dir, fname)
		break
	}
	tryTime := 0
	for {
		err = sf.meta.AppendDoneFile(doneFile)
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
	rl = &LagInfo{Size: -sf.offset}
	logReading := filepath.Base(sf.currFile)
	sf.mux.Unlock()

	logs, err := ReadDirByTime(sf.dir)
	if err != nil {
		err = fmt.Errorf("ReadDirByTime err %v, can't get stats", err)
		return
	}
	for _, l := range logs {
		if l.IsDir() {
			continue
		}
		rl.Size += l.Size()
		if l.Name() == logReading {
			break
		}
	}
	rl.SizeUnit = "bytes"
	return
}
