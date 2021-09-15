package singlefile

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/rateio"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

type SingleFile struct {
	realpath   string // 处理文件路径
	originpath string
	pfi        os.FileInfo // path 的文件信息
	f          *os.File    // 当前处理文件
	ratereader io.ReadCloser
	offset     int64 // 当前处理文件offset
	stopped    int32

	lastSyncPath   string
	lastSyncOffset int64

	mux  sync.Mutex
	meta *reader.Meta // 记录offset的元数据
}

func NewSingleFile(meta *reader.Meta, path, whence string, originOffset int64, errDirectReturn bool) (sf *SingleFile, err error) {
	var pfi os.FileInfo
	var f *os.File
	originpath := path
	backoff := utils.NewBackoff(2, 4, 1*time.Minute, 10*time.Minute)
	for {
		path, pfi, err = GetRealPath(path)
		if err != nil || pfi == nil {
			if errDirectReturn {
				return sf, fmt.Errorf("runner[%v] %s - utils.GetRealPath failed, err:%v", meta.RunnerName, path, err)
			}
			if !IsSelfRunner(meta.RunnerName) {
				log.Warnf("Runner[%v] %s - utils.GetRealPath failed, err:%v", meta.RunnerName, path, err)
			} else {
				log.Debugf("Runner[%v] %s - utils.GetRealPath failed, err:%v", meta.RunnerName, path, err)
			}
			time.Sleep(backoff.Duration())
			continue
		}
		if !pfi.Mode().IsRegular() {
			if errDirectReturn {
				return sf, fmt.Errorf("runner[%v] %s - file failed, err: file is not regular", meta.RunnerName, path)
			}
			if !IsSelfRunner(meta.RunnerName) {
				log.Warnf("Runner[%v] %s - file failed, err: file is not regular", meta.RunnerName, path)
			} else {
				log.Debugf("Runner[%v] %s - file failed, err: file is not regular", meta.RunnerName, path)
			}
			time.Sleep(backoff.Duration())
			continue
		}
		f, err = os.Open(path)
		if err != nil {
			if errDirectReturn {
				return sf, fmt.Errorf("runner[%v] %s - open file err:%v", meta.RunnerName, path, err)
			}
			if !IsSelfRunner(meta.RunnerName) {
				log.Warnf("Runner[%v] %s - open file err:%v", meta.RunnerName, path, err)
			} else {
				log.Debugf("Runner[%v] %s - open file err:%v", meta.RunnerName, path, err)
			}
			time.Sleep(backoff.Duration())
			continue
		}
		break
	}

	omitMeta := false
	metafile, offset, err := meta.ReadOffset()
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
		} else {
			if !IsSelfRunner(meta.RunnerName) {
				log.Warnf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
			} else {
				log.Debugf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
			}
		}
		omitMeta = true
	}
	if metafile != originpath {
		if !IsSelfRunner(meta.RunnerName) {
			log.Warnf("Runner[%v] %v -meta file <%v> is not current file <%v>， omit meta data", meta.RunnerName, meta.MetaFile(), metafile, originpath)
		} else {
			log.Debugf("Runner[%v] %v -meta file <%v> is not current file <%v>， omit meta data", meta.RunnerName, meta.MetaFile(), metafile, originpath)
		}
		omitMeta = true
	}

	sf = &SingleFile{
		meta:       meta,
		realpath:   path,
		originpath: originpath,
		pfi:        pfi,
		f:          f,
		mux:        sync.Mutex{},
	}

	if meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, meta.Readlimit)
	} else {
		sf.ratereader = f
	}

	// 如果meta初始信息损坏或者没有meta信息
	if omitMeta {
		if originOffset != 0 {
			offset = originOffset // 使用传入offset
			if _, err = sf.f.Seek(offset, io.SeekStart); err != nil {
				return nil, err
			}
		} else {
			offset, err = sf.startOffset(whence)
			if err != nil {
				return nil, err
			}
		}
	} else {
		log.Debugf("Runner[%v] %v restore meta success", sf.meta.RunnerName, sf.Name())
	}

	sf.offset = offset
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	//遇到Offset超过最大的文件了,重新来过
	if sf.offset > st.Size() {
		sf.offset = 0
	}
	_, err = f.Seek(sf.offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return sf, nil
}

func (sf *SingleFile) statFile(path string) (pfi os.FileInfo, err error) {

	for {
		if atomic.LoadInt32(&sf.stopped) > 0 {
			return pfi, errors.New("reader " + sf.Name() + " has been exited")
		}
		path, pfi, err = GetRealPath(path)
		if err != nil || pfi == nil {
			if !IsSelfRunner(sf.meta.RunnerName) {
				log.Warnf("Runner[%v] %s - utils.GetRealPath failed, err:%v", sf.meta.RunnerName, path, err)
			} else {
				log.Debugf("Runner[%v] %s - utils.GetRealPath failed, err:%v", sf.meta.RunnerName, path, err)
			}
			time.Sleep(time.Minute)
			continue
		}
		break
	}
	return
}

func (sf *SingleFile) openSingleFile(path string) (pfi os.FileInfo, f *os.File, err error) {
	path, pfi, err = GetRealPath(path)
	if err != nil || pfi == nil {
		return nil, nil, fmt.Errorf("runner[%v] %s - utils.GetRealPath failed, err:%v", sf.meta.RunnerName, path, err)
	}
	if !pfi.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("runner[%v] %s - file failed, err: file is not regular", sf.meta.RunnerName, path)
	}
	f, err = os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("runner[%v] %s - open file err:%v", sf.meta.RunnerName, path, err)
	}
	return pfi, f, nil
}

func (sf *SingleFile) startOffset(whence string) (int64, error) {
	switch whence {
	case config.WhenceOldest:
		return 0, nil
	case config.WhenceNewest:
		return sf.f.Seek(0, io.SeekEnd)
	default:
		return 0, errors.New("whence not supported " + whence)
	}
}

func (sf *SingleFile) Name() string {
	return "SingleFile:" + sf.originpath
}

func (sf *SingleFile) Source() string {
	return sf.originpath
}

func (sf *SingleFile) Close() (err error) {
	atomic.AddInt32(&sf.stopped, 1)
	sf.mux.Lock()
	defer sf.mux.Unlock()
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.f != nil {
		err = sf.f.Close()
		if err != nil && err != os.ErrClosed {
			return err
		}
	}
	return nil
}

func (sf *SingleFile) detectMovedName(inode uint64) (name string) {
	dir := filepath.Dir(sf.realpath)
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		if !IsSelfRunner(sf.meta.RunnerName) {
			log.Errorf("Runner[%v] read SingleFile path %v err %v", sf.meta.RunnerName, dir, err)
		} else {
			log.Debugf("Runner[%v] read SingleFile path %v err %v", sf.meta.RunnerName, dir, err)
		}
		return
	}
	for _, fi := range fis {
		// 前缀过滤
		if fi.IsDir() || !strings.HasPrefix(fi.Name(), sf.pfi.Name()) {
			continue
		}
		newInode, err := utilsos.GetIdentifyIDByPath(filepath.Join(dir, fi.Name()))
		if err != nil {
			if !IsSelfRunner(sf.meta.RunnerName) {
				log.Error(err)
			} else {
				log.Debug(err)
			}
			continue
		}
		if newInode == inode {
			name = filepath.Join(dir, fi.Name())
			return
		}
	}
	return
}

func (sf *SingleFile) Reopen() (err error) {
	newInode, err := utilsos.GetIdentifyIDByPath(sf.originpath)
	if err != nil {
		return
	}
	oldInode, err := utilsos.GetIdentifyIDByFile(sf.f)
	if err != nil {
		return
	}
	doneFileOffset := sf.offset

	if newInode == oldInode {
		return
	}
	sf.f.Close()
	sf.f = nil
	detectStr := sf.detectMovedName(oldInode)
	if detectStr != "" {
		if derr := sf.meta.AppendDoneFileInode(detectStr, oldInode, doneFileOffset); derr != nil {
			if !IsSelfRunner(sf.meta.RunnerName) {
				log.Errorf("Runner[%v] AppendDoneFile %v error %v", sf.meta.RunnerName, detectStr, derr)
			} else {
				log.Debugf("Runner[%v] AppendDoneFile %v error %v", sf.meta.RunnerName, detectStr, derr)
			}
		}
	} else {
		detectStr = "not detected"
	}
	if !IsSelfRunner(sf.meta.RunnerName) {
		log.Infof("Runner[%v] rotate %s successfully , rotated file is <%v>", sf.meta.RunnerName, sf.originpath, detectStr)
	} else {
		log.Debugf("Runner[%v] rotate %s successfully , rotated file is <%v>", sf.meta.RunnerName, sf.originpath, detectStr)
	}
	pfi, f, err := sf.openSingleFile(sf.originpath)
	if err != nil {
		return
	}
	sf.pfi = pfi
	sf.f = f
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, sf.meta.Readlimit)
	} else {
		sf.ratereader = f
	}
	sf.offset = 0
	return
}

func (sf *SingleFile) reopenForESTALE() (err error) {
	f, err := os.Open(sf.originpath)
	if err != nil {
		return
	}
	pfi, err := f.Stat()
	if err != nil {
		f.Close()
		return
	}
	_, err = f.Seek(sf.offset, io.SeekStart)
	if err != nil {
		f.Close()
		return
	}
	sf.f.Close()
	sf.pfi = pfi
	sf.f = f
	if sf.ratereader != nil {
		sf.ratereader.Close()
	}
	if sf.meta.Readlimit > 0 {
		sf.ratereader = rateio.NewRateReader(f, sf.meta.Readlimit)
	} else {
		sf.ratereader = f
	}
	return
}

func (sf *SingleFile) Read(p []byte) (n int, err error) {
	if atomic.LoadInt32(&sf.stopped) > 0 {
		return 0, errors.New("reader " + sf.Name() + " has been exited")
	}
	sf.mux.Lock()
	defer sf.mux.Unlock()
	eofTimes := 0
	n1 := 0
	n = 0
	for n < len(p) && eofTimes <= 2 {
		n1, err = sf.ratereader.Read(p[n:])
		if err != nil && strings.Contains(err.Error(), "stale NFS file handle") {
			nerr := sf.reopenForESTALE()
			if nerr != nil {
				if !IsSelfRunner(sf.meta.RunnerName) {
					log.Errorf("Runner[%v] %v meet eror %v reopen error %v", sf.meta.RunnerName, sf.originpath, err, nerr)
				} else {
					log.Debugf("Runner[%v] %v meet eror %v reopen error %v", sf.meta.RunnerName, sf.originpath, err, nerr)
				}
			}
			return n, err
		}
		if n1 > 0 {
			eofTimes = 0
			n += n1
			sf.offset += int64(n1)
		}
		if err == io.EOF {
			if n1 > 0 {
				err = nil
			}
			time.Sleep(time.Millisecond * 10)
			eofTimes++
			err1 := sf.Reopen()
			if err1 != nil {
				return n, err1
			}
			continue
		} else if err != nil {
			return n, err
		}
	}
	return n, err
}

func (sf *SingleFile) SyncMeta() error {
	sf.mux.Lock()
	defer sf.mux.Unlock()
	if sf.lastSyncOffset == sf.offset && sf.lastSyncPath == sf.originpath {
		log.Debugf("Runner[%v] %v was just syncd %v %v ignore it...", sf.meta.RunnerName, sf.Name(), sf.lastSyncPath, sf.lastSyncOffset)
		return nil
	}
	log.Debugf("Runner[%v] %v Sync file success: %v", sf.meta.RunnerName, sf.Name(), sf.offset)
	sf.lastSyncOffset = sf.offset
	sf.lastSyncPath = sf.originpath
	return sf.meta.WriteOffset(sf.originpath, sf.offset)
}

func (sf *SingleFile) Lag() (rl *LagInfo, err error) {
	sf.mux.Lock()
	rl = &LagInfo{Size: -sf.offset, SizeUnit: "bytes"}
	sf.mux.Unlock()

	fi, err := os.Stat(sf.originpath)
	if os.IsNotExist(err) {
		rl.Size = 0
		return rl, nil
	}

	if err != nil {
		rl.Size = 0
		return rl, err
	}
	rl.Size += fi.Size()

	return rl, nil
}
