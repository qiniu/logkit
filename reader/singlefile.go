package reader

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/qiniu/logkit/utils"

	"github.com/qiniu/log"
)

type SingleFile struct {
	path    string      // 处理文件路径
	pfi     os.FileInfo // path 的文件信息
	f       *os.File    // 当前处理文件
	offset  int64       // 当前处理文件offset
	stopped int32

	lastSyncPath   string
	lastSyncOffset int64

	meta *Meta // 记录offset的元数据
}

func NewSingleFile(meta *Meta, path, whence string) (sf *SingleFile, err error) {
	var pfi os.FileInfo
	var f *os.File

	for {
		path, pfi, err = utils.GetRealPath(path)
		if err != nil || pfi == nil {
			log.Warnf("%s - utils.GetRealPath failed, err:%v", path, err)
			time.Sleep(time.Minute)
			continue
		}
		if !pfi.Mode().IsRegular() {
			log.Warnf("%s - file failed, err: file is not regular", path)
			time.Sleep(time.Minute)
			continue
		}
		f, err = os.Open(path)
		if err != nil {
			log.Warnf("%s - open file err:%v", path, err)
			time.Sleep(time.Minute)
			continue
		}
		break
	}

	omitMeta := false
	metafile, offset, err := meta.ReadOffset()
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("%v -meta data is corrupted err:%v, omit meta data", meta.MetaFile(), err)
		} else {
			log.Warnf("%v -meta data is corrupted err:%v, omit meta data", meta.MetaFile(), err)
		}
		omitMeta = true
	}
	if metafile != path {
		log.Warnf("%v -meta file <%v> is not current file <%v>， omit meta data", meta.MetaFile(), metafile, path)
		omitMeta = true
	}

	sf = &SingleFile{
		meta: meta,
		path: path,
		pfi:  pfi,
		f:    f,
	}

	// 如果meta初始信息损坏
	if omitMeta {
		offset, err = sf.startOffset(whence)
		if err != nil {
			return nil, err
		}
	} else {
		log.Debugf("%v restore meta success", sf.Name())
	}
	sf.offset = offset
	f.Seek(offset, os.SEEK_SET)
	return sf, nil
}

func (sf *SingleFile) statFile(path string) (pfi os.FileInfo, err error) {

	for {
		if atomic.LoadInt32(&sf.stopped) > 0 {
			err = errors.New("reader " + sf.Name() + " has been exited")
			return
		}
		path, pfi, err = utils.GetRealPath(path)
		if err != nil || pfi == nil {
			log.Warnf("%s - utils.GetRealPath failed, err:%v", path, err)
			time.Sleep(time.Minute)
			continue
		}
		break
	}
	return
}

func (sf *SingleFile) openSingleFile(path string) (pfi os.FileInfo, f *os.File, err error) {

	for {
		if atomic.LoadInt32(&sf.stopped) > 0 {
			err = errors.New("reader " + sf.Name() + " has been exited")
			return
		}

		path, pfi, err = utils.GetRealPath(path)
		if err != nil || pfi == nil {
			log.Warnf("%s - utils.GetRealPath failed, err:%v", path, err)
			time.Sleep(time.Minute)
			continue
		}
		if !pfi.Mode().IsRegular() {
			log.Warnf("%s - file failed, err: file is not regular", path)
			time.Sleep(time.Minute)
			continue
		}
		f, err = os.Open(path)
		if err != nil {
			log.Warnf("%s - open file err:%v", path, err)
			time.Sleep(time.Minute)
			continue
		}
		break
	}
	return
}

func (sf *SingleFile) startOffset(whence string) (int64, error) {
	switch whence {
	case WhenceOldest:
		return 0, nil
	case WhenceNewest:
		return sf.f.Seek(0, os.SEEK_END)
	default:
		return 0, errors.New("whence not supported " + whence)
	}
}

func (sf *SingleFile) Name() string {
	return "SingleFile:" + sf.path
}

func (sf *SingleFile) Source() string {
	return sf.path
}

func (sf *SingleFile) Close() (err error) {
	atomic.AddInt32(&sf.stopped, 1)
	return sf.f.Close()
}

func (sf *SingleFile) detectMovedName(inode uint64) (name string) {
	dir := filepath.Dir(sf.path)
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Errorf("read SingleFile path %v err %v", dir, err)
		return
	}
	for _, fi := range fis {
		// 前缀过滤
		if fi.IsDir() || !strings.HasPrefix(fi.Name(), sf.pfi.Name()) {
			continue
		}
		newInode, err := utils.GetIdentifyIDByPath(filepath.Join(dir, fi.Name()))
		if err != nil {
			log.Error(err)
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
	newInode, err := utils.GetIdentifyIDByPath(sf.path)
	if err != nil {
		return
	}
	oldInode, err := utils.GetIdentifyIDByFile(sf.f)
	if err != nil {
		return
	}

	if newInode == oldInode {
		return
	}
	sf.f.Close()
	detectStr := sf.detectMovedName(oldInode)
	if detectStr != "" {
		derr := sf.meta.AppendDoneFile(detectStr)
		if derr != nil {
			log.Errorf("AppendDoneFile %v error %v", detectStr, derr)
		}
	}
	log.Infof("rotate %s successfully , rotated file is <%v>", sf.path, detectStr)
	pfi, f, err := sf.openSingleFile(sf.path)
	if err != nil {
		return
	}
	sf.pfi = pfi
	sf.f = f
	sf.offset = 0
	return
}

func (sf *SingleFile) Read(p []byte) (n int, err error) {
	if atomic.LoadInt32(&sf.stopped) > 0 {
		return 0, errors.New("reader " + sf.Name() + " has been exited")
	}
	n, err = sf.f.Read(p)
	sf.offset += int64(n)
	if err == io.EOF {
		//读到了，如果n大于0，先把EOF抹去，返回
		if n > 0 {
			err = nil
			return
		}
		err = sf.Reopen()
		if err != nil {
			return
		}
		n, err = sf.f.Read(p)
		sf.offset += int64(n)
		return
	}
	return
}

func (sf *SingleFile) SyncMeta() error {
	if sf.lastSyncOffset == sf.offset && sf.lastSyncPath == sf.path {
		log.Debugf("%v was just syncd %v %v ignore it...", sf.Name(), sf.lastSyncPath, sf.lastSyncOffset)
		return nil
	}
	log.Debugf("%v Sync file success: %v", sf.Name(), sf.offset)
	sf.lastSyncOffset = sf.offset
	sf.lastSyncPath = sf.path
	return sf.meta.WriteOffset(sf.path, sf.offset)
}
