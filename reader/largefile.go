package reader

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/utils"
)

type LFReader struct {
	dir     string   // 文件目录
	f       *os.File // 当前处理文件
	offset  int64    // 当前处理文件offset
	fname   string   // 文件名
	stopped *int32   // 停止标志位
	mode    string   // 处理文件夹模式还是文件模式
	inode   uint64   // 当前文件inode
}

func (l *LFReader) Offset() int64 {
	return l.offset
}

func (l *LFReader) FilePath() string {
	return filepath.Join(l.dir, l.fname)
}

func (l *LFReader) Name() string {
	return "LFReader:" + l.dir
}

func (l *LFReader) Close() (err error) {
	return l.f.Close()
}

func (l *LFReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		n1, err := l.f.Read(p[n:])
		l.offset += int64(n1)
		n += n1
		if err != nil {
			if err != io.EOF {
				return n, err
			}
			fi, err1 := l.nextFile()
			if os.IsNotExist(err1) {
				// dir removed or file rotated
				log.Debugf("%s - nextFile: %v", l.dir, err1)
				time.Sleep(WaitNoSuchFile)
				continue
			}
			if err1 != nil {
				return n, err1
			}
			if fi != nil {
				log.Infof("%s - nextFile: %s", l.dir, fi.Name())
				err2 := l.open(fi)
				if err2 != nil {
					return n, err2
				}
			} else {
				time.Sleep(time.Millisecond * 500)
				continue
			}
		}
	}
	return
}

func (l *LFReader) nextFile() (fi os.FileInfo, err error) {
	switch l.mode {
	case "dir":
		fi, err = getLatestFile(l.dir)
	case "file":
		fi, err = os.Stat(filepath.Join(l.dir, l.fname))
	default:
		err = fmt.Errorf("invalid mode %s", l.mode)
	}
	if err != nil {
		return nil, err
	}
	if l.isNewFile(fi) {
		return fi, nil
	}
	return nil, nil
}

func (l *LFReader) isNewFile(newFileInfo os.FileInfo) bool {
	if newFileInfo == nil {
		return false
	}
	newInode := getInode(newFileInfo)
	newName := newFileInfo.Name()
	newFsize := newFileInfo.Size()
	if newInode != 0 && l.inode != 0 && newInode == l.inode {
		return false
	}
	if newInode != l.inode {
		log.Debugf("%s - newInode: %d, l.inode: %d", l.dir, newInode, l.inode)
		return true
	}
	if newFsize < l.offset {
		log.Debugf("%s - newFsize: %d, l.offset: %d", l.dir, newFsize, l.offset)
		return true
	}
	if newName != l.fname {
		log.Debugf("%s - newName: %d, l.fname: %d", l.dir, newName, l.fname)
		return true
	}
	return false
}

func (l *LFReader) open(fi os.FileInfo) (err error) {
	if fi == nil {
		return
	}
	err = l.f.Close()
	if err != nil && err != syscall.EINVAL {
		log.Warnf("%s - %s f.Close: %v", l.dir, l.fname)
		return
	}

	l.fname = fi.Name()
	for {
		f, err := os.Open(filepath.Join(l.dir, l.fname))
		if os.IsNotExist(err) {
			log.Debugf("os.Open %s: %v", l.fname, err)
			time.Sleep(WaitNoSuchFile)
			continue
		}
		if err != nil {
			log.Warnf("os.Open %s: %v", l.fname, err)
			return err
		}
		l.f = f
		l.offset = 0
		l.inode = getInode(fi)
		log.Infof("%s - start tail new file: %s", l.dir, l.fname)
		break
	}
	return
}

func NewLFReader(path string, mode string, stopped *int32) (lfr *LFReader, err error) {
	var fname, dir string
	var f *os.File
	for {
		if stopped != nil {
			if atomic.LoadInt32(stopped) > 0 {
				return nil, ErrStopped
			}
		}
		var pfi os.FileInfo
		path, pfi, err = utils.GetRealPath(path)
		if err != nil || pfi == nil {
			log.Warnf("%s - utils.GetRealPath failed, err:%v", path, err)
			time.Sleep(time.Minute)
			continue
		}
		switch mode {
		case "dir":
			if !pfi.IsDir() {
				return nil, fmt.Errorf("%s is not a dir", path)
			}
			dir = path
			fi, err := getLatestFile(dir)
			if err != nil {
				log.Warnf("%s getLatestFile failed, err: %v", dir, err)
				time.Sleep(time.Minute)
				continue
			}
			if fi == nil {
				log.Debugf("%s getLatestFile empty dir", dir)
				time.Sleep(WaitNoSuchFile)
				continue
			}
			fname = fi.Name()
		case "file":
			if !pfi.Mode().IsRegular() {
				return nil, fmt.Errorf("%s is not a regular file", path)
			}
			dir = filepath.Dir(path)
			fname = filepath.Base(path)
		default:
			return nil, fmt.Errorf("invalid mode %s", mode)
		}
		fullpath := filepath.Join(dir, fname)
		f, err = os.Open(fullpath)
		if err == nil {
			break
		}
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	offset, err := f.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	inode := getInode(fi)

	log.Infof("%s - NewLFReader with: %s, inode: %d, offset: %d", dir, fname, inode, offset)
	lfr = &LFReader{
		dir:     dir,
		fname:   fname,
		offset:  offset,
		f:       f,
		stopped: stopped,
		mode:    mode,
		inode:   inode,
	}
	return
}
