package extract

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

// FileReader reader 接口方法
type Reader struct {
	underlying SourceReader
	path       string
	tp         string
	m          *reader.Meta
	done       int32
}

type Opts struct {
	IgnoreHidden       bool
	NewFileNewLine     bool
	IgnoreFileSuffixes []string
	ValidFilesRegex    string
}

//NewReader 实现对压缩包的读取
// 还可以实现更多功能：
// 1. ignoreHidden 忽略压缩包中的隐藏文件
// 2. suffixes []string 只读某些后缀
// 3. validFileRegex  只读匹配的文件
func NewReader(meta *reader.Meta, path string, opt Opts) (*Reader, error) {
	var rd SourceReader
	var err error
	var tp, curFile string
	var offset int64
	if strings.HasSuffix(path, ".tar.gz") {
		rd, err = NewTarGz(path, opt)
		if err != nil {
			log.Errorf("New .tar.gz err %v", err)
			return nil, err
		}
		tp = "targz"
	} else if strings.HasSuffix(path, ".tar") {
		rd, err = NewTar(path, opt)
		if err != nil {
			log.Errorf("New .tar err %v", err)
			return nil, err
		}
		tp = "tar"
	} else if strings.HasSuffix(path, ".gz") {
		rd, err = NewGZ(path, opt)
		if err != nil {
			log.Errorf("New .gz err %v", err)
			return nil, err
		}
		tp = "gz"
	} else if strings.HasSuffix(path, ".zip") {
		rd, err = NewZIP(path, opt)
		if err != nil {
			log.Errorf("New .zip err %v", err)
			return nil, err
		}
		tp = "zip"
	}
	var done int32 = 0
	curFile, offset, err = meta.ReadOffset()
	if err == nil && curFile == path && offset > 0 {
		log.Infof("log(%s) has been already read done before", path)
		done = 1
	}
	return &Reader{
		underlying: rd,
		path:       path,
		tp:         tp,
		m:          meta,
		done:       done,
	}, nil

}

func (r *Reader) Name() string {
	return r.tp + ":" + r.path
}

func (r *Reader) Source() string {
	return r.underlying.Source()
}

func (r *Reader) NewSourceIndex() []reader.SourceIndex {
	return r.underlying.NewSourceIndex()
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if atomic.LoadInt32(&r.done) > 0 {
		return 0, io.EOF
	}
	n, err = r.underlying.Read(p)
	if err == io.EOF && n == 0 {
		atomic.StoreInt32(&r.done, 1)
	}
	return n, err
}

func (r *Reader) Close() error {
	return r.underlying.Close()
}

func (r *Reader) SyncMeta() error {
	if atomic.LoadInt32(&r.done) > 0 {
		r.Close()
		return r.m.WriteOffset(r.path, 1)
	}
	return nil
}

func (r *Reader) Lag() (rl *LagInfo, err error) {
	return r.underlying.Lag()
}

func (r *Reader) ReadDone() bool {
	return atomic.LoadInt32(&r.done) > 0
}

// Compile-time checks to ensure type implements desired interfaces.
var (
	_ = reader.FileReader(new(Reader))
	_ = SourceReader(new(Tar))
	_ = SourceReader(new(ZIP))
	_ = SourceReader(new(GZ))
)

type SourceReader interface {
	io.ReadCloser
	reader.NewSourceRecorder
	Source() string
	Lag() (rl *LagInfo, err error)
}

type Tar struct {
	rd     *tar.Reader
	f      *os.File
	header *tar.Header
	path   string
	opt    Opts

	sourceIndexes []reader.SourceIndex //新文件被读取时的bytes位置
	source        string
	sLock         *sync.RWMutex
	totalSize     int64
	curSize       int64
}

func NewTarGz(path string, opt Opts) (*Tar, error) {
	if !strings.HasSuffix(path, ".tar.gz") {
		return nil, fmt.Errorf("%s is not .tar.gz format", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	gzf, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Tar{
		rd:        tar.NewReader(gzf),
		f:         f,
		path:      path,
		sLock:     new(sync.RWMutex),
		totalSize: 0,
		curSize:   0,
		opt:       opt,
	}, nil
}

func NewTar(path string, opt Opts) (*Tar, error) {
	if !strings.HasSuffix(path, ".tar") {
		return nil, fmt.Errorf("%s is not .tar format", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Tar{
		rd:        tar.NewReader(f),
		f:         f,
		path:      path,
		sLock:     new(sync.RWMutex),
		totalSize: 0,
		curSize:   0,
		opt:       opt,
	}, nil
}

func (t *Tar) next() (err error) {
	t.header, err = t.rd.Next()
	if err != nil {
		return
	}
	t.sLock.Lock()
	defer t.sLock.Unlock()
	t.source = t.header.Name
	t.totalSize += t.header.Size
	return nil
}

func (t *Tar) Read(p []byte) (n int, err error) {
	defer func() {
		t.sLock.Lock()
		defer t.sLock.Unlock()
		t.curSize += int64(n)
	}()
	t.sourceIndexes = []reader.SourceIndex{}
	for {
		if t.header == nil ||
			reader.IgnoreHidden(t.header.Name, t.opt.IgnoreHidden) ||
			reader.IgnoreFileSuffixes(t.header.Name, t.opt.IgnoreFileSuffixes) ||
			!reader.ValidFileRegex(filepath.Base(t.header.Name), t.opt.ValidFilesRegex) {
			if t.header != nil {
				log.Infof("ignore  %s in path %s", t.header.Name, t.path)
			}
			err = t.next()
			if err != nil {
				return 0, err
			}
			continue
		}
		log.Infof("start to read %s in path %s", t.header.Name, t.path)
		break
	}
	t.sourceIndexes = []reader.SourceIndex{{Index: 0, Source: t.header.Name}}
	for {
		n, err = t.rd.Read(p)
		if n > 0 {
			t.sourceIndexes[0].Index = n
			t.sourceIndexes[0].Source = t.header.Name
		}
		if err == io.EOF {
			//如果已经EOF，但是上次还读到了，先返回上次的结果
			if n > 0 {
				return n, nil
			}
			err = t.next()
			if err != nil {
				return n, err
			}
			continue
		}
		if err != nil {
			return n, err
		}
		break
	}
	return n, nil
}

func (t *Tar) Close() error {
	return t.f.Close()
}

func (t *Tar) Source() string {
	t.sLock.RLock()
	defer t.sLock.RUnlock()
	return t.source
}

func (t *Tar) NewSourceIndex() []reader.SourceIndex {
	return t.sourceIndexes
}

func (t *Tar) Lag() (rl *LagInfo, err error) {
	t.sLock.Lock()
	defer t.sLock.Unlock()
	lag := t.totalSize - t.curSize
	if lag < 0 {
		lag = 0
	}
	return &LagInfo{
		Size:     lag,
		SizeUnit: "byte",
	}, nil
}

type GZ struct {
	rd   *gzip.Reader
	f    *os.File
	path string

	source string
	opt    Opts
}

func NewGZ(path string, opt Opts) (*GZ, error) {
	if !strings.HasSuffix(path, ".gz") {
		return nil, fmt.Errorf("%s is not .gz format", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gzipData, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	if gzipData.Name == "" {
		gzipData.Name = strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".gz"), ".tar.gz")
	}
	source := filepath.Join(filepath.Dir(path), gzipData.Name)
	return &GZ{
		rd:     gzipData,
		f:      f,
		path:   path,
		source: source,
		opt:    opt,
	}, nil
}

func (t *GZ) Read(p []byte) (n int, err error) {
	return t.rd.Read(p)
}

func (t *GZ) Close() error {
	err := t.f.Close()
	if err != nil {
		return err
	}
	return t.rd.Close()
}

func (t *GZ) Source() string {
	return t.source
}

func (t *GZ) Lag() (rl *LagInfo, err error) {
	return &LagInfo{SizeUnit: "byte"}, nil
}

func (t *GZ) NewSourceIndex() []reader.SourceIndex {
	return nil
}

type ZIP struct {
	rd   *zip.ReadCloser
	f    io.ReadCloser
	zipf *zip.File
	path string
	idx  int
	opt  Opts

	source        string
	sourceIndexes []reader.SourceIndex
	sLock         *sync.RWMutex
}

func NewZIP(path string, opt Opts) (*ZIP, error) {
	if !strings.HasSuffix(path, ".zip") {
		return nil, fmt.Errorf("%s is not .zip format", path)
	}
	rd, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	return &ZIP{
		rd:    rd,
		path:  path,
		sLock: new(sync.RWMutex),
		idx:   0,
		opt:   opt,
	}, nil
}

func (t *ZIP) next() (err error) {
	if t.f != nil {
		t.f.Close()
	}
	for ; t.idx >= 0 && t.idx < len(t.rd.File); t.idx++ {
		t.zipf = t.rd.File[t.idx]
		if t.zipf.FileInfo().IsDir() {
			continue
		}
		t.f, err = t.zipf.Open()
		if err != nil {
			return err
		}
		t.sLock.Lock()
		t.source = t.zipf.Name
		t.sLock.Unlock()
		break
	}
	if t.idx >= len(t.rd.File) || t.idx < 0 {
		return io.EOF
	}
	t.idx++

	return nil
}

func (t *ZIP) Read(p []byte) (n int, err error) {
	t.sourceIndexes = []reader.SourceIndex{}
	for {
		if t.f == nil ||
			reader.IgnoreHidden(t.zipf.Name, t.opt.IgnoreHidden) ||
			reader.IgnoreFileSuffixes(t.zipf.Name, t.opt.IgnoreFileSuffixes) ||
			!reader.ValidFileRegex(filepath.Base(t.zipf.Name), t.opt.ValidFilesRegex) {
			if t.f != nil {
				log.Infof("ignore  %s in path %s", t.zipf.Name, t.path)
			}
			err = t.next()
			if err != nil {
				return 0, err
			}
			continue
		}
		log.Infof("start to read %s in path %s", t.zipf.Name, t.path)
		break
	}
	t.sourceIndexes = []reader.SourceIndex{{Index: 0, Source: t.zipf.Name}}
	for {
		n, err = t.f.Read(p)
		if n > 0 {
			t.sourceIndexes[0].Index += n
			t.sourceIndexes[0].Source = t.zipf.Name
		}
		if err == io.EOF {
			//如果已经EOF，但是上次还读到了，先返回上次的结果
			if n > 0 {
				return n, nil
			}
			err = t.next()
			if err != nil {
				return 0, err
			}
			continue
		}
		if err != nil {
			return n, err
		}
		break
	}
	return n, nil
}

func (t *ZIP) Close() error {
	if t.f != nil {
		t.f.Close()
	}
	return t.rd.Close()
}

func (t *ZIP) Source() string {
	t.sLock.RLock()
	defer t.sLock.RUnlock()
	return t.source
}

func (t *ZIP) NewSourceIndex() []reader.SourceIndex {
	return t.sourceIndexes
}

func (t *ZIP) Lag() (rl *LagInfo, err error) {
	return &LagInfo{SizeUnit: "byte"}, nil
}
