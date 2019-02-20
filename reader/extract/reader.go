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

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/reader"
)

// FileReader reader 接口方法
type Reader struct {
	underlying SourceReader
	path       string
	tp         string
	m          *reader.Meta
}

//NewReader 实现对压缩包的读取
// 还可以实现更多功能：
// 1. ignoreHidden 忽略压缩包中的隐藏文件
// 2. suffixes []string 只读某些后缀
// 3. validFileRegex  只读匹配的文件
func NewReader(meta *reader.Meta, path string) (*Reader, error) {
	var rd SourceReader
	var err error
	var tp string
	if strings.HasSuffix(path, ".tar.gz") {
		rd, err = NewTarGz(path)
		if err != nil {
			log.Errorf("New .tar.gz err %v", err)
			return nil, err
		}
		tp = "targz"
	} else if strings.HasSuffix(path, ".tar") {
		rd, err = NewTar(path)
		if err != nil {
			log.Errorf("New .tar err %v", err)
			return nil, err
		}
		tp = "tar"
	} else if strings.HasSuffix(path, ".gz") {
		rd, err = NewGZ(path)
		if err != nil {
			log.Errorf("New .gz err %v", err)
			return nil, err
		}
		tp = "gz"
	} else if strings.HasSuffix(path, ".zip") {
		rd, err = NewZIP(path)
		if err != nil {
			log.Errorf("New .zip err %v", err)
			return nil, err
		}
		tp = "zip"
	}
	return &Reader{
		underlying: rd,
		path:       path,
		tp:         tp,
		m:          meta,
	}, nil

}

func (r *Reader) Name() string {
	return r.tp + ":" + r.path
}

func (r *Reader) Source() string {
	return r.underlying.Source()
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.underlying.Read(p)
}

func (r *Reader) Close() error {
	return r.underlying.Close()
}

func (r *Reader) SyncMeta() error {
	return nil
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
	Source() string
}

type Tar struct {
	rd     *tar.Reader
	f      *os.File
	header *tar.Header
	path   string

	source string
	sLock  *sync.RWMutex
}

func NewTarGz(path string) (*Tar, error) {
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
		rd:    tar.NewReader(gzf),
		f:     f,
		path:  path,
		sLock: new(sync.RWMutex),
	}, nil

}

func NewTar(path string) (*Tar, error) {
	if !strings.HasSuffix(path, ".tar") {
		return nil, fmt.Errorf("%s is not .tar format", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Tar{
		rd:    tar.NewReader(f),
		f:     f,
		path:  path,
		sLock: new(sync.RWMutex),
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
	return nil
}

func (t *Tar) Read(p []byte) (n int, err error) {
	if t.header == nil {
		err = t.next()
		if err != nil {
			return 0, err
		}
	}
	for {
		n, err = t.rd.Read(p)
		if err == io.EOF {
			err = t.next()
			if err != nil {
				return n, err
			}
			if n > 0 {
				return n, nil
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

type GZ struct {
	rd   *gzip.Reader
	f    *os.File
	path string

	source string
}

func NewGZ(path string) (*GZ, error) {
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

type ZIP struct {
	rd   *zip.ReadCloser
	f    io.ReadCloser
	path string
	idx  int

	source string
	sLock  *sync.RWMutex
}

func NewZIP(path string) (*ZIP, error) {
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
	}, nil
}

func (t *ZIP) next() (err error) {
	if t.f != nil {
		t.f.Close()
	}
	if t.idx >= 0 && t.idx < len(t.rd.File) {
		zipf := t.rd.File[t.idx]
		t.f, err = zipf.Open()
		if err != nil {
			return err
		}
		t.sLock.Lock()

		t.source = zipf.Name
		defer t.sLock.Unlock()
	} else {
		return io.EOF
	}
	t.idx++

	return nil
}

func (t *ZIP) Read(p []byte) (n int, err error) {
	if t.f == nil {
		err = t.next()
		if err != nil {
			return 0, err
		}
	}
	for {
		n, err = t.f.Read(p)
		if err == io.EOF {
			err = t.next()
			if err != nil {
				return 0, err
			}
			if n > 0 {
				return n, nil
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
