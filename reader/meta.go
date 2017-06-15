package reader

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"

	"github.com/qiniu/log"
)

const (
	metaFileName      = "file.meta"
	doneFileName      = "file.done"
	deletedFileName   = "file.deleted"
	bufMetaFilePath   = "buf.meta"
	bufFilePath       = "buf.dat"
	lineCacheFilePath = "cache.dat"
	doneFileRetention = "donefile_retention"
)

const (
	defaultDirPerm      = 0755
	defaultFilePerm     = 0600
	defautFileRetention = 7
	metaFormat          = "%s\t%d\n"
	bufMetaFormat       = "read:%d\nwrite:%d\nbufsize:%d\n"
	defaultIOLimit      = 20 //默认读取速度为20MB/s
)

type Meta struct {
	mode              string //reader mode
	dir               string // 记录文件处理进度的路径
	metaFilePath      string // 记录当前文件offset文件
	doneFilePath      string // 记录扫描过文件记录的文件
	bufMetaFilePath   string // 记录buf的offset数据
	bufFilePath       string // 记录buf数据
	lineCacheFile     string //记录多行的缓存line
	donefileretention int    // done.file保留时间，单位为天
	encodingWay       string //文件编码格式，默认为utf-8
	logpath           string
	dataSourceTag     string //记录文件路径的标签名称
	readlimit         int    //读取磁盘限速单位 MB/s
}

func getValidDir(dir string) (realPath string, err error) {
	realPath, fi, err := utils.GetRealPath(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(realPath, defaultDirPerm); err != nil {
			err = fmt.Errorf("fail to newMeta cannot create %v, err:%v", realPath, err)
		}
		return
	}
	if err != nil {
		return
	}
	if !fi.Mode().IsDir() {
		log.Errorf("%v is not directory", fi.Name())
		err = ErrFileNotDir
	}
	return
}

func NewMeta(metadir, filedonedir, logpath, mode string, donefileRetention int) (m *Meta, err error) {
	metadir, err = getValidDir(metadir)
	if err != nil {
		log.Error(err)
		return
	}
	if filedonedir != metadir {
		filedonedir, err = getValidDir(filedonedir)
		if err != nil {
			log.Error(err)
			return
		}
	}
	return &Meta{
		dir:               metadir,
		metaFilePath:      filepath.Join(metadir, metaFileName),
		doneFilePath:      filedonedir,
		bufFilePath:       filepath.Join(metadir, bufFilePath),
		bufMetaFilePath:   filepath.Join(metadir, bufMetaFilePath),
		lineCacheFile:     filepath.Join(metadir, lineCacheFilePath),
		donefileretention: donefileRetention,
		logpath:           logpath,
		mode:              mode,
		readlimit:         defaultIOLimit * 1024 * 1024,
	}, nil
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return strconv.Itoa(int(h.Sum32()))
}

func getLogPathAbs(conf conf.MapConf) (logpath string, err error) {
	logpath, err = conf.GetString(KeyLogPath)
	if err != nil {
		err = fmt.Errorf("get logpath in new meta error %v", err)
		return
	}
	return filepath.Abs(logpath)
}

func NewMetaWithConf(conf conf.MapConf) (meta *Meta, err error) {
	mode, _ := conf.GetStringOr(KeyMode, ModeDir)
	logPath, err := getLogPathAbs(conf)
	if err != nil && (mode == ModeDir || mode == ModeFile) {
		return
	}
	err = nil
	metapath, err := conf.GetString(KeyMetaPath)
	if err != nil {
		runnerName, _ := conf.GetString(utils.GlobalKeyName)
		base := filepath.Base(logPath)
		metapath = "meta/" + runnerName + "_" + hash(base)
		log.Debugf("Using %s as default metaPath", metapath)
	}
	datasourceTag, _ := conf.GetStringOr(KeyDataSourceTag, "")
	filedonepath, _ := conf.GetStringOr(KeyFileDone, metapath)
	donefileRetention, _ := conf.GetIntOr(doneFileRetention, defautFileRetention)
	readlimit, _ := conf.GetIntOr(KeyReadIOLimit, defaultIOLimit)
	meta, err = NewMeta(metapath, filedonepath, logPath, mode, donefileRetention)
	if err != nil {
		log.Warnf("%s - newMeta failed, err:%v", metapath, err)
		return
	}
	meta.dataSourceTag = datasourceTag
	meta.readlimit = readlimit * 1024 * 1024 //readlimit*MB
	return
}

func (m *Meta) IsExist() bool {
	return !m.IsNotExist()
}

func (m *Meta) IsValid() bool {
	return !m.IsNotValid()
}

// IsNotExist meta 不存在，用来判断是第一次创建
func (m *Meta) IsNotExist() bool {
	path := m.MetaFile()
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

// IsNotValid meta 数据已经过时，用来判断offset文件是否已经不存在，或者meta文件是否损坏
func (m *Meta) IsNotValid() bool {
	path, _, err := m.ReadOffset()
	if err != nil {
		return true
	}
	_, err = os.Stat(path)
	return os.IsNotExist(err)
}

// Clear 删除所有meta信息
func (m *Meta) Clear() error {
	err := os.RemoveAll(m.dir)
	if err != nil {
		log.Errorf("remove %v err %v", m.dir, err)
		return err
	}
	return os.MkdirAll(m.dir, defaultDirPerm)
}

func (m *Meta) CacheLineFile() string {
	return m.lineCacheFile
}

func (m *Meta) ReadCacheLine() ([]byte, error) {
	return ioutil.ReadFile(m.CacheLineFile())
}

func (m *Meta) WriteCacheLine(lines string) error {
	return ioutil.WriteFile(m.CacheLineFile(), []byte(lines), defaultFilePerm)
}

func (m *Meta) ReadBufMeta() (r, w, bufsize int, err error) {
	f, err := os.Open(m.BufMetaFile())
	if err != nil {
		return
	}
	defer f.Close()
	_, err = fmt.Fscanf(f, bufMetaFormat, &r, &w, &bufsize)
	return
}

func (m *Meta) ReadBuf(buf []byte) (n int, err error) {
	f, err := os.Open(m.BufFile())
	if err != nil {
		return
	}
	defer f.Close()
	return f.Read(buf)
}

func (m *Meta) WriteBuf(buf []byte, r, w, bufsize int) (err error) {
	var f *os.File
	bufMetaFileName := m.BufMetaFile()
	bufFileName := m.BufFile()

	tmpBufMetaFileName := fmt.Sprintf("%s.%d.tmp", bufMetaFileName, rand.Int())
	tmpBufFileName := fmt.Sprintf("%s.%d.tmp", bufFileName, rand.Int())

	defer func() {
		os.RemoveAll(tmpBufMetaFileName)
		os.RemoveAll(tmpBufFileName)
	}()

	// write to tmp file
	f, err = os.OpenFile(tmpBufMetaFileName, os.O_RDWR|os.O_CREATE, defaultFilePerm)
	if err != nil {
		return
	}
	_, err = fmt.Fprintf(f, bufMetaFormat, r, w, bufsize)
	if err != nil {
		f.Close()
		return
	}
	f.Sync()
	f.Close()

	// write to tmp file
	f, err = os.OpenFile(tmpBufFileName, os.O_RDWR|os.O_CREATE, defaultFilePerm)
	if err != nil {
		return
	}

	_, err = f.Write(buf)
	if err != nil {
		f.Close()
		return
	}
	f.Sync()
	f.Close()

	err = os.Rename(tmpBufMetaFileName, bufMetaFileName)
	if err != nil {
		return
	}
	return os.Rename(tmpBufFileName, bufFileName)
}

// ReadOffset 读取当前读取的文件和offset
func (m *Meta) ReadOffset() (currFile string, offset int64, err error) {
	f, err := os.Open(m.MetaFile())
	if err != nil {
		return
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, metaFormat, &currFile, &offset)
	if err != nil {
		log.Errorf("meta file format err %v", err)
		return
	}
	if m.mode == ModeDir || m.mode == ModeFile {
		_, err = os.Stat(currFile)
		if err != nil {
			if os.IsNotExist(err) {
				log.Errorf("meta content outdated, the file %v has been deleted", currFile)
			}
			return
		}
	}
	return
}

// WriteOffset 将当前文件和offset写入meta中
func (m *Meta) WriteOffset(currFile string, offset int64) (err error) {
	var f *os.File
	fileName := m.MetaFile()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, defaultFilePerm)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, metaFormat, currFile, offset)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	return os.Rename(tmpFileName, fileName)
}

// AppendDoneFile 将处理完的文件写入doneFile中
func (m *Meta) AppendDoneFile(path string) (err error) {
	f, err := os.OpenFile(m.DoneFile(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFilePerm)
	if err != nil {
		return
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%s\n", path)
	return
}

// DoneFile 处理完成文件地址，按日进行rotate
func (m *Meta) DoneFile() string {
	now := time.Now()
	return fmt.Sprintf("%v.%d-%d-%d", filepath.Join(m.doneFilePath, doneFileName), now.Year(), now.Month(), now.Day())
}

// DeleteFile 处理完成文件地址，按日进行rotate
func (m *Meta) DeleteFile() string {
	now := time.Now()
	return fmt.Sprintf("%v.%d-%d-%d", filepath.Join(m.doneFilePath, deletedFileName), now.Year(), now.Month(), now.Day())
}

func (m *Meta) AppendDeleteFile(path string) (err error) {
	f, err := os.OpenFile(m.DeleteFile(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, defaultFilePerm)
	if err != nil {
		return
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%s\n", path)
	return
}

// IsDoneFile 返回是否是Donefile格式的文件
func (m *Meta) IsDoneFile(file string) bool {
	file = filepath.Base(file)
	return strings.HasPrefix(file, doneFileName)
}

// MetaFile 返回metaFileoffset 的meta文件地址
func (m *Meta) MetaFile() string {
	return m.metaFilePath
}

// BufFile 返回buf的文件路径
func (m *Meta) BufFile() string {
	return m.bufFilePath
}

// BufMetaFile 返回buf的meta文件路径
func (m *Meta) BufMetaFile() string {
	return m.bufMetaFilePath
}

//DoneFilePath 返回meta的filedone文件的存放目录
func (m *Meta) DoneFilePath() string {
	return m.doneFilePath
}

func (m *Meta) LogPath() string {
	return m.logpath
}

func (m *Meta) DeleteDoneFile(path string) error {
	path = filepath.Base(path)
	if !strings.HasPrefix(path, doneFileName) {
		return fmt.Errorf("%v file was not valid done file format", path)
	}
	dates := strings.Split(path[len(doneFileName)+1:], "-")
	if len(dates) < 3 {
		return fmt.Errorf("%v file was not valid done file format", path)
	}
	dy, _ := strconv.ParseInt(dates[0], 10, 64)
	dm, _ := strconv.ParseInt(dates[1], 10, 64)
	dd, _ := strconv.ParseInt(dates[2], 10, 64)
	dur := time.Now().Sub(time.Date(int(dy), time.Month(dm), int(dd), 0, 0, 0, 0, time.Local))
	if float64(m.donefileretention*24) < dur.Hours() {
		return os.Remove(filepath.Join(m.doneFilePath, path))
	}
	return nil
}

func (m *Meta) GetDoneFiles() (doneFiles []utils.File, err error) {
	dir := m.doneFilePath
	// 按文件时间从新到旧排列
	files, err := utils.ReadDirByTime(dir)
	if err != nil {
		log.Error(files, err)
		return
	}
	for _, f := range files {
		if f.IsDir() {
			log.Warnf("search file done skipped dir %v", f.Name())
			continue
		}
		fname := f.Name()
		if m.IsDoneFile(fname) {
			doneFiles = append(doneFiles, utils.File{
				Info: f,
				Path: filepath.Join(dir, fname),
			})
		}
	}
	return
}

//SetEncodingWay 设置文件编码方式，默认为 utf-8
func (m *Meta) SetEncodingWay(e string) {
	if e != "utf-8" {
		m.encodingWay = e
	}
}

//GetEncodingWay 获取文件编码方式
func (m *Meta) GetEncodingWay() (e string) {
	return m.encodingWay
}

func (m *Meta) GetMode() string {
	return m.mode
}

func (m *Meta) GetDataSourceTag() string {
	return m.dataSourceTag
}
