package reader

import (
	"errors"
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

	"github.com/json-iterator/go"
	"github.com/qiniu/log"
)

const (
	metaFileName      = "file.meta"
	doneFileName      = "file.done"
	deletedFileName   = "file.deleted"
	bufMetaFilePath   = "buf.meta"
	bufFilePath       = "buf.dat"
	lineCacheFilePath = "cache.dat"
	statisticFileName = "statistic.meta"
	doneFileRetention = "donefile_retention"
	ftSaveLogPath     = "ft_log" // ft log 在 meta 中的文件夹名字
)

const (
	defaultDirPerm      = 0755
	defaultFilePerm     = 0600
	defautFileRetention = 7
	metaFormat          = "%s\t%d\n"
	bufMetaFormat       = "read:%d\nwrite:%d\nbufsize:%d\n"
	defaultIOLimit      = 20 //默认读取速度为20MB/s
	ModeMetrics         = "metrics"
)

type Statistic struct {
	ReaderCnt int64               `json:"reader_count"` // 读取总条数
	ParserCnt [2]int64            `json:"parser_connt"` // [解析成功, 解析失败]
	SenderCnt map[string][2]int64 `json:"sender_count"` // [发送成功, 发送失败]
}

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
	dataSourceTag     string                 //记录文件路径的标签名称
	tagFile           string                 //记录tag文件路径的标签名称
	tags              map[string]interface{} //记录tag文件内容
	readlimit         int                    //读取磁盘限速单位 MB/s
	statisticPath     string                 // 记录 runner 计数信息
	ftSaveLogPath     string                 // 记录 ft_sender 日志信息
	RunnerName        string
}

func getValidDir(dir string) (realPath string, err error) {
	realPath, fi, err := utils.GetRealPath(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(realPath, defaultDirPerm); err != nil {
			//此处的error需要直接返回，后面会根据error类型是否为path error做判断
			log.Errorf("fail to newMeta cannot create %v, err:%v", realPath, err)
		}
		return
	}
	if err != nil {
		return
	}
	if !fi.Mode().IsDir() {
		err = ErrFileNotDir
	}
	return
}

func NewMeta(metadir, filedonedir, logpath, mode, tagfile string, donefileRetention int) (m *Meta, err error) {
	metadir, err = getValidDir(metadir)
	if err != nil {
		//此处的error需要直接返回，后面会根据error类型是否为path error做判断
		log.Errorf("check dir %v error %v", metadir, err)
		return
	}
	if filedonedir != metadir {
		filedonedir, err = getValidDir(filedonedir)
		if err != nil {
			//此处的error需要直接返回，后面会根据error类型是否为path error做判断
			log.Errorf("check dir %v error %v", filedonedir, err)
			return
		}
	}

	tags, err := getTags(tagfile)
	if err != nil {
		log.Errorf("failed to get tags from %v error %v", tagfile, err)
		return m, err
	}

	return &Meta{
		dir:               metadir,
		metaFilePath:      filepath.Join(metadir, metaFileName),
		doneFilePath:      filedonedir,
		bufFilePath:       filepath.Join(metadir, bufFilePath),
		bufMetaFilePath:   filepath.Join(metadir, bufMetaFilePath),
		lineCacheFile:     filepath.Join(metadir, lineCacheFilePath),
		statisticPath:     filepath.Join(metadir, statisticFileName),
		ftSaveLogPath:     filepath.Join(metadir, ftSaveLogPath),
		donefileretention: donefileRetention,
		logpath:           logpath,
		tagFile:           tagfile,
		mode:              mode,
		tags:              tags,
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

func getTagFileAbs(conf conf.MapConf) (tagfile string, err error) {
	tagfile, _ = conf.GetStringOr(KeyTagFile, "")
	if tagfile != "" {
		return filepath.Abs(tagfile)
	}
	return
}

func NewMetaWithConf(conf conf.MapConf) (meta *Meta, err error) {
	runnerName, _ := conf.GetStringOr(KeyRunnerName, "UndefinedRunnerName")
	mode, _ := conf.GetStringOr(KeyMode, ModeDir)
	logPath, err := getLogPathAbs(conf)
	if err != nil && (mode == ModeDir || mode == ModeFile) {
		return
	}
	err = nil
	tagFile, err := getTagFileAbs(conf)
	if err != nil {
		return
	}
	metapath, _ := conf.GetStringOr(KeyMetaPath, "")
	if metapath == "" {
		runnerName, _ := conf.GetString(utils.GlobalKeyName)
		base := filepath.Base(logPath)
		metapath = "meta/" + runnerName + "_" + hash(base)
		log.Debugf("Runner[%v] Using %s as default metaPath", runnerName, metapath)
	}
	datasourceTag, _ := conf.GetStringOr(KeyDataSourceTag, "")
	filedonepath, _ := conf.GetStringOr(KeyFileDone, metapath)
	donefileRetention, _ := conf.GetIntOr(doneFileRetention, defautFileRetention)
	readlimit, _ := conf.GetIntOr(KeyReadIOLimit, defaultIOLimit)
	meta, err = NewMeta(metapath, filedonepath, logPath, mode, tagFile, donefileRetention)
	if err != nil {
		log.Warnf("Runner[%v] %s - newMeta failed, err:%v", runnerName, metapath, err)
		return
	}
	meta.dataSourceTag = datasourceTag
	meta.readlimit = readlimit * 1024 * 1024 //readlimit*MB
	meta.RunnerName = runnerName
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
		log.Errorf("Runner[%v] remove %v err %v", m.RunnerName, m.dir, err)
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

// 	 读取当前读取的文件和offset
func (m *Meta) ReadOffset() (currFile string, offset int64, err error) {
	f, err := os.Open(m.MetaFile())
	if err != nil {
		return
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, metaFormat, &currFile, &offset)
	if err != nil {
		log.Debugf("meta file format err %v", err)
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

// StatisticFile 返回 Runner 统计信息的文件路径
func (m *Meta) StatisticFile() string {
	return m.statisticPath
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

// FtSaveLogPath 返回 ft_sender 日志信息记录文件夹路径
func (m *Meta) FtSaveLogPath() string {
	return m.ftSaveLogPath
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
			log.Debugf("Runner[%v] search file done skipped dir %v", m.RunnerName, f.Name())
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
	e = strings.ToUpper(e)
	if e != "UTF-8" {
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

func (m *Meta) IsFileMode() bool {
	return m.mode == ModeDir || m.mode == ModeFile || m.mode == ModeTailx
}

func (m *Meta) GetDataSourceTag() string {
	return m.dataSourceTag
}

func (m *Meta) GetTagFile() string {
	return m.tagFile
}

func (m *Meta) GetTags() map[string]interface{} {
	return m.tags
}

func (m *Meta) Reset() error {
	if m == nil {
		return errors.New("Reset error as meta is nil")
	}
	if err := os.RemoveAll(m.statisticPath); err != nil {
		return err
	}
	if err := os.RemoveAll(m.metaFilePath); err != nil {
		return err
	}
	if err := os.RemoveAll(m.doneFilePath); err != nil {
		return err
	}
	return nil
}

func (m *Meta) ReadStatistic() (stat Statistic, err error) {
	statData, err := ioutil.ReadFile(m.StatisticFile())
	if statData == nil || err != nil {
		return
	}
	err = jsoniter.Unmarshal(statData, &stat)
	return
}

func (m *Meta) WriteStatistic(stat *Statistic) error {
	statStr, err := jsoniter.Marshal(stat)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(m.StatisticFile(), statStr, defaultFilePerm)
}
