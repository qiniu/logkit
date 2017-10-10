package utils

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"encoding/json"

	"github.com/qiniu/log"
)

const (
	GlobalKeyName = "name"
)

type File struct {
	Info os.FileInfo
	Path string
}

// Int64Slice attaches the methods of Interface to []int64, sorting in decreasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] > p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p Int64Slice) Sort() { sort.Sort(p) }

// SortFilesByTime 按照文件更新的unixnano从大到小排，即最新的文件在前
func SortFilesByTime(files []os.FileInfo) (soredfiles []os.FileInfo) {
	filemap := make(map[int64]os.FileInfo)
	var times Int64Slice
	for index, f := range files {
		// 解决文件的创建时间相同的问题，加一个index
		nano := f.ModTime().UnixNano() + int64(index)
		times = append(times, nano)
		filemap[nano] = f
	}
	times.Sort()
	for _, t := range times {
		soredfiles = append(soredfiles, filemap[t])
	}
	return
}

// ReadDirByTime 读取文件目录后按时间排序，时间最新的文件在前
func ReadDirByTime(dir string) (files []os.FileInfo, err error) {
	files, err = ioutil.ReadDir(dir)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadDir(%s): %v, err:%v", dir, files, err)
		return
	}
	files = SortFilesByTime(files)
	return
}

func TrimeList(strs []string) (ret []string) {
	for _, s := range strs {
		s = strings.TrimSpace(s)
		if len(s) <= 0 {
			continue
		}
		ret = append(ret, s)
	}
	return
}

// GetRealPath 处理软链接等，找到文件真实路径
func GetRealPath(path string) (newPath string, fi os.FileInfo, err error) {
	newPath = path
	fi, err = os.Lstat(path)
	if err != nil {
		return
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		log.Infof("%s is symbol link", path)
		newPath, err = filepath.EvalSymlinks(path)
		if err != nil {
			return
		}
		log.Infof("%s is symbol link to %v", path, newPath)
		fi, err = os.Lstat(newPath)
	}
	newPath, err = filepath.Abs(newPath)
	if err != nil {
		return
	}
	return
}

func GetLogFiles(doneFilePath string) (files []File) {
	body, err := ioutil.ReadFile(doneFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Errorf("file %s not exisit", doneFilePath)
			return
		}
		log.Errorf("read file %s error %v", doneFilePath, err)
		return
	}
	readDoneFiles := TrimeList(strings.Split(string(body), "\n"))
	for i := len(readDoneFiles) - 1; i >= 0; i-- {
		df := readDoneFiles[i]
		dfi, err := os.Stat(df)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			log.Errorf("read file %v error %v", df, err)
			continue
		}
		files = append(files, File{
			Info: dfi,
			Path: df,
		})
	}
	return
}

type StatsError struct {
	StatsInfo
	ErrorDetail error `json:"error"`
	Ft          bool  `json:"-"`
	ErrorIndex  []int
}

type StatsInfo struct {
	Errors    int64   `json:"errors"`
	Success   int64   `json:"success"`
	Speed     float64 `json:"speed"`
	Trend     string  `json:"trend"`
	LastError string  `json:"last_error"`
	Ftlag     int64   `json:"-"`
}

func (se *StatsError) AddSuccess() {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Success, 1)
}

func (se *StatsError) AddErrors() {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Errors, 1)
}

func (se *StatsError) Error() string {
	if se == nil {
		return ""
	}
	return fmt.Sprintf("success %v errors %v errordetail %v", se.Success, se.Errors, se.ErrorDetail)
}

func (se *StatsError) ErrorIndexIn(idx int) bool {
	for _, v := range se.ErrorIndex {
		if v == idx {
			return true
		}
	}
	return false
}

// parse ${ENV} to ENV
// get ENV value from os
func GetEnv(env string) string {
	var envName string
	if strings.HasPrefix(env, "${") && strings.HasSuffix(env, "}") {
		envName = strings.Trim(strings.Trim(strings.Trim(env, "$"), "{"), "}")
	} else {
		log.Debug("cannot parse your ak sk as ${YOUR_ENV_NAME} format, use it as raw ak.sk instead")
		return ""
	}
	if osEnv := os.Getenv(envName); osEnv != "" {
		return osEnv
	}
	log.Warnf("cannot find %s in current system env", envName)
	return ""
}

//TuoEncode 把[]byte数组按照长度拼接到一起，每个sql.RawBytes之间间隔4个byte用于存储长度。
func TuoEncode(values []sql.RawBytes) (ret []byte) {
	ret = make([]byte, 0)
	for _, v := range values {
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, uint32(len(v)))
		ret = append(ret, tmp...)
		ret = append(ret, v...)
	}
	return
}

//TuoDecode 按照 TuoEncode的压缩算法解压，还原出[]byte数组。
func TuoDecode(value []byte) (values [][]byte, err error) {
	values = make([][]byte, 0)
	lens := len(value)
	idx := 0
	for idx < lens {
		if idx+2 >= lens {
			err = errors.New("TuoDecode failed as length of bytes should store in 2 bytes")
			return
		}
		tmp := value[idx : idx+4]
		idx += 4
		l := binary.LittleEndian.Uint32(tmp)
		if idx+int(l) > lens {
			err = fmt.Errorf("TuoDecode failed as length of bytes %v exceed total length %v", idx+int(l), lens)
			return
		}
		values = append(values, value[idx:idx+int(l)])
		idx += int(l)
	}
	return
}

//CreateDirIfNotExist 检查文件夹，不存在时创建
func CreateDirIfNotExist(dir string) (err error) {
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModeDir|os.ModePerm)
		if err != nil {
			return
		}
	}
	return
}

type ErrorResponse struct {
	Error error `json:"error"`
}

func NewErrorResponse(err error) *ErrorResponse {
	return &ErrorResponse{Error: err}
}

type OSInfo struct {
	Kernel   string
	Core     string
	Platform string
	OS       string
	Hostname string
}

func (oi *OSInfo) String() string {
	return fmt.Sprintf("(%s; %s; %s %s)", oi.OS, oi.Core, oi.Kernel, oi.Platform)
}

func IsJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}
