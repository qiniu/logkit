package models

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/qiniu/log"

	"github.com/json-iterator/go"
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

type SchemaErr struct {
	Number int64
	Last   time.Time
}

func (s *SchemaErr) Output(count int64, err error) {
	s.Number += count
	if time.Now().Sub(s.Last) > 3*time.Second {
		log.Errorf("%v parse line errors occured, same as %v", s.Number, err)
		s.Number = 0
		s.Last = time.Now()
	}
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

type ErrorResponse struct {
	Error error `json:"error"`
}

func NewErrorResponse(err error) *ErrorResponse {
	return &ErrorResponse{Error: err}
}

func IsJsonString(s string) bool {
	var x interface{}
	if err := jsoniter.Unmarshal([]byte(s), &x); err != nil {
		return false
	}
	switch x.(type) {
	case []interface{}, map[string]interface{}:
		return true
	default:
		return false
	}
}

func ExtractField(slice []string) ([]string, error) {
	var err error
	switch len(slice) {
	case 1:
		return slice, err
	case 2:
		rgexpr := "^%\\{\\[\\S+\\]}$" // --->  %{[type]}
		r, _ := regexp.Compile(rgexpr)
		slice[0] = strings.TrimSpace(slice[0])
		bol := r.MatchString(slice[0])
		if bol {
			rs := []rune(slice[0])
			slice[0] = string(rs[3 : len(rs)-2])
			return slice, err
		}
	default:
	}
	err = fmt.Errorf("parameters error,  you can write two parameters like: %{[type]}, default or only one: default")
	return nil, err
}

func AddHttpProtocal(url string) string {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return "http://" + url
	}
	return url
}

func RemoveHttpProtocal(url string) (hostport, schema string) {
	chttps := "https://"
	chttp := "http://"
	if strings.HasPrefix(url, chttp) {
		return strings.TrimPrefix(url, chttp), chttp
	}
	if strings.HasPrefix(url, chttps) {
		return strings.TrimPrefix(url, chttps), chttps
	}
	return url, chttp
}

type HashSet struct {
	data map[interface{}]bool
	mu   *sync.RWMutex
}

func NewHashSet() *HashSet {
	return &HashSet{
		data: make(map[interface{}]bool),
		mu:   new(sync.RWMutex),
	}
}

func (s *HashSet) Add(ele interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[ele] = true
}

func (s *HashSet) AddStringArray(ele []string) {
	for _, e := range ele {
		s.Add(e)
	}
}

func (s *HashSet) Remove(ele interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, ele)
}

func (s *HashSet) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[interface{}]bool)
}

func (s *HashSet) IsIn(ele interface{}) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[ele]
}

func (s *HashSet) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Len() == 0 {
		return true
	}
	return false
}

func (s *HashSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *HashSet) Elements() []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	element := make([]interface{}, 0)
	for key, _ := range s.data {
		element = append(element, key)
	}
	return element
}

// 创建目录，并返回日志模式
func LogDirAndPattern(logpath string) (dir, pattern string, err error) {
	dir, err = filepath.Abs(filepath.Dir(logpath))
	if err != nil {
		if !os.IsNotExist(err) {
			err = fmt.Errorf("get logkit log dir error %v", err)
			return
		}
	}
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, DefaultDirPerm); err != nil {
			err = fmt.Errorf("create logkit log dir error %v", err)
			return
		}
	}
	pattern = filepath.Base(logpath)
	return
}

func DecompressZip(packFilePath, dstDir string) (packDir string, err error) {
	r, err := zip.OpenReader(packFilePath) //读取zip文件
	if err != nil {
		return "", err
	}
	defer r.Close()
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return "", err
		}
		defer rc.Close()

		fpath := filepath.Join(dstDir, f.Name)
		if f.FileInfo().IsDir() {
			if packDir == "" {
				packDir = fpath
			}
			os.MkdirAll(fpath, f.Mode())
		} else {
			var fdir string
			if lastIndex := strings.LastIndex(fpath, string(os.PathSeparator)); lastIndex > -1 {
				fdir = fpath[:lastIndex]
			}
			err = os.MkdirAll(fdir, f.Mode())
			if err != nil {
				fmt.Println(err)
				return "", err
			}
			f, err := os.OpenFile(
				fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return "", err
			}
			defer f.Close()

			_, err = io.Copy(f, rc)
			if err != nil {
				return "", err
			}
		}
	}
	return
}

func DecompressGzip(packPath, dstDir string) (packDir string, err error) {
	srcFile, err := os.Open(packPath)
	if err != nil {
		return "", err
	}
	defer srcFile.Close()
	gr, err := gzip.NewReader(srcFile)
	if err != nil {
		return "", err
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		path := filepath.Join(dstDir, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, info.Mode()); err != nil {
				return "", err
			}
			if packDir == "" {
				packDir = path
			}
			continue
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return "", err
		}
		defer file.Close()
		_, err = io.Copy(file, tr)
		if err != nil {
			return "", err
		}
	}
	return
}

//通过层级key设置value值.
//如果key不存在,将会自动创建.
//当coercive为true时,会强制将非map[string]interface{}类型替换为map[string]interface{}类型,有可能导致数据丢失
func SetMapValue(m map[string]interface{}, val interface{}, coercive bool, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	curr := m
	for _, k := range keys[0 : len(keys)-1] {
		if _, ok := curr[k]; !ok {
			n := make(map[string]interface{})
			curr[k] = n
			curr = n
			continue
		}
		if _, ok := curr[k].(map[string]interface{}); !ok {
			if coercive {
				n := make(map[string]interface{})
				curr[k] = n
			} else {
				err := fmt.Errorf("SetMapValue failed, %v is not the type of map[string]interface{}", curr[k])
				return err
			}
		}
		curr = curr[k].(map[string]interface{})
	}
	curr[keys[len(keys)-1]] = val
	return nil
}

//通过层级key删除key-val,并返回被删除的val,是否删除成功
//如果key不存在,则返回 nil,false
func DeleteMapValue(m map[string]interface{}, keys ...string) (interface{}, bool) {
	var val interface{}
	val = m
	for i, k := range keys {
		if _, ok := val.(map[string]interface{}); ok {
			if _, ok := val.(map[string]interface{})[k]; ok {
				if i == len(keys)-1 {
					delVal := val.(map[string]interface{})[k]
					delete(val.(map[string]interface{}), keys[len(keys)-1])
					return delVal, true
				}
				val = val.(map[string]interface{})[k]
			} else {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return nil, false
}

//根据key字符串,拆分出层级keys数据
func GetKeys(keyStr string) []string {
	keys := strings.FieldsFunc(keyStr, isSeparator)
	return keys
}

func isSeparator(separator rune) bool {
	return separator == '.' || unicode.IsSpace(separator)
}

//通过层级key获取value.
//所有层级的map必须为 map[string]interface{} 类型.
//keys为空切片,返回原m
func GetMapValue(m map[string]interface{}, keys ...string) (interface{}, error) {
	var err error
	var val interface{}
	val = m
	for i, k := range keys {
		//判断val是否为map[string]interface{}类型
		if _, ok := val.(map[string]interface{}); ok {
			//判断val(k)是否存在
			if _, ok := val.(map[string]interface{})[k]; ok {
				val = val.(map[string]interface{})[k]
			} else {
				keys = keys[0 : i+1]
				err = fmt.Errorf("GetMapValue failed, keys %v are non-existent", keys)
				return nil, err
			}
		} else {
			err = fmt.Errorf("GetMapValue failed, %v is not the type of map[string]interface{}", val)
			return nil, err
		}
	}
	return val, err
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

func CheckFileMode(path string, fileMode os.FileMode) error {
	perm := fileMode.Perm()

	// 73: 000 001 001 001
	checkPerm := perm & os.FileMode(73)
	if uint32(checkPerm) != uint32(73) {
		changePerm := perm | os.FileMode(73)
		err := os.Chmod(path, changePerm)
		if err != nil {
			err = fmt.Errorf("change mode for %v error %v", path, err)
			return err
		}
	}
	return nil
}

func Hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return strconv.Itoa(int(h.Sum32()))
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

func DecodeString(target string) (result string, err error) {
	if target != "" {
		bytes, err := base64.URLEncoding.DecodeString(target)
		if err != nil {
			err = fmt.Errorf("base64 decode %v error: %v", target, err)
			return "", err
		}
		result, err = url.PathUnescape(string(bytes))
		if err != nil {
			err = fmt.Errorf("path unescape decode %v error: %v", target, err)
			return "", err
		}
	}
	return
}

func EncodeString(target string) (result string) {
	if target != "" {
		result = url.PathEscape(target)
		result = base64.URLEncoding.EncodeToString([]byte(result))
	}

	return
}
