package utils

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/qiniu/log"
	"gopkg.in/mgo.v2/bson"
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
	return fmt.Sprintf("%s; %s; %s; %s %s", oi.Hostname, oi.OS, oi.Core, oi.Kernel, oi.Platform)
}

func GetExtraInfo() map[string]string {
	osInfo := GetOSInfo()
	exInfo := make(map[string]string)
	exInfo["core"] = osInfo.Core
	exInfo["hostname"] = osInfo.Hostname
	exInfo["osinfo"] = osInfo.OS + "-" + osInfo.Kernel + "-" + osInfo.Platform
	if ip, err := GetLocalIP(); err != nil {
		exInfo["localip"] = ip
	}
	return exInfo
}

func IsJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
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

//根据key字符串,拆分出层级keys数据
func GetKeys(keyStr string) []string {
	if keyStr == "" {
		return []string{}
	}
	separator := "."
	keys := strings.Split(keyStr, separator)
	return keys
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

//通过层级key设置value值.
//如果key不存在,将会自动创建.
//当coercive为true时,将不考虑数据丢弃,强制set数据
//当coercive为false时,如果操作将造成数据丢失,返回err,且不执行任何操作
//造成数据丢失的两种情况:1.被替换的value是一个map 2.在设置层级key中(非最后一个)遇到一个非map值
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
	if _, ok := curr[keys[len(keys)-1]].(map[string]interface{}); ok {
		if coercive {
			curr[keys[len(keys)-1]] = val
			return nil
		} else {
			err := fmt.Errorf("SetMapValue failed, %v is the type of map[string]interface{}", curr[keys[len(keys)-1]])
			return err
		}
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

//深度拷贝
func DeepCopy(value interface{}) interface{} {
	if valueMap, ok := value.(map[string]interface{}); ok {
		newMap := make(map[string]interface{})
		for k, v := range valueMap {
			newMap[k] = DeepCopy(v)
		}

		return newMap
	} else if valueSlice, ok := value.([]interface{}); ok {
		newSlice := make([]interface{}, len(valueSlice))
		for k, v := range valueSlice {
			newSlice[k] = DeepCopy(v)
		}

		return newSlice
	} else if valueMap, ok := value.(bson.M); ok {
		newMap := make(bson.M)
		for k, v := range valueMap {
			newMap[k] = DeepCopy(v)
		}
	}
	return value
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

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1", fmt.Errorf("Get local IP error: %v\n", err)
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "127.0.0.1", errors.New("no local IP found")
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
