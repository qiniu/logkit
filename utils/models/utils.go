package models

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/json-iterator/go"

	"github.com/qiniu/pandora-go-sdk/pipeline"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/times"
)

type File struct {
	Info os.FileInfo
	Path string
}

// FileInfos attaches the methods of Interface to []int64, sorting in decreasing order.
type FileInfos []os.FileInfo

func (p FileInfos) Len() int           { return len(p) }
func (p FileInfos) Less(i, j int) bool { return ModTimeLater(p[i], p[j]) }
func (p FileInfos) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p FileInfos) Sort() { sort.Sort(p) }

// SortFilesByTime 按照文件更新的unixnano从大到小排，即最新的文件在前,相同时间的则按照文件名字典序，字典序在后面的排在前面
func SortFilesByTime(files FileInfos) (soredfiles []os.FileInfo) {
	files.Sort()
	return files
}

// ModTimeLater 按最后修改时间进行比较
func ModTimeLater(f1, f2 os.FileInfo) bool {
	if f1.ModTime().UnixNano() != f2.ModTime().UnixNano() {
		return f1.ModTime().UnixNano() > f2.ModTime().UnixNano()
	}
	return f1.Name() > f2.Name()
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
	readDoneFileLines, err := ReadFileContent(doneFilePath)
	if err != nil {
		return
	}
	var readDoneFiles []string
	for _, v := range readDoneFileLines {
		sps := strings.Split(v, "\t")
		readDoneFiles = append(readDoneFiles, sps[0])
	}

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
	switch len(slice) {
	case 1:
		return slice, nil
	case 2:
		rgexpr := "^%\\{\\[\\S+\\]}$" // --->  %{[type]}
		r, _ := regexp.Compile(rgexpr)
		slice[0] = strings.TrimSpace(slice[0])
		bol := r.MatchString(slice[0])
		if bol {
			rs := []rune(slice[0])
			slice[0] = string(rs[3 : len(rs)-2])
			return slice, nil
		}
	default:
	}
	return nil, errors.New("parameters error,  you can write two parameters like: %%{[type]}, default or only one: default")
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

// extractZipFileToPath 写出 *zip.File 对象的内容到指定路径
func extractZipFileToPath(f *zip.File, path string) error {
	r, err := f.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	return nil
}

// DecompressZip 将 ZIP 格式的文件解包到指定目录并返回指定文件所在的解包后的目录，
// 如果存在多个同名指定文件，则返回第一个找到的目录
func DecompressZip(srcPath, dstPath, targetFile string) (targetDir string, _ error) {
	if err := os.MkdirAll(dstPath, os.ModePerm); err != nil {
		return "", err
	}

	zr, err := zip.OpenReader(srcPath)
	if err != nil {
		return "", err
	}
	defer zr.Close()

	foundTarget := false
	for _, f := range zr.File {
		fpath := filepath.Join(dstPath, f.Name)
		if f.FileInfo().IsDir() {
			if err = os.MkdirAll(fpath, f.Mode()); err != nil {
				return "", err
			}
			continue
		}

		if !foundTarget && strings.HasSuffix(fpath, targetFile) {
			foundTarget = true
			targetDir = filepath.Dir(fpath)
		}
		if err = extractZipFileToPath(f, fpath); err != nil {
			return "", err
		}
	}

	if !foundTarget {
		return "", errors.New("target file does not exist")
	}
	return targetDir, nil
}

// extractTarFileToPath 写出 *tar.Reader 对象的内容到指定路径
func extractTarFileToPath(tr *tar.Reader, info os.FileInfo, path string) error {
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = io.Copy(w, tr)
	if err != nil {
		return err
	}
	return nil
}

// DecompressTarGzip 将 TAR.GZ 格式的文件解包到指定目录并返回指定文件所在的解包后的目录，
// 如果存在多个同名指定文件，则返回第一个找到的目录
func DecompressTarGzip(srcPath, dstPath, targetFile string) (targetDir string, _ error) {
	if err := os.MkdirAll(dstPath, os.ModePerm); err != nil {
		return "", err
	}

	srcFile, err := os.Open(srcPath)
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
	foundTarget := false
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}

		fpath := filepath.Join(dstPath, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(fpath, info.Mode()); err != nil {
				return "", err
			}
			continue
		}

		if !foundTarget && strings.HasSuffix(fpath, targetFile) {
			foundTarget = true
			targetDir = filepath.Dir(fpath)
		}

		if err = extractTarFileToPath(tr, info, fpath); err != nil {
			return "", err
		}
	}

	if !foundTarget {
		return "", errors.New("target file does not exist")
	}
	return targetDir, nil
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
			if _, ok := curr[k].(Data); !ok {
				if coercive {
					n := make(map[string]interface{})
					curr[k] = n
				} else {
					err := fmt.Errorf("SetMapValue failed, %v is not the type of map[string]interface{}", curr[k])
					return err
				}
			}
		}
		if m, ok := curr[k].(Data); ok {
			curr = map[string]interface{}(m)
		} else {
			curr = curr[k].(map[string]interface{})
		}
	}
	curr[keys[len(keys)-1]] = val
	return nil
}

//通过层级key设置value值, 如果keys不存在则不加前缀，否则加前缀
func SetMapValueExistWithPrefix(m map[string]interface{}, val interface{}, prefix string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	var curr map[string]interface{}
	curr = m
	for _, k := range keys[0 : len(keys)-1] {
		finalVal, ok := curr[k]
		if !ok {
			n := make(map[string]interface{})
			curr[k] = n
			curr = n
			continue
		}
		//判断val是否为map[string]interface{}类型
		if curr, ok = finalVal.(map[string]interface{}); ok {
			continue
		}
		if curr, ok = finalVal.(Data); ok {
			continue
		}
		return fmt.Errorf("SetMapValueWithPrefix failed, %v is not the type of map[string]interface{}", keys)
	}
	//判断val(k)是否存在
	_, exist := curr[keys[len(keys)-1]]
	if exist {
		curr[prefix+"_"+keys[len(keys)-1]] = val
	} else {
		curr[keys[len(keys)-1]] = val
	}
	return nil
}

//通过层级key删除key-val,并返回被删除的val,是否删除成功
//如果key不存在,则返回 nil,false
func DeleteMapValue(m map[string]interface{}, keys ...string) (interface{}, bool) {
	var val interface{}
	val = m
	for i, k := range keys {
		if m, ok := val.(Data); ok {
			val = map[string]interface{}(m)
		}
		if m, ok := val.(map[string]interface{}); ok {
			if temp, ok := m[k]; ok {
				if i == len(keys)-1 {
					delete(m, keys[len(keys)-1])
					return temp, true
				}
				val = temp
			} else {
				return nil, false
			}
		}
	}
	return nil, false
}

func PickMapValue(m map[string]interface{}, pick map[string]interface{}, keys ...string) {
	var val interface{}
	val = m
	if len(keys) == 0 {
		return
	}
	if m, ok := val.(Data); ok {
		val = map[string]interface{}(m)
	}

	if _, ok := val.(map[string]interface{}); !ok {
		return
	}

	v, ok := val.(map[string]interface{})[keys[0]]
	if !ok {
		return
	}

	if len(keys) == 1 {
		pick[keys[0]] = v
		return
	}

	if m, ok := v.(Data); ok {
		v = map[string]interface{}(m)
	}
	// 判断keys[0]的值是不是map，如果不是，keys[1]pick的值为空，退出该keys的pick
	if _, ok := v.(map[string]interface{}); !ok {
		return
	}

	if _, ok := pick[keys[0]]; !ok {
		pick[keys[0]] = map[string]interface{}{}
	}
	PickMapValue(v.(map[string]interface{}), pick[keys[0]].(map[string]interface{}), keys[1:]...)
	if len(pick[keys[0]].(map[string]interface{})) == 0 {
		delete(pick, keys[0])
	}
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
	curKeys := keys
	for i, k := range curKeys {
		//判断val是否为map[string]interface{}类型
		if m, ok := val.(Data); ok {
			val = map[string]interface{}(m)
		}
		if _, ok := val.(map[string]interface{}); ok {
			//判断val(k)是否存在
			if _, ok := val.(map[string]interface{})[k]; ok {
				val = val.(map[string]interface{})[k]
			} else {
				curKeys = curKeys[0 : i+1]
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

func Bool2String(i bool) string {
	if i {
		return "true"
	}
	return "false"
}

func ConvertDate(layoutBefore, layoutAfter string, offset int, loc *time.Location, v interface{}) (interface{}, error) {
	var s int64
	switch newv := v.(type) {
	case int64:
		s = newv
	case int:
		s = int64(newv)
	case int32:
		s = int64(newv)
	case int16:
		s = int64(newv)
	case uint64:
		s = int64(newv)
	case uint32:
		s = int64(newv)
	case string:
		if layoutBefore != "" {
			t, err := time.ParseInLocation(layoutBefore, newv, loc)
			if err != nil {
				return v, fmt.Errorf("can not parse %v with layout %v", newv, layoutAfter)
			}
			return FormatWithUserOption(layoutAfter, offset, t), nil
		}
		t, err := times.StrToTimeLocation(newv, loc)
		if err != nil {
			return v, err
		}
		return FormatWithUserOption(layoutAfter, offset, t), nil
	case json.Number:
		jsonNumber, err := newv.Int64()
		if err != nil {
			return v, err
		}
		s = jsonNumber
	default:
		return v, fmt.Errorf("can not parse %v type %v as date time", v, reflect.TypeOf(v))
	}
	news := s
	timestamp := strconv.FormatInt(news, 10)
	timeSecondPrecision := 16
	//补齐16位
	for i := len(timestamp); i < timeSecondPrecision; i++ {
		timestamp += "0"
	}
	// 取前16位，截取精度 微妙
	timestamp = timestamp[0:timeSecondPrecision]
	t, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return v, err
	}
	tm := time.Unix(0, t*int64(time.Microsecond))
	return FormatWithUserOption(layoutAfter, offset, tm), nil
}

func FormatWithUserOption(layoutAfter string, offset int, t time.Time) interface{} {
	t = t.Add(time.Duration(offset) * time.Hour)
	if layoutAfter != "" {
		return t.Format(layoutAfter)
	}
	return t.Format(time.RFC3339Nano)
}

func ReadFileContent(path string) (content []string, err error) {
	body, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Errorf("file %s not exisit", path)
			return
		}
		log.Errorf("read file %s error %v", path, err)
		return
	}
	content = TrimeList(strings.Split(string(body), "\n"))
	return
}

func GetMapList(data string) map[string]string {
	v := strings.Split(data, ",")
	var newV []string
	for _, i := range v {
		trimI := strings.TrimSpace(i)
		if len(trimI) > 0 {
			newV = append(newV, trimI)
		}
	}
	ret := make(map[string]string)
	for _, v := range newV {
		fids := strings.Fields(v)
		if len(fids) >= 2 {
			ret[fids[0]] = fids[1]
		}
	}
	return ret
}

//为了提升性能做的一个预先检查，避免CPU浪费
func CheckPandoraKey(key string) bool {
	for _, c := range key {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			continue
		}
		return false
	}
	return true
}

func DeepConvertKey(data map[string]interface{}) map[string]interface{} {
	for k, v := range data {
		switch nv := v.(type) {
		case map[string]interface{}:
			v = DeepConvertKey(nv)
		case Data:
			v = DeepConvertKey(nv)
		}
		valid := CheckPandoraKey(k)
		if !valid {
			delete(data, k)
			k, _ := pipeline.PandoraKey(k)
			data[k] = v
		}
	}
	return data
}

//注意：cache如果是nil，这个函数就完全没有意义，不如调用 DeepConvertKey
func DeepConvertKeyWithCache(data map[string]interface{}, cache map[string]KeyInfo) map[string]interface{} {
	for k, v := range data {
		if nv, ok := v.(map[string]interface{}); ok {
			v = DeepConvertKeyWithCache(nv, cache)
		} else if nv, ok := v.(Data); ok {
			v = DeepConvertKeyWithCache(nv, cache)
		}
		keyInfo, exist := cache[k]
		if !exist {
			keyInfo.NewKey, keyInfo.Valid = pipeline.PandoraKey(k)
			if cache == nil {
				cache = make(map[string]KeyInfo)
			}
			cache[k] = keyInfo
		}
		if !keyInfo.Valid {
			delete(data, k)
			data[keyInfo.NewKey] = v
		}
	}
	return data
}

func CheckErr(err error) error {
	se, ok := err.(*StatsError)
	var errorCnt int64
	if ok {
		errorCnt = se.Errors
		err = errors.New(se.LastError)
	} else {
		errorCnt = 1
	}

	if err != nil {
		return fmt.Errorf("%v parse line errors occured, error %v ", errorCnt, err.Error())
	}
	return nil
}

type KeyInfo struct {
	Valid  bool
	NewKey string
}

func TruncateStrSize(err string, size int) string {
	if len(err) <= size {
		return err
	}

	return fmt.Sprintf(err[:size]+"......(only show %d bytes, remain %d bytes)",
		size, len(err)-size)
}

func IsSubMetaExpire(submetaExpire, expire time.Duration) bool {
	return submetaExpire.Nanoseconds() > 0 && expire.Nanoseconds() > 0
}

func IsSubmetaExpireValid(submetaExpire, expire time.Duration) bool {
	return submetaExpire.Nanoseconds() > 0 && submetaExpire < expire
}
