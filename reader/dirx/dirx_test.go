package dirx

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/log"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/utils/models"
)

func createFileWithContent(filepathn, lines string) {
	file, err := os.OpenFile(filepathn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, DefaultFilePerm)
	if err != nil {
		log.Error(err)
		return
	}
	file.WriteString(lines)
	file.Close()
}

func appendFileWithContent(filepathn, lines string) {
	file, err := os.OpenFile(filepathn, os.O_APPEND|os.O_WRONLY, DefaultFilePerm)
	if err != nil {
		log.Error(err)
		return
	}
	file.WriteString(lines)
	file.Close()
}

func createDirWithName(dirx string) {
	err := os.MkdirAll(dirx, DefaultDirPerm)
	if err != nil {
		log.Error(err)
		return
	}
}

func TestStart(t *testing.T) {
	c := make(chan string)

	// 以下几个函数 sleep 时间较长，放在此处并发执行
	funcMap := map[string]func(*testing.T){
		"multiReaderOneLineTest":          multiReaderOneLineTest,
		"multiReaderMultiLineTest":        multiReaderMultiLineTest,
		"multiReaderEmptyDirxTest":        multiReaderEmptyDirxTest,
		"multiReaderSyncMetaOneLineTest":  multiReaderSyncMetaOneLineTest,
		"multiReaderSyncMetaMutilineTest": multiReaderSyncMetaMutilineTest,
		"multiReaderNewestTest":           multiReaderNewestTest,
		"multiReaderNewestOffsetTest":     multiReaderNewestOffsetTest,
		"multiReaderSameInodeTest":        multiReaderSameInodeTest,
		"readerExpireDeleteTest":          readerExpireDeleteTest,
		"readerExpireDeleteTarTest":       readerExpireDeleteTarTest,
	}

	for k, f := range funcMap {
		go func(k string, f func(*testing.T), c chan string) {
			f(t)
			c <- k
		}(k, f, c)
	}
	funcCnt := len(funcMap)
	for i := 0; i < funcCnt; i++ {
		<-c
	}
}

func multiReaderOneLineTest(t *testing.T) {
	dirName := "TestMultiReaderOneLine"
	dir1 := filepath.Join(dirName, "logs/abc")
	dir2 := filepath.Join(dirName, "logs/xyz")
	dir3 := filepath.Join(dirName, "logs/abc123")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir3file3 := filepath.Join(dir3, "file3.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "xyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\n")
	createFileWithContent(dir3file3, "xyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\n")
	expectResults := map[string]int{
		"abc123\n": 5,
		"xyz\n":    10,
		"hahaha\n": 3,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirName, "logs/*")
	ignoreLogPathPattern := filepath.Join(dirName, "logs/*123")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"ignore_log_path": ignoreLogPathPattern,
		"stat_interval":   "1s",
		"expire":          "5s",
		"submeta_expire":  "0h",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirName,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	assert.Equal(t, 5*time.Second, dr.expire)
	assert.Equal(t, time.Duration(0), dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 15 || emptyNum > 10 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "hahaha\nhahaha\nhahaha\n")
	time.Sleep(5 * time.Second)
	assert.Equal(t, 1, dr.dirReaders.Num(), "Number of readers")
	assert.Equal(t, "TestMultiReaderOneLine/logs/xyz", dr.dirReaders.getReaders()[0].originalPath)

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func multiReaderMultiLineTest(t *testing.T) {
	dirName := "TestMultiReaderMultiLine"
	dir1 := filepath.Join(dirName, "logs/abc")
	dir2 := filepath.Join(dirName, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "abc456\n789\nabc456\n789\n")
	expectResults := map[string]int{
		"abc123\n":      5,
		"abc456\n789\n": 2,
		"abc\nx\n":      3,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirName, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "5s",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirName,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	r.SetMode(ReadModeHeadPatternString, "^abc*")
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	assert.Equal(t, 5*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\nx\nabc\nx\n")
	time.Sleep(5 * time.Second)
	assert.Equal(t, 1, dr.dirReaders.Num(), "Number of readers")
	assert.Equal(t, "TestMultiReaderMultiLine/logs/xyz", dr.dirReaders.getReaders()[0].originalPath)

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func multiReaderSyncMetaOneLineTest(t *testing.T) {
	dirName := "TestMultiReaderSyncMetaOneLine"
	dir1 := filepath.Join(dirName, "logs/abc")
	dir2 := filepath.Join(dirName, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "123\n124\n125\n126\n127\n")
	createFileWithContent(dir1file2, "456\n457\n458\n459\n")
	expectResults := map[string]int{
		"123\n": 1,
		"124\n": 1,
		"125\n": 1,
		"126\n": 1,
		"127\n": 1,
		"456\n": 1,
		"457\n": 1,
		"458\n": 1,
		"459\n": 1,
		"ab1\n": 1,
		"ab2\n": 1,
		"ab3\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirName, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "10s",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirName,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 10*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	assert.NoError(t, dr.Close())
	t.Log("Reader has closed")

	r, err = NewReader(meta, c)
	assert.NoError(t, err)

	dr = r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started again", maxNum)
	defer dr.Close()
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one again")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "ab1\nab2\nab3\n")
	time.Sleep(10 * time.Second)
	assert.Equal(t, 1, dr.dirReaders.Num(), "Number of readers")
	assert.Equal(t, "TestMultiReaderSyncMetaOneLine/logs/xyz", dr.dirReaders.getReaders()[0].originalPath)

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func multiReaderSyncMetaMutilineTest(t *testing.T) {
	dirname := "TestMultiReaderSyncMetaMutiline"
	dir1 := filepath.Join(dirname, "logs/abc")
	dir2 := filepath.Join(dirname, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	createFileWithContent(dir1file2, "abc456\n789\nabc012\n111\n")
	expectResults := map[string]int{
		"abc123\n":      1,
		"abc124\n":      1,
		"abc125\n":      1,
		"abc126\n":      1,
		"abc127\n":      1,
		"abc456\n789\n": 1,
		"abc012\n111\n": 1,
		"abc\nx\n":      1,
		"abc\ny\n":      1,
		"abc\nz\n":      1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirname, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "10s",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirname,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	r.SetMode(ReadModeHeadPatternString, "^abc*")
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 10*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	assert.NoError(t, dr.Close())
	t.Log("Reader has closed")

	r, err = NewReader(meta, c)
	assert.NoError(t, err)

	r.SetMode(ReadModeHeadPatternString, "^abc*")
	dr = r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started again", maxNum)
	defer dr.Close()
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one again")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	time.Sleep(10 * time.Second)
	assert.Equal(t, 1, dr.dirReaders.Num(), "Number of readers")

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func multiReaderNewestTest(t *testing.T) {
	dirname := "multiReaderNewestTest"
	dir1 := filepath.Join(dirname, "logs/abc")
	dir2 := filepath.Join(dirname, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expectResults := map[string]int{
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirname, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "10s",
		"max_open_files":  "128",
		"read_from":       "newest",
		"reader_buf_size": "1024",
		"meta_path":       dirname,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	err = r.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	assert.Equal(t, 10*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			t.Log("Data:", data, dr.Source(), maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if emptyNum > 5 {
			break
		}
	}
	assert.EqualValues(t, 0, maxNum)
	t.Log("Reader has finished reading one")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	assert.Equal(t, 1, dr.dirReaders.Num(), "Number of readers")

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, dr.Source(), maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func multiReaderNewestOffsetTest(t *testing.T) {
	dirname := "multiReaderNewestOffsetTest"
	dir1 := filepath.Join(dirname, "logs/abc")
	dir2 := filepath.Join(dirname, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expectResults := map[string]int{
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
		"abc\na\n": 1,
		"abc\nb\n": 1,
		"abc\nc\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirname, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "10s",
		"max_open_files":  "128",
		"read_from":       "newest",
		"reader_buf_size": "1024",
		"meta_path":       dirname,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	err = r.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	assert.Equal(t, 10*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			t.Log("Data:", data, dr.Source(), maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if emptyNum > 60 {
			break
		}
	}
	assert.EqualValues(t, 0, maxNum)
	t.Log("Reader has finished reading one")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	appendFileWithContent(dir1file1, "abc\na\nabc\nb\nabc\nc\n")
	time.Sleep(5 * time.Second)
	assert.Equal(t, 2, dr.dirReaders.Num(), "Number of readers")

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, dr.Source(), maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func multiReaderSameInodeTest(t *testing.T) {
	
}

func TestMultiReaderSameInodeTest(t *testing.T) {
	dirname := "multiReaderSameInodeTest"
	dir1 := filepath.Join(dirname, "logs/abc")
	dir2 := filepath.Join(dirname, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expectResults := map[string]int{
		"abc123\n": 3,
		"abc124\n": 3,
		"abc125\n": 3,
		"abc126\n": 3,
		"abc127\n": 2,
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirname, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "0s",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirname,
		"mode":            ModeDirx,
		"read_same_inode": "true",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	err = r.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	createFileWithContent(dir1file2, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	time.Sleep(5 * time.Second)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	time.Sleep(5 * time.Second)

	assert.Equal(t, 2, dr.dirReaders.Num(), "Number of readers")

	t.Log("Reader has started to read two")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading two")
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\n")
	time.Sleep(5 * time.Second)
	assert.Equal(t, 2, dr.dirReaders.Num(), "Number of readers")

	t.Log("Reader has started to read three")
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading three")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
	files1, err := ioutil.ReadDir(dir1)
	assert.NoError(t, err)
	files2, err := ioutil.ReadDir(dir2)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(files1)+len(files2))
}

func readerExpireDeleteTest(t *testing.T) {
	dirname := "readerExpireDeleteTest"
	dir1 := filepath.Join(dirname, "logs/dir1")
	dir2 := filepath.Join(dirname, "logs/dir2")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	expectResults := map[string]int{
		"abc123\n": 2,
		"abc124\n": 2,
		"abc125\n": 2,
		"abc126\n": 2,
		"abc127\n": 2,
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirname, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "10s",
		KeyExpireDelete:   "true",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirname,
		"mode":            ModeDirx,
		"read_same_inode": "true",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	err = r.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	time.Sleep(time.Second)
	createFileWithContent(dir1file2, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	time.Sleep(2 * time.Second)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum, dr.Source())
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 100 || emptyNum > 100 {
			break
		}
	}

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
	time.Sleep(8 * time.Second)
	files1, err := ioutil.ReadDir(dir1)
	assert.Equal(t, true, os.IsNotExist(err))
	files2, err := ioutil.ReadDir(dir2)
	assert.Equal(t, true, os.IsNotExist(err))
	assert.Equal(t, 0, len(files1)+len(files2))
}

func tarit(source, target string) error {
	filename := filepath.Base(source)
	target = filepath.Join(target, fmt.Sprintf("%s.tar", filename))
	tarfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer tarfile.Close()

	tarball := tar.NewWriter(tarfile)
	defer tarball.Close()

	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	return filepath.Walk(source,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}
			if baseDir != "" {
				header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
			}
			if err := tarball.WriteHeader(header); err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(tarball, file)
			return err
		})
}

func readerExpireDeleteTarTest(t *testing.T) {
	dirname := "readerExpireDeleteTarTest"
	dir1 := filepath.Join(dirname, "logs/dir1")
	dir2 := filepath.Join(dirname, "logs/dir2")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")
	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc111\nabc124\nabc125\nabc126\nabc127\n")
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	createFileWithContent(dir1file2, "abc122\nabc124\nabc125\nabc126\nabc127\n")
	err := tarit(dir1, filepath.Join(dirname, "logs"))
	assert.NoError(t, err)
	err = tarit(dir2, filepath.Join(dirname, "logs"))
	assert.NoError(t, err)
	os.RemoveAll(dir1)
	os.RemoveAll(dir2)

	expectResults := map[string]int{
		"abc111\n": 1,
		"abc122\n": 1,
		"abc124\n": 2,
		"abc125\n": 2,
		"abc126\n": 2,
		"abc127\n": 2,
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirname, "logs/*.tar")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "100s",
		KeyExpireDelete:   "true",
		"max_open_files":  "128",
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       dirname,
		"mode":            ModeDirx,
		"read_same_inode": "true",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	err = r.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	time.Sleep(2 * time.Second)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum, dr.Source())
			if data == "abc111\n" {
				assert.Equal(t, "dir1/file1.log", dr.Source())
			}
			if data == "abc122\n" {
				assert.Equal(t, "dir1/file2.log", dr.Source())
			}
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("maxNum ", maxNum, "emptyNum", emptyNum)
	time.Sleep(60 * time.Second)
	assert.EqualValues(t, len(expectResults), len(actualResults))
	for k, v := range expectResults {
		actualV, ok := actualResults[k]
		assert.True(t, ok)
		assert.EqualValues(t, v, actualV)
	}
	assert.Equal(t, StatsInfo{}, dr.Status())
	time.Sleep(5 * time.Second)
	files1, err := ioutil.ReadDir(dir1)
	assert.Equal(t, true, os.IsNotExist(err))
	files2, err := ioutil.ReadDir(dir2)
	assert.Equal(t, true, os.IsNotExist(err))
	assert.Equal(t, 0, len(files1)+len(files2))
}

func TestMultiReaderReset(t *testing.T) {
	dirName := "TestMultiReaderReset"
	dir := filepath.Join(dirName, "logs/abc")
	metaDir := filepath.Join(dirName, "meta")
	file1 := filepath.Join(dir, "file1.log")
	file2 := filepath.Join(dir, "file2.log")
	file3 := filepath.Join(dir, "file3.log")
	file4 := filepath.Join(dir, "file4.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir)
	createFileWithContent(file1, "abc111\nabc112\n")
	createFileWithContent(file2, "abc121\nabc122\n")
	createFileWithContent(file3, "abc131\nabc132\n")
	createFileWithContent(file4, "abc141\nabc142\n")
	expectResults := map[string]int{
		"abc111\n": 1,
		"abc112\n": 1,
		"abc121\n": 1,
		"abc122\n": 1,
		"abc131\n": 1,
		"abc132\n": 1,
		"abc141\n": 1,
		"abc142\n": 1,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirName, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       metaDir,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 0*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	emptyNum := 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
			t.Log(data, maxNum)
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	assert.NoError(t, dr.Close())
	t.Log("Reader has closed")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())

	// 重置
	assert.NoError(t, dr.Reset())
	t.Log("Reader has resetted")

	r, err = NewReader(meta, c)
	assert.Nil(t, err)
	dr = r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started again")
	defer dr.Close()

	actualResults = make(map[string]int)
	maxNum = 0
	emptyNum = 0
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 60 || emptyNum > 60 {
			break
		}
	}
	t.Log("Reader has finished reading one again")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
}

func TestReaderErrBegin(t *testing.T) {
	dirName := "TestReaderErr"
	dir := filepath.Join(dirName, "logs/abc")
	metaDir := filepath.Join(dirName, "meta")
	file1 := filepath.Join(dir, "file1.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir)

	file, err := os.OpenFile(file1, os.O_CREATE|os.O_WRONLY, 0200)
	if err != nil {
		log.Error(err)
		return
	}
	_, err = file.WriteString("abc111\nabc112\n")
	assert.NoError(t, err)
	assert.NoError(t, file.Close())

	logPathPattern := filepath.Join(dirName, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"read_from":       "oldest",
		"reader_buf_size": "1024",
		"meta_path":       metaDir,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	r, err := NewReader(meta, c)
	assert.NoError(t, err)

	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 0*time.Second, dr.expire)
	assert.Equal(t, 720*time.Hour, dr.submetaExpire)

	maxNum := 0
	for {
		_, err = dr.ReadLine()
		if err != nil {
			break
		}

		maxNum++
		if err == io.EOF {
			break
		}
		if maxNum >= 60 {
			break
		}
	}
	if !strings.Contains(err.Error(), os.ErrPermission.Error()) {
		t.Errorf("expect permission error, but got: %v", err)
	}
	assert.NoError(t, dr.Close())
	t.Log("Reader has closed")
}

func multiReaderEmptyDirxTest(t *testing.T) {
	dirName := "multiReaderEmptyDirxTest"
	dir1 := filepath.Join(dirName, "logs/abc")
	dir2 := filepath.Join(dirName, "logs/xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createDirWithName(dir2)
	go func() {
		time.Sleep(5 * time.Second)
		dir1file2 := filepath.Join(dir1, "file2.log")
		createFileWithContent(dir1file2, "xyz1\nxyz2\nxyz3\nxyz4\nxyz5\nxyz6\nxyz7\nxyz8\nxyz9\nxyz10\n")
	}()

	logPathPattern := filepath.Join(dirName, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"stat_interval":   "1s",
		"expire":          "0s",
		"submeta_expire":  "720h",
		"max_open_files":  "128",
		"read_from":       "newest",
		"reader_buf_size": "1024",
		"meta_path":       dirName,
		"mode":            ModeDirx,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.Nil(t, err)
	r, err := NewReader(meta, c)
	assert.Nil(t, err)

	dr := r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started")
	defer dr.Close()

	maxNum := 0
	emptyNum := 0
	var lastError error
	for {
		data, err := dr.ReadLine()
		if data != "" {
			t.Log("Data:", data, maxNum)
			maxNum++
		} else {
			if err != nil {
				lastError = err
			}
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 10 {
			break
		}
	}
	t.Log("Reader has finished reading one")
	assert.Equal(t, 10, maxNum)
	if emptyNum < 0 {
		t.Fatalf("expect > 0, but got: %d", emptyNum)
	}
	assert.NotNil(t, lastError)
	assert.Equal(t, "file does not exist", lastError.Error())
}
