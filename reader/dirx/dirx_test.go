package dirx

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/utils/models"
)

func createFileWithContent(filepathn, lines string) {
	file, err := os.OpenFile(filepathn, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
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
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir1)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "xyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\n")
	expectResults := map[string]int{
		"abc123\n": 5,
		"xyz\n":    10,
		"hahaha\n": 3,
	}
	actualResults := make(map[string]int)
	logPathPattern := filepath.Join(dirName, "logs/*")
	c := conf.MapConf{
		"log_path":        logPathPattern,
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
	time.Sleep(6 * time.Second)
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
		if maxNum >= 18 || emptyNum > 10 {
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
		if maxNum >= 7 || emptyNum > 10 {
			break
		}
	}
	t.Log("Reader has finished reading one")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\nx\nabc\nx\n")
	time.Sleep(6 * time.Second)
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
		if maxNum >= 10 || emptyNum > 10 {
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
		if maxNum >= 2 || emptyNum > 10 {
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
		if maxNum >= 9 || emptyNum > 10 {
			break
		}
	}
	t.Log("Reader has finished reading one again")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "ab1\nab2\nab3\n")
	time.Sleep(11 * time.Second)
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
		if maxNum >= 12 || emptyNum > 10 {
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
		if maxNum >= 5 || emptyNum > 10 {
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
		if maxNum >= 7 || emptyNum > 10 {
			break
		}
	}
	t.Log("Reader has finished reading one again")

	// 确保上个 reader 已过期，新的 reader 已经探测到并创建成功
	createDirWithName(dir2)
	createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	time.Sleep(11 * time.Second)
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
		if maxNum >= 10 || emptyNum > 10 {
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

	assert.NoError(t, dr.Close())
	t.Log("Reader has closed")

	r, err = NewReader(meta, c)
	assert.NoError(t, err)

	err = r.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	dr = r.(*Reader)
	assert.NoError(t, dr.Start())
	emptyNum = 0
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
			t.Log("Data:", data, dr.Source(), maxNum)
			actualResults[data]++
			maxNum++
		} else {
			emptyNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 3 || emptyNum > 10 {
			break
		}
	}
	t.Log("Reader has finished reading two")

	assert.EqualValues(t, expectResults, actualResults)
	assert.Equal(t, StatsInfo{}, dr.Status())
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
		if maxNum >= 8 || emptyNum > 10 {
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
	dr = r.(*Reader)
	assert.NoError(t, dr.Start())
	t.Log("Reader has started again")
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
		if maxNum >= 8 || emptyNum > 10 {
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
		if maxNum >= 8 {
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
