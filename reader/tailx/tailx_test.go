package tailx

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
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
	err := os.Mkdir(dirx, DefaultDirPerm)
	if err != nil {
		log.Error(err)
		return
	}
}

func Test_ActiveReader(t *testing.T) {
	t.Parallel()
	testfile := "Test_ActiveReader"
	CreateDir()
	meta, err := reader.NewMeta(MetaDir, MetaDir, testfile, ModeDir, "", reader.DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	defer DestroyDir()
	ppath := filepath.Join(Dir, testfile)
	testContent := "1234567812345678"
	CreateFile(ppath, testContent)
	ppath, err = filepath.Abs(ppath)
	assert.NoError(t, err)
	r := &Reader{
		msgChan: make(chan Result),
		errChan: make(chan error),
		meta:    meta,
	}
	ar, err := NewActiveReader(ppath, ppath, WhenceOldest, "", r)
	assert.NoError(t, err)
	go ar.Run()
	data := <-r.msgChan
	assert.Equal(t, testContent, data.result)

	assert.Equal(t, StatsInfo{}, ar.Status())
	ar.Close()
}

func TestStart(t *testing.T) {
	t.Parallel()

	// 以下几个函数 sleep 时间较长，放在此处并发执行
	funcMap := map[string]func(*testing.T){
		"multiReaderOneLineTest":          multiReaderOneLineTest,
		"multiReaderMultiLineTest":        multiReaderMultiLineTest,
		"multiReaderSyncMetaOneLineTest":  multiReaderSyncMetaOneLineTest,
		"multiReaderSyncMetaMutilineTest": multiReaderSyncMetaMutilineTest,
		"multiReaderNewestTest":           multiReaderNewestTest,
		"multiReaderNewestOffsetTest":     multiReaderNewestOffsetTest,
		"multiReaderNewestModify":         multiReaderNewestModify,
		"readerExpireDeleteTest":          readerExpireDeleteTest,
		"readerGZDeleteTest":              readerGZDeleteTest,
		"multiReaderOutRunTime":           multiReaderOutRunTime,
		"multiReaderInRunTime":            multiReaderInRunTime,
		"logOverwrittenTest":              logOverwrittenTest,
	}
	wg := new(sync.WaitGroup)
	for k, f := range funcMap {
		wg.Add(1)
		go func(k string, f func(*testing.T), wg *sync.WaitGroup) {
			f(&testing.T{})
			wg.Done()
		}(k, f, wg)
	}
	wg.Wait()
}

func multiReaderOneLineTest(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderOneLineTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "xyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\n")
	expresult := map[string]int{
		"abc123\n": 5,
		"xyz\n":    10,
		"hahaha\n": 3,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "15s",
		"submeta_expire":  "0s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.Nil(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.Nil(t, mr.Start())
	t.Log("Reader has started")
	defer mr.Close()

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, time.Duration(0), mr.submetaExpire)

	go func() {
		time.Sleep(10 * time.Second)
		createFileWithContent(dir2file1, "hahaha\nhahaha\nhahaha\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 15 || spacenum > 20 {
			break
		}
		t.Log(data)
	}
	time.Sleep(15 * time.Second)
	t.Log("mr finished read one")
	var lens int
	mr.armapmux.Lock()
	lens = len(mr.fileReaders)
	mr.armapmux.Unlock()
	assert.Equal(t, 1, lens, "activereader number")

	t.Log("mr listen 2 round")

	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		t.Log(data)
		if maxnum >= 18 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finish listen 2 round")

	assert.EqualValues(t, expresult, resultmap)

	assert.Equal(t, StatsInfo{}, mr.Status())
	files1, err := ioutil.ReadDir(dir1)
	assert.NoError(t, err)
	files2, err := ioutil.ReadDir(dir2)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(files1)+len(files2))
}

func readerExpireDeleteTest(t *testing.T) {
	maxnum := 0
	dirname := "readerExpireDeleteTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "xyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\n")
	createFileWithContent(dir2file1, "hahaha\nhahaha\nhahaha\n")
	expresult := map[string]int{
		"abc123\n": 5,
		"xyz\n":    10,
		"hahaha\n": 3,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "10s",
		"submeta_expire":  "0s",
		KeyExpireDelete:   "true",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")
	defer mr.Close()

	assert.Equal(t, 10*time.Second, mr.expire)
	assert.Equal(t, time.Duration(0), mr.submetaExpire)

	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		t.Log(data)
		if maxnum >= 18 || spacenum > 20 {
			break
		}
	}
	mr.ReadLine()
	assert.EqualValues(t, expresult, resultmap)
	assert.Equal(t, StatsInfo{}, mr.Status())
	time.Sleep(10 * time.Second)

	files1, _ := ioutil.ReadDir(dir1)
	files2, _ := ioutil.ReadDir(dir2)
	assert.Equal(t, 0, len(files1)+len(files2))
}

func gzipit(source, target string) error {
	reader, err := os.Open(source)
	if err != nil {
		return err
	}

	filename := filepath.Base(source)
	target = filepath.Join(target, fmt.Sprintf("%s.gz", filename))
	writer, err := os.Create(target)
	if err != nil {
		return err
	}
	defer writer.Close()

	archiver := gzip.NewWriter(writer)
	archiver.Name = filename
	defer archiver.Close()

	_, err = io.Copy(archiver, reader)
	return err
}

func readerGZDeleteTest(t *testing.T) {
	maxnum := 0
	dirname := "readerGZDeleteTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "xyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\nxyz\n")
	createFileWithContent(dir2file1, "hahaha\nhahaha\nhahaha\n")
	assert.NoError(t, gzipit(dir1file1, dir1))
	assert.NoError(t, gzipit(dir1file2, dir1))
	assert.NoError(t, gzipit(dir2file1, dir2))
	os.RemoveAll(dir1file1)
	os.RemoveAll(dir1file2)
	os.RemoveAll(dir2file1)
	expresult := map[string]int{
		"abc123\n": 5,
		"xyz\n":    10,
		"hahaha\n": 3,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.gz")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "10s",
		"submeta_expire":  "0s",
		KeyExpireDelete:   "true",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")
	defer mr.Close()

	assert.Equal(t, 10*time.Second, mr.expire)
	assert.Equal(t, time.Duration(0), mr.submetaExpire)

	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		t.Log(data)
		if maxnum >= 18 || spacenum > 20 {
			break
		}
	}
	mr.ReadLine()
	assert.EqualValues(t, expresult, resultmap)
	assert.Equal(t, StatsInfo{}, mr.Status())
	time.Sleep(20 * time.Second)

	files1, _ := ioutil.ReadDir(dir1)
	files2, _ := ioutil.ReadDir(dir2)
	assert.Equal(t, 0, len(files1)+len(files2))
}

func logOverwrittenTest(t *testing.T) {
	maxnum := 0
	dirname := "logOverwrittenTest"
	dir1file1 := filepath.Join(dirname, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	expresult := map[string]int{
		"abc123\n": 5,
		"11111\n":  1,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "15s",
		"submeta_expire":  "0s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.Nil(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.Nil(t, mr.Start())
	t.Log("Reader has started")
	defer mr.Close()

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, time.Duration(0), mr.submetaExpire)

	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 15 || spacenum > 20 {
			break
		}
		t.Log(data)
	}
	time.Sleep(15 * time.Second)
	t.Log("mr finished read one")
	var lens int
	mr.armapmux.Lock()
	lens = len(mr.fileReaders)
	mr.armapmux.Unlock()
	assert.Equal(t, 1, lens, "activereader number")

	overwritten := "11111"
	cmdStr := fmt.Sprintf("echo %s > %s", overwritten, dir1file1)
	exec.Command(cmdStr)
	t.Log("mr listen 2 round")

	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		t.Log(data)
		if maxnum >= 18 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finish listen 2 round")

	assert.EqualValues(t, expresult, resultmap)
	assert.Equal(t, StatsInfo{}, mr.Status())
}

func multiReaderMultiLineTest(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderMultiLineTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir1file3 := filepath.Join(dir1, "file3.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc123\nabc123\nabc123\nabc123\n")
	createFileWithContent(dir1file2, "abc456\n789\nabc456\n789\n")
	createFileWithContent(dir1file3, "abc456\n789\nabc456\n789\n")
	expresult := map[string]int{
		"abc123\n":      5,
		"abc456\n789\n": 2,
		"abc\nx\n":      3,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	ignoreLogPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*3.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"ignore_log_path": ignoreLogPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "15s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")
	defer mr.Close()

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(10 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\nx\nabc\nx\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 7 || spacenum > 20 {
			break
		}
		t.Log(data)
	}
	time.Sleep(15 * time.Second)
	t.Log("mr finished read one")
	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(ar.originpath)
	}
	mr.armapmux.Unlock()

	t.Log("mr listen 2 round")
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		t.Log(data)
		if maxnum >= 10 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finish listen 2 round")

	assert.EqualValues(t, expresult, resultmap)
}

func multiReaderSyncMetaOneLineTest(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderSyncMetaOneLineTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "123\n124\n125\n126\n127\n")
	createFileWithContent(dir1file2, "456\n457\n458\n459\n")
	expresult := map[string]int{
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
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "15s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(10 * time.Second)
		createFileWithContent(dir2file1, "ab1\nab2\nab3\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 2 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finished read one")

	mr.SyncMeta()
	t.Log("mr finished SyncMeta")
	time.Sleep(time.Second)
	err = mr.Close()
	t.Log(">>>>>>>>>>>>>>>>mr Closed")

	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	mmr, err = NewReader(meta, c)
	assert.Nil(t, err)
	mr = mmr.(*Reader)
	assert.NoError(t, mr.Start())
	defer mr.Close()

	time.Sleep(500 * time.Millisecond)
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 9 || spacenum > 20 {
			break
		}
	}
	t.Log("mr Started again", maxnum)
	time.Sleep(20 * time.Second)

	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(">>>> alive reader", ar.originpath)
	}
	mr.armapmux.Unlock()

	t.Log("mr listen 2 round")
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 12 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finish listen 2 round")
	assert.EqualValues(t, expresult, resultmap)
}

func multiReaderSyncMetaMutilineTest(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderSyncMetaMutilineTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir1file2 := filepath.Join(dir1, "file2.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	createFileWithContent(dir1file2, "abc456\n789\nabc012\n111\n")
	expresult := map[string]int{
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
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"expire":          "15s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(15 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 5 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finished read one")
	err = mr.Close()
	t.Log(">>>>>>>>>>>>>>>>mr Closed")
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	mmr, err = NewReader(meta, c)
	assert.Nil(t, err)
	mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	mr = mmr.(*Reader)
	assert.NoError(t, mr.Start())
	defer mr.Close()
	time.Sleep(100 * time.Millisecond)
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 7 || spacenum > 20 {
			break
		}
	}
	t.Log("mr Started again")
	time.Sleep(20 * time.Second)
	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(">>>> alive reader", ar.originpath)
	}
	mr.armapmux.Unlock()
	t.Log("mr listen 2 round")
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 10 || spacenum > 20 {
			break
		}
	}
	t.Log("mr finish listen 2 round")
	assert.EqualValues(t, expresult, resultmap)
}

func multiReaderNewestTest(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderNewestTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expresult := map[string]int{
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "newest",
		"expire":          "15s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(15 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if spacenum > 5 {
			break
		}
	}
	assert.EqualValues(t, 0, maxnum)

	t.Log("mr finished read one")
	err = mr.Close()
	t.Log(">>>>>>>>>>>>>>>>mr Closed")
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	mmr, err = NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	mr = mmr.(*Reader)
	assert.NoError(t, mr.Start())
	defer mr.Close()
	t.Log("mr Started again")
	time.Sleep(20 * time.Second)
	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(">>>> alive reader", ar.originpath)
	}
	mr.armapmux.Unlock()
	spacenum = 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 3 || spacenum > 10 {
			break
		}
	}
	t.Log("mr finish listen")
	assert.EqualValues(t, expresult, resultmap)
}

func multiReaderNewestOffsetTest(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderNewestOffsetTest"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expresult := map[string]int{
		"abc\nx\n": 1,
		"abc\ny\n": 1,
		"abc\nz\n": 1,
		"abc\na\n": 1,
		"abc\nb\n": 1,
		"abc\nc\n": 1,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "newest",
		"expire":          "15s",
		"stat_interval":   "1s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(15 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
		appendFileWithContent(dir1file1, "abc\na\nabc\nb\nabc\nc\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if spacenum > 5 {
			break
		}
	}
	assert.EqualValues(t, 0, maxnum)

	t.Log("mr finished read one")
	err = mr.Close()
	t.Log(">>>>>>>>>>>>>>>>mr Closed")
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	mmr, err = NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	mr = mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("mr Started again")
	defer mr.Close()
	time.Sleep(20 * time.Second)
	mr.armapmux.Lock()
	assert.Equal(t, 2, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(">>>> alive reader", ar.originpath)
	}
	mr.armapmux.Unlock()
	spacenum = 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if err == io.EOF {
			break
		}
		if maxnum >= 6 || spacenum > 10 {
			break
		}
	}
	t.Log("mr finish listen")
	assert.EqualValues(t, expresult, resultmap)
}

func multiReaderNewestModify(t *testing.T) {
	maxnum := 0
	dirname := "multiReaderNewestModify"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expresult := map[string]int{
		"abc\nx\n": 1,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "newest",
		"expire":          "15s",
		"stat_interval":   "5s",
		"max_open_files":  "128",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(2 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if len(resultmap) > 0 {
			break
		}
	}
	assert.EqualValues(t, 1, maxnum)

	t.Log("mr finished")
	err = mr.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, expresult, resultmap)
}

func multiReaderOutRunTime(t *testing.T) {
	runTime := strconv.Itoa(time.Now().Hour() + 2)
	maxnum := 0
	dirname := "multiReaderOutRunTime"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "newest",
		"expire":          "15s",
		"stat_interval":   "5s",
		"max_open_files":  "128",
		"run_time":        runTime,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	runTimeReader, ok := mmr.(reader.RunTimeReader)
	assert.True(t, ok)
	err = runTimeReader.SetRunTime(ReadModeRunTimeString, runTime+"-")
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(2 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		assert.Nil(t, err)
		if spacenum > 30 {
			break
		}
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
			time.Sleep(time.Second)
		}
		if len(resultmap) > 0 {
			break
		}
	}
	assert.EqualValues(t, 0, maxnum)

	t.Log("mr finished")
	err = mr.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, 0, len(resultmap))
}

func multiReaderInRunTime(t *testing.T) {
	if time.Now().Second() < 10 {
		time.Sleep(15 * time.Second)
	}
	runTime := strconv.Itoa(time.Now().Hour())
	maxnum := 0
	dirname := "multiReaderInRunTime"
	dir1 := filepath.Join(dirname, "abc")
	dir2 := filepath.Join(dirname, "xyz")
	dir1file1 := filepath.Join(dir1, "file1.log")
	dir2file1 := filepath.Join(dir2, "file1.log")

	createDirWithName(dirname)
	defer os.RemoveAll(dirname)

	createDirWithName(dir1)
	createDirWithName(dir2)
	createFileWithContent(dir1file1, "abc123\nabc124\nabc125\nabc126\nabc127\n")
	expresult := map[string]int{
		"abc\nx\n": 1,
	}
	resultmap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirname, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       dirname,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "newest",
		"expire":          "15s",
		"stat_interval":   "5s",
		"max_open_files":  "128",
		"run_time":        runTime,
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	err = mmr.SetMode(ReadModeHeadPatternString, "^abc*")
	assert.Nil(t, err)
	runTimeReader, ok := mmr.(reader.RunTimeReader)
	assert.True(t, ok)
	err = runTimeReader.SetRunTime(ReadModeRunTimeString, runTime+"-")
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 15*time.Second, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	go func() {
		time.Sleep(2 * time.Second)
		createFileWithContent(dir2file1, "abc\nx\nabc\ny\nabc\nz\n")
	}()
	spacenum := 0
	for {
		data, err := mr.ReadLine()
		assert.Nil(t, err)
		if data != "" {
			resultmap[data]++
			maxnum++
			t.Log(data, maxnum)
		} else {
			spacenum++
		}
		if len(resultmap) > 0 {
			break
		}
	}
	assert.EqualValues(t, 1, maxnum)

	t.Log("mr finished")
	err = mr.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, expresult, resultmap)
}

func TestMultiReaderReset(t *testing.T) {
	t.Parallel()
	dirName := "TestMultiReaderReset"
	dir := filepath.Join(dirName, "abc")
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
	expResult := map[string]int{
		"abc111\n": 1,
		"abc112\n": 1,
		"abc121\n": 1,
		"abc122\n": 1,
		"abc131\n": 1,
		"abc132\n": 1,
		"abc141\n": 1,
		"abc142\n": 1,
	}
	resultMap := make(map[string]int)
	logPathPattern := filepath.Join(filepath.Join(dirName, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       metaDir,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	assert.NoError(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("Reader has started")

	assert.Equal(t, 24*time.Hour, mr.expire)
	assert.Equal(t, 720*time.Hour, mr.submetaExpire)

	maxNum := 0
	spaceNum := 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultMap[data]++
			maxNum++
			t.Log(data, maxNum)
		} else {
			spaceNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 8 || spaceNum > 20 {
			break
		}
	}
	t.Log("mr finished read one")
	err = mr.Close()
	assert.NoError(t, err)
	t.Log(">>>>>>>>>>>>>>>>mr Closed")
	assert.EqualValues(t, expResult, resultMap)

	time.Sleep(500 * time.Millisecond)

	// 重置
	err = mr.Reset()
	assert.NoError(t, err)
	mmr, err = NewReader(meta, c)
	assert.Nil(t, err)
	mr = mmr.(*Reader)
	assert.NoError(t, mr.Start())
	t.Log("mr Started again")
	defer mr.Close()
	time.Sleep(100 * time.Millisecond)
	resultMap = make(map[string]int)
	maxNum = 0
	spaceNum = 0
	for {
		data, err := mr.ReadLine()
		if data != "" {
			resultMap[data]++
			maxNum++
			t.Log(data, maxNum)
		} else {
			spaceNum++
		}
		if err == io.EOF {
			break
		}
		if maxNum >= 8 || spaceNum > 20 {
			break
		}
	}

	assert.EqualValues(t, expResult, resultMap)
}

func TestReaderErrBegin(t *testing.T) {
	t.Parallel()
	dirName := "TestReaderErrBegin"
	dir := filepath.Join(dirName, "abc")
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
	file.WriteString("abc111\nabc112\n")
	file.Close()

	logPathPattern := filepath.Join(filepath.Join(dirName, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       metaDir,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"runner_name":     "LogkitInternalCollectLogRunnerTest",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	maxNum := 0
	for {
		_, err = mr.ReadLine()
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
		t.Error("no matched error")
	}
	err = mr.Close()
	assert.Nil(t, err)
}

func TestReaderErrMiddle(t *testing.T) {
	t.Parallel()
	dirName := "TestReaderErrMiddle"
	os.RemoveAll(dirName)
	dir := filepath.Join(dirName, "abc")
	metaDir := filepath.Join(dirName, "meta")
	file1 := filepath.Join(dir, "file1.log")
	file1rename := filepath.Join(dir, "file1.xlog")

	createDirWithName(dirName)
	defer os.RemoveAll(dirName)

	createDirWithName(dir)
	createFileWithContent(file1, "abc111\nabc112\n")

	go func() {
		time.Sleep(3 * time.Second)
		os.Rename(file1, file1rename)
		file, err := os.OpenFile(file1, os.O_CREATE|os.O_WRONLY, 0200)
		if err != nil {
			log.Error(err)
			return
		}
		file.WriteString("abc111\nabc112\n")
		file.Close()
	}()

	logPathPattern := filepath.Join(filepath.Join(dirName, "*"), "*.log")
	c := conf.MapConf{
		"log_path":        logPathPattern,
		"meta_path":       metaDir,
		"mode":            ModeTailx,
		"sync_every":      "1",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"stat_interval":   "50ms",
	}
	meta, err := reader.NewMetaWithConf(c)
	assert.NoError(t, err)
	mmr, err := NewReader(meta, c)
	assert.Nil(t, err)
	mr := mmr.(*Reader)
	assert.NoError(t, mr.Start())
	maxNum := 0
	for {
		_, err = mr.ReadLine()
		if err != nil {
			break
		}
		maxNum++
		if err == io.EOF {
			break
		}
		time.Sleep(time.Second)
		if maxNum >= 5 {
			break
		}
	}
	if err == nil || !strings.Contains(err.Error(), os.ErrPermission.Error()) {
		t.Errorf("no matched error %v, expect %v", err, os.ErrPermission)
	}
	err = mr.Close()
	assert.Nil(t, err)
}
