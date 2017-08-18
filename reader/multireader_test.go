package reader

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/log"
	"github.com/stretchr/testify/assert"
)

func createFileWithContent(filepathn, lines string) {
	file, err := os.OpenFile(filepathn, os.O_CREATE|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		log.Error(err)
		return
	}
	file.WriteString(lines)
	file.Close()
}

func createDirWithName(dirx string) {
	err := os.Mkdir(dirx, 0755)
	if err != nil {
		log.Error(err)
		return
	}
}
func Test_ActiveReader(t *testing.T) {
	testfile := "Test_ActiveReader"
	createDir()
	meta, err := NewMeta(metaDir, metaDir, testfile, ModeDir, defautFileRetention)
	if err != nil {
		t.Error(err)
	}
	defer destroyFile()
	ppath := filepath.Join(dir, testfile)
	testContent := "1234567812345678"
	createTestFile(ppath, testContent)
	ppath, err = filepath.Abs(ppath)
	assert.NoError(t, err)
	msgchan := make(chan Result)
	ar, err := NewActiveReader(ppath, WhenceOldest, meta, msgchan)
	assert.NoError(t, err)
	go ar.Run()
	data := <-msgchan
	assert.Equal(t, testContent, data.result)
	ar.Close()
}

func TestMultiReaderOneLine(t *testing.T) {
	maxnum := 0
	dirname := "TestMultiReaderOneLine"
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
	}
	meta, err := NewMetaWithConf(c)
	assert.NoError(t, err)
	mr, err := NewMultiReader(meta, logPathPattern, WhenceOldest, "15s", "1s", 128)
	mr.Start()
	t.Log("mr started")
	go func() {
		time.Sleep(15 * time.Second)
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
		if maxnum >= 15 || spacenum > 100 {
			break
		}
		t.Log(data)
	}
	time.Sleep(26 * time.Second)
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
		if maxnum >= 18 || spacenum > 100 {
			break
		}
	}
	t.Log("mr finish listen 2 round")

	assert.EqualValues(t, expresult, resultmap)
}

func TestMultiReaderMultiLine(t *testing.T) {
	maxnum := 0
	dirname := "TestMultiReaderMultiLine"
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
	createFileWithContent(dir1file2, "abc456\n789\nabc456\n789\n")
	expresult := map[string]int{
		"abc123\n":      5,
		"abc456\n789\n": 2,
		"abc\nx\n":      3,
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
	}
	meta, err := NewMetaWithConf(c)
	assert.NoError(t, err)
	mr, err := NewMultiReader(meta, logPathPattern, WhenceOldest, "15s", "1s", 128)
	mr.SetMode(ReadModeHeadPatternString, "^abc*")
	mr.Start()
	t.Log("mr started")
	go func() {
		time.Sleep(15 * time.Second)
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
		if maxnum >= 7 || spacenum > 100 {
			break
		}
		t.Log(data)
	}
	time.Sleep(26 * time.Second)
	t.Log("mr finished read one")
	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(ar.logpath)
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
		if maxnum >= 10 || spacenum > 100 {
			break
		}
	}
	t.Log("mr finish listen 2 round")

	assert.EqualValues(t, expresult, resultmap)
}

func TestMultiReaderSyncMetaOneLine(t *testing.T) {
	maxnum := 0
	dirname := "TestMultiReaderSyncMetaOneLine"
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
	}
	meta, err := NewMetaWithConf(c)
	assert.NoError(t, err)
	mr, err := NewMultiReader(meta, logPathPattern, WhenceOldest, "15s", "1s", 128)
	mr.Start()
	t.Log("mr started")
	go func() {
		time.Sleep(15 * time.Second)
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
		if maxnum >= 2 || spacenum > 100 {
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
	mr, err = NewMultiReader(meta, logPathPattern, WhenceOldest, "15s", "1s", 128)
	mr.Start()
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
		if maxnum >= 9 || spacenum > 100 {
			break
		}
	}
	t.Log("mr Started again", maxnum)
	time.Sleep(22 * time.Second)

	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(">>>> alive reader", ar.logpath)
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
		if maxnum >= 12 || spacenum > 100 {
			break
		}
	}
	t.Log("mr finish listen 2 round")
	assert.EqualValues(t, expresult, resultmap)
}

func TestMultiReaderSyncMetaMutiline(t *testing.T) {
	maxnum := 0
	dirname := "TestMultiReaderSyncMetaMutiline"
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
	}
	meta, err := NewMetaWithConf(c)
	assert.NoError(t, err)
	mr, err := NewMultiReader(meta, logPathPattern, WhenceOldest, "15s", "1s", 128)
	mr.SetMode(ReadModeHeadPatternString, "^abc*")
	mr.Start()
	t.Log("mr started")
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
		if maxnum >= 5 || spacenum > 100 {
			break
		}
	}
	t.Log("mr finished read one")
	err = mr.Close()
	t.Log(">>>>>>>>>>>>>>>>mr Closed")
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	mr, err = NewMultiReader(meta, logPathPattern, WhenceOldest, "15s", "1s", 128)
	mr.SetMode(ReadModeHeadPatternString, "^abc*")
	mr.Start()
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
		if maxnum >= 7 || spacenum > 100 {
			break
		}
	}
	t.Log("mr Started again")
	time.Sleep(20 * time.Second)
	mr.armapmux.Lock()
	assert.Equal(t, 1, len(mr.fileReaders), "activereader number")
	for _, ar := range mr.fileReaders {
		t.Log(">>>> alive reader", ar.logpath)
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
		if maxnum >= 10 || spacenum > 100 {
			break
		}
	}
	t.Log("mr finish listen 2 round")
	assert.EqualValues(t, expresult, resultmap)
}
