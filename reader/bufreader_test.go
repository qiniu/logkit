package reader

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"

	"github.com/stretchr/testify/assert"
)

var lines = "123456789\n123456789\n123456789\n123456789\n"

func createSeqFile(interval int, lines string) {
	err := os.Mkdir(dir, 0755)
	if err != nil {
		log.Error(err)
		return
	}
	for _, f := range files {
		file, err := os.OpenFile(filepath.Join(dir, f), os.O_CREATE|os.O_WRONLY, defaultFilePerm)
		if err != nil {
			log.Error(err)
			return
		}

		file.WriteString(lines)
		file.Close()
		time.Sleep(time.Millisecond * time.Duration(interval))
	}
}

func destroySeqFile() {
	os.RemoveAll(dir)
	os.RemoveAll(metaDir)
}

func Test_BuffReader(t *testing.T) {
	createSeqFile(1000, lines)
	defer destroySeqFile()
	c := conf.MapConf{
		"log_path":        dir,
		"meta_path":       metaDir,
		"mode":            DirMode,
		"sync_every":      "1",
		"ignore_hidden":   "true",
		"reader_buf_size": "24",
		"read_from":       "oldest",
	}
	isFromWeb := false
	r, err := NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	rest := []string{}
	for {
		line, err := r.ReadLine()
		if err == nil {
			rest = append(rest, line)
		} else {
			break
		}
	}
	if len(rest) != 12 {
		t.Errorf("rest should be 12, but got %v", len(rest))
	}
	r.Close()
}

func Test_BuffReaderBufSizeLarge(t *testing.T) {
	createSeqFile(1000, lines)
	defer destroySeqFile()
	c := conf.MapConf{
		"log_path":        dir,
		"meta_path":       metaDir,
		"mode":            DirMode,
		"sync_every":      "1",
		"ignore_hidden":   "true",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
	}
	isFromWeb := false
	r, err := NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	rest := []string{}
	for {
		line, err := r.ReadLine()
		if err == nil {
			rest = append(rest, line)
		} else {
			break
		}
	}
	if len(rest) != 12 {
		t.Errorf("rest should be 12, but got %v", len(rest))
	}
	r.Close()
}

func Test_GBKEncoding(t *testing.T) {
	body := "\x82\x31\x89\x38"
	createSeqFile(1000, body)
	exp := "㧯"
	defer destroySeqFile()
	c := conf.MapConf{
		"log_path":        dir,
		"meta_path":       metaDir,
		"mode":            DirMode,
		"sync_every":      "1",
		"ignore_hidden":   "true",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"encoding":        "gb18030",
	}
	isFromWeb := false
	r, err := NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	rest := []string{}
	for {
		line, err := r.ReadLine()
		if err == nil {
			rest = append(rest, line)
		} else {
			break
		}
		if line != exp {
			t.Fatalf("should exp %v but got %v", exp, line)
		}
	}
	r.Close()
}

func Test_NoPanicEncoding(t *testing.T) {
	body := "123123"
	createSeqFile(1000, body)
	exp := "123123"
	defer destroySeqFile()
	c := conf.MapConf{
		"log_path":        dir,
		"meta_path":       metaDir,
		"mode":            DirMode,
		"sync_every":      "1",
		"ignore_hidden":   "true",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"encoding":        "nopanic",
	}
	isFromWeb := false
	r, err := NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	rest := []string{}
	for {
		line, err := r.ReadLine()
		if err == nil {
			rest = append(rest, line)
		} else {
			break
		}
		if line != exp {
			t.Fatalf("should exp %v but got %v", exp, line)
		}
	}
	r.Close()
}

func Test_BuffReaderMultiLine(t *testing.T) {
	body := "test123\n12\n34\n56\ntest\nxtestx\n123\n"
	createSeqFile(1000, body)
	defer destroySeqFile()
	c := conf.MapConf{
		"log_path":        dir,
		"meta_path":       metaDir,
		"mode":            DirMode,
		"sync_every":      "1",
		"ignore_hidden":   "true",
		"reader_buf_size": "1024",
		"read_from":       "oldest",
		"head_pattern":    "^test*",
	}
	isFromWeb := false
	r, err := NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	rest := make(map[string]int)
	num := 0
	for {
		line, err := r.ReadLine()
		rest[line]++
		num++

		r.SyncMeta()
		if num > 3 {
			break
		}
		r.SyncMeta()
		assert.NoError(t, err)
	}
	r.Close()
	r, err = NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	for {
		line, err := r.ReadLine()
		rest[line]++
		num++

		r.SyncMeta()
		if num >= 6 {
			break
		}
		r.SyncMeta()
		assert.NoError(t, err)
	}
	exp := map[string]int{
		"test123\n12\n34\n56\n": 3,
		"test\nxtestx\n123\n":   3,
	}
	assert.Equal(t, exp, rest)
	r.Close()
}

func Test_BuffReaderStats(t *testing.T) {
	body := "Test_BuffReaderStats\n"
	createSeqFile(1000, body)
	defer destroySeqFile()
	c := conf.MapConf{
		"log_path":  dir,
		"meta_path": metaDir,
		"mode":      DirMode,
		"read_from": "oldest",
	}
	isFromWeb := false
	r, err := NewFileBufReader(c, isFromWeb)
	if err != nil {
		t.Error(err)
	}
	_, err = r.ReadLine()
	assert.NoError(t, err)
	str, ok := r.(StatsReader)
	assert.Equal(t, true, ok)
	stsx := str.Status()
	expsts := utils.StatsInfo{}
	assert.Equal(t, expsts, stsx)
	r.Close()
}

func Test_FileNotFound(t *testing.T) {
	createSeqFile(1000, lines)
	defer destroySeqFile()
	c := conf.MapConf{
		"mode":            ModeFile,
		"log_path":        "/home/users/john/log/my.log",
		"meta_path":       metaDir,
		"sync_every":      "1",
		"ignore_hidden":   "true",
		"reader_buf_size": "24",
		"read_from":       "oldest",
	}
	isFromWeb := true
	r, err := NewFileBufReader(c, isFromWeb)
	assert.Error(t, err)

	c["log_path"] = filepath.Join(dir, files[0])
	r, err = NewFileBufReader(c, isFromWeb)
	assert.NoError(t, err)
	rest := []string{}
	for {
		line, err := r.ReadLine()
		if err == nil {
			rest = append(rest, line)
		} else {
			break
		}
	}
	if len(rest) != 4 {
		t.Errorf("rest should be 4, but got %v", len(rest))
	}
	r.Close()
}
