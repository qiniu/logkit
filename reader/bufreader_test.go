package reader

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/log"
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
	r, err := NewFileBufReader(c)
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
	r, err := NewFileBufReader(c)
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
	r, err := NewFileBufReader(c)
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
