package sql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/reader"

	"github.com/qiniu/logkit/utils/models"
)

func RestoreMeta(meta *reader.Meta, rawSqls string, magicLagDur time.Duration) (offsets []int64, sqls []string, omitMeta bool) {
	now := time.Now().Add(-magicLagDur)
	sqls = UpdateSqls(rawSqls, now)
	omitMeta = true
	sqlAndOffsets, length, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
		return
	}
	tmps := strings.Split(sqlAndOffsets, SqlOffsetConnector)
	if int64(len(tmps)) != 2*length || int64(len(sqls)) != length {
		log.Errorf("Runner[%v] %v -meta file is not invalid sql meta file %v， omit meta data", meta.RunnerName, meta.MetaFile(), sqlAndOffsets)
		return
	}
	omitMeta = false
	offsets = make([]int64, length)
	for idx, sql := range sqls {
		syncSQL := strings.Replace(tmps[idx], "@", " ", -1)
		offset, err := strconv.ParseInt(tmps[idx+int(length)], 10, 64)
		if err != nil || sql != syncSQL {
			log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse offset err %v， omit this offset", meta.RunnerName, meta.MetaFile(), syncSQL, err)
		}
		offsets[idx] = offset
	}
	return
}

func RestoreTimestampIntOffset(doneFilePath string) (int64, map[string]string, error) {
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	cachemapfilename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)

	filePath := filepath.Join(doneFilePath, filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return 0, nil, err
	}
	tm, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return tm, nil, err
	}

	cacheMapFilePath := filepath.Join(doneFilePath, cachemapfilename)
	data, err = ioutil.ReadFile(cacheMapFilePath)
	if err != nil {
		return tm, nil, err
	}
	cache := make(map[string]string)
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return tm, nil, err
	}
	return tm, cache, nil
}

func RestoreTimestampOffset(doneFilePath string) (time.Time, map[string]string, error) {
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	cachemapfilename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)

	filePath := filepath.Join(doneFilePath, filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return time.Time{}, nil, err
	}

	tm, err := time.Parse(time.RFC3339Nano, string(data))
	if err != nil {
		return tm, nil, err
	}

	cacheMapFilePath := filepath.Join(doneFilePath, cachemapfilename)
	data, err = ioutil.ReadFile(cacheMapFilePath)
	if err != nil {
		return tm, nil, err
	}
	cache := make(map[string]string)
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return tm, nil, err
	}
	return tm, cache, nil
}

func RestoreTimestampStrOffset(doneFilePath string) (string, map[string]string, error) {
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	cachemapfilename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)

	filePath := filepath.Join(doneFilePath, filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", nil, err
	}

	tmStr := string(data)
	cacheMapFilePath := filepath.Join(doneFilePath, cachemapfilename)
	data, err = ioutil.ReadFile(cacheMapFilePath)
	if err != nil {
		return "", nil, err
	}
	cache := make(map[string]string)
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return "", nil, err
	}
	return tmStr, cache, nil
}

func WriteCacheMap(doneFilePath string, cache map[string]string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return f.Sync()
}

func WriteTimestmapOffset(doneFilePath, content string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}

	return f.Sync()
}

// WriteRecordsFile 将当前文件写入donefiel中
func WriteRecordsFile(doneFilePath, content string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, DefaultDoneRecordsFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}

	return f.Sync()
}
