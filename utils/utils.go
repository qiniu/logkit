package utils

import (
	"archive/zip"
	"bytes"
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(models.Data{})

}

var JSONTool = jsoniter.Config{UseNumber: true}.Froze()

// IsExist checks whether a file or directory exists.
// It returns false when the file or directory does not exist.
func IsExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

// 获取测试数据
func GetParseTestData(line string, size int) []string {
	testSlice := make([]string, 0)
	totalSize := 0
	for {
		if totalSize > size {
			return testSlice
		}
		testSlice = append(testSlice, line)
		totalSize += len(line)
	}
}

func DeepCopyByJSON(dst, src interface{}) {
	confBytes, err := JSONTool.Marshal(src)
	if err != nil {
		log.Errorf("DeepCopyByJSON marshal error %v, use same pointer", err)
		dst = src
		return
	}
	if err = JSONTool.Unmarshal(confBytes, dst); err != nil {
		log.Errorf("DeepCopyByJSON unmarshal error %v, use same pointer", err)
		dst = src
		return
	}
}

func DeepCopyByGob(dst, src interface{}) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	dec := gob.NewDecoder(&network)

	err := enc.Encode(src)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	err = dec.Decode(dst)
	if err != nil {
		log.Fatal("decode error:", err)
	}
}

func BatchFullOrTimeout(runnerName string, stopped *int32, batchLen, batchSize int64, lastSend time.Time,
	maxBatchLen, maxBatchSize, maxBatchInterval int) bool {
	// 达到最大行数
	if maxBatchLen > 0 && int(batchLen) >= maxBatchLen {
		log.Debugf("Runner[%v] meet the max batch length %v", runnerName, maxBatchLen)
		return true
	}
	// 达到最大字节数
	if maxBatchSize > 0 && int(batchSize) >= maxBatchSize {
		log.Debugf("Runner[%v] meet the max batch size %v", runnerName, maxBatchSize)
		return true
	}
	// 超过最长的发送间隔
	if time.Now().Sub(lastSend).Seconds() >= float64(maxBatchInterval) {
		log.Debugf("Runner[%v] meet the max batch send interval %v", runnerName, maxBatchInterval)
		return true
	}
	// 如果任务已经停止
	if atomic.LoadInt32(stopped) > 0 {
		if !models.IsSelfRunner(runnerName) {
			log.Warnf("Runner[%v] meet the stopped signal", runnerName)
		} else {
			log.Debugf("Runner[%v] meet the stopped signal", runnerName)
		}
		return true
	}
	return false
}

func CheckNotExistFile(runnerName string, expireMap map[string]int64) {
	for inodePath := range expireMap {
		arr := strings.SplitN(inodePath, "_", 2)
		if len(arr) < 2 {
			log.Errorf("Runner[%s] expect inode_path, but got: %s", runnerName, inodePath)
			return
		}
		// 不存在时删除
		if !IsExist(arr[1]) {
			delete(expireMap, inodePath)
		}
	}
}

func UpdateExpireMap(runnerName string, fileMap map[string]string, expireMap map[string]int64) {
	if expireMap == nil {
		expireMap = make(map[string]int64)
	}
	for realPath, inode := range fileMap {
		f, errOpen := os.Open(realPath)
		if errOpen != nil {
			log.Errorf("Runner[%s] update expire map open file: %s offset failed: %v, ignore...", runnerName, realPath, errOpen)
			continue
		}

		offset, errSeek := f.Seek(0, io.SeekEnd)
		if errSeek != nil {
			log.Errorf("Runner[%s] update expire map get file: %s offset failed: %v, ignore...", runnerName, realPath, errSeek)
			f.Close()
			continue
		}
		expireMap[inode+"_"+realPath] = offset
		f.Close()
	}
}

//获取指定目录下的所有文件和对应inode
func GetFiles(runnerName, dirPath string) (map[string]string, error) {
	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var (
		files    = make(map[string]string)
		inodeStr string
	)
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}

		filePath := filepath.Join(dirPath, fi.Name())
		inode, err := utilsos.GetIdentifyIDByPath(filePath)
		if err != nil {
			log.Errorf("Runner[%s]update expire map get file: %s inode failed: %v, ignore...", runnerName, filePath, err)
		} else {
			inodeStr = strconv.FormatUint(inode, 10)
		}

		files[filePath] = inodeStr
	}

	return files, nil
}

//gzip 前两位是固定的： https://stackoverflow.com/questions/6059302/how-to-check-if-a-file-is-gzip-compressed
func IsGzipped(data []byte) bool {
	if data == nil || len(data) < 2 {
		return false
	}
	return data[0] == 31 && data[1] == 139
}

func WriteZipToFile(zipf *zip.File, filename string) error {
	srcF, err := zipf.Open()
	if err != nil {
		return err
	}
	defer srcF.Close()
	distF, err := os.OpenFile(filepath.Join(filepath.Dir(filename), zipf.Name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		return err
	}
	defer distF.Close()
	_, err = io.Copy(distF, srcF)
	if err != nil {
		return err
	}
	return nil
}
