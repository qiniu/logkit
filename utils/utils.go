package utils

import (
	"bytes"
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

	"compress/gzip"

	"archive/tar"
	"archive/zip"

	"fmt"

	"github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

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
	confBytes, err := jsoniter.Marshal(src)
	if err != nil {
		log.Errorf("DeepCopyByJSON marshal error %v, use same pointer", err)
		dst = src
		return
	}
	if err = jsoniter.Unmarshal(confBytes, dst); err != nil {
		log.Errorf("DeepCopyByJSON unmarshal error %v, use same pointer", err)
		dst = src
		return
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

func CheckAndUnCompress(realPath string) (string, error) {
	if strings.HasSuffix(realPath, ".tar.gz") {
		f, err := os.Open(realPath)
		if err != nil {
			return realPath, err
		}
		defer f.Close()

		gzf, err := gzip.NewReader(f)
		if err != nil {
			return realPath, err
		}

		tarReader := tar.NewReader(gzf)
		targetDir := filepath.Dir(realPath)
		realdir := targetDir
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				return realPath, err
			}

			path := filepath.Join(targetDir, header.Name)
			lists := strings.Split(header.Name, string(os.PathSeparator))
			if len(lists) >= 1 { //.tar.gz 允许解压出文件，而.tar只允许文件夹
				realdir = filepath.Join(targetDir, lists[0])
			}
			info := header.FileInfo()
			if info.IsDir() {
				if err = os.MkdirAll(path, info.Mode()); err != nil {
					return realPath, err
				}
				continue
			}

			file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
			if err != nil {
				return realPath, err
			}
			_, err = io.Copy(file, tarReader)
			if err != nil {
				file.Close()
				return realPath, err
			}
			file.Close()
		}
		return realdir, nil
	} else if strings.HasSuffix(realPath, ".tar") {
		file, err := os.Open(realPath)
		if err != nil {
			return realPath, err
		}
		defer file.Close()
		tarReader := tar.NewReader(file)
		targetDir := filepath.Dir(realPath)
		realdir := targetDir
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				return realPath, err
			}

			path := filepath.Join(targetDir, header.Name)
			lists := strings.Split(header.Name, string(os.PathSeparator))
			if len(lists) > 1 {
				realdir = filepath.Join(targetDir, lists[0])
			}
			info := header.FileInfo()
			if info.IsDir() {
				if err = os.MkdirAll(path, info.Mode()); err != nil {
					return realPath, err
				}
				continue
			}

			file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
			if err != nil {
				return realPath, err
			}
			_, err = io.Copy(file, tarReader)
			if err != nil {
				file.Close()
				return realPath, err
			}
			file.Close()
		}
		return realdir, nil
	} else if strings.HasSuffix(realPath, ".gz") || strings.HasSuffix(realPath, ".zip") {
		data, err := ioutil.ReadFile(realPath)
		if IsGzipped(data) {
			gzipData, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				log.Errorf("reader file %v as gzip error %v", realPath, err)
				return realPath, err
			}
			defer gzipData.Close()
			if gzipData.Name == "" {
				path := filepath.Base(realPath)
				gzipData.Name = strings.TrimSuffix(strings.TrimSuffix(path, ".gz"), ".tar.gz")
			}
			filename := filepath.Join(filepath.Dir(realPath), gzipData.Name)
			f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0644))
			if err != nil {
				log.Errorf("open file %s to write gzip data error %v", filename, err)
				return realPath, err
			}
			defer f.Close()
			_, err = io.Copy(f, gzipData)
			if err != nil {
				log.Errorf("io.Copy gzip data to file error %v", err)
				return realPath, err
			}
			return filename, nil
		}

		rd, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Errorf("reader file %v as zip error %v", realPath, err)
			return realPath, err
		}
		var writeErr error
		realfile := realPath
		for _, f := range rd.File {
			err = WriteZipToFile(f, f.Name)
			if err != nil {
				writeErr = fmt.Errorf("write to %v err %v; %v", f.Name, err, writeErr)
			} else {
				realfile = filepath.Join(realPath, f.Name)
			}
		}
		if writeErr != nil {
			return realPath, writeErr
		}
		return realfile, nil
	}
	return realPath, nil
}
