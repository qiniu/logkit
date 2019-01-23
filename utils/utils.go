package utils

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/utils/models"
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
