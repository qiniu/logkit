package utils

import (
	"os"

	"github.com/qiniu/logkit/utils/models"
)

// IsExist checks whether a file or directory exists.
// It returns false when the file or directory does not exist.
func IsExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func GetTestData(line string) []string {
	testSlice := make([]string, 0)
	totalSize := 0
	for {
		if totalSize > models.DefaultMaxBatchSize {
			return testSlice
		}
		testSlice = append(testSlice, line)
		totalSize += len(line)
	}
}
