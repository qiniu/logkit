package aws_s3

import (
	"fmt"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// todo
// 1. 时间间隔 done 待测试
// 2. 删除文件 done 待测试
// 3. 包含文件目录 done 待测试
// 4. 压缩 done 待测试
// 5. writer 待测试
func TestSender_Send(t *testing.T) {
	mc := conf.MapConf{
		KeyName:        "s3",
		KeyS3Region:    "ap-northeast-1",
		KeyS3Endpoint:  "",
		KeyS3Bucket: "",
		KeyS3AccessKey: "",
		KeyS3SecretKey: "",

		KeyS3MaxLines:     "1",
		KeyS3MaxSize:      "10",
		KeyS3SendInternal: "30",
		KeyS3Compress:"",
		KeyS3CompressType:"",
		KeyFileWriteRaw:   "true",

		KeyS3Pattern: "test_logkit_pattern-%Y-%m-%d-%H-%S.log",
	}
	sender, err := NewSender(mc)
	assert.NoError(t, err)
	datas := make([]models.Data, 0)
	datas = append(datas, models.Data{
		"test1": "11111",
		"raw":"rwassss1",
	})
	datas = append(datas, models.Data{
		"test2": "11122",
		"raw":"rwassss2",
	})
	datas = append(datas, models.Data{
		"test3": "111333",
		"raw":"rwassss3",
	})
	err = sender.Send(datas)
	fmt.Println("err: ", err)
	assert.NoError(t, err)
}

func TestSender_ss(t *testing.T) {
	dir := "./s3data"
	if !utils.IsExist(dir) {
		realDir, err := filepath.Abs(dir)
		if err != nil {
			return
		}
		fmt.Println("realDir: ", realDir)
		if err := os.Mkdir(realDir, models.DefaultDirPerm); err != nil {
			fmt.Println("err: ", err)
		}
	}
}