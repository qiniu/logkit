package aws_s3

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/json-iterator/go"
	"github.com/lestrrat-go/strftime"
	"github.com/mitchellh/goamz/aws"

	"github.com/qbox/phoenix/go/agent/utils/models"
	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	defaultMaxRows      = 10000
	defaultMaxSize      = 500 * models.MB
	defaultSendInternal = 60

	defaultDir = "./s3_send_data"
)

type Sender struct {
	name      string
	region    string
	endpoint  string
	bucket    string
	objectKey string
	accessKey string
	secretKey string

	maxRows  uint64 // 文件最大行
	maxSize  uint64 // 文件最大大小，单位B
	compress bool

	internal uint64 //  发送间隔，单位秒

	marshalFunc func([]Data) ([]byte, error)
	pattern     *strftime.Strftime

	*senderStatus // 发送状态

	timer chan struct{} // 更新timer
	stopChan  chan struct{} // close

	ste *StatsError
}

type senderStatus struct {
	filename    string
	tmpRows     uint64
	tmpSize     uint64
	isListening int32
}

func init() {
	sender.RegisterConstructor(TypeS3, NewSender)
}

func NewSender(conf conf.MapConf) (sender.Sender, error) {
	name, err := conf.GetString(KeyName)
	if err != nil {
		return nil, err
	}
	region, err := conf.GetString(KeyS3Region)
	if err != nil {
		return nil, err
	}
	endpoint, err := conf.GetString(KeyS3Endpoint)
	if err != nil {
		return nil, err
	}
	bucket, err := conf.GetString(KeyS3Bucket)
	if err != nil {
		return nil, err
	}
	accessKey, err := conf.GetPasswordEnvString(KeyS3AccessKey)
	if err != nil {
		return nil, err
	}
	secretKey, err := conf.GetPasswordEnvString(KeyS3SecretKey)
	if err != nil {
		return nil, err
	}
	pattern, err := conf.GetString(KeyS3Pattern)
	if err != nil {
		return nil, err
	}
	maxRows, _ := conf.GetInt64Or(KeyS3MaxLines, defaultMaxRows)
	maxSize, _ := conf.GetInt64Or(KeyS3MaxSize, defaultMaxSize)
	internal, _ := conf.GetInt32Or(KeyS3SendInternal, defaultSendInternal)
	rawMarshal, _ := conf.GetBoolOr(KeyFileWriteRaw, false)
	compress, _ := conf.GetBoolOr(KeyS3Compress, false)

	marshal := jsonMarshalWithNewLineFunc
	if rawMarshal {
		marshal = writeRawFunc
	}
	p, err := strftime.New(pattern)
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(defaultDir, 0755); err != nil && err != os.ErrExist {
		return nil, err
	}

	return &Sender{
		name:        name,
		region:      region,
		endpoint:    endpoint,
		bucket:      bucket,
		accessKey:   accessKey,
		secretKey:   secretKey,
		maxRows:     uint64(maxRows),
		maxSize:     uint64(maxSize),
		compress:    compress,
		internal:    uint64(internal),
		marshalFunc: marshal,
		pattern:     p,

		timer: make(chan struct{}, 1),
		stopChan:  make(chan struct{}, 1),

		senderStatus: &senderStatus{
			filename: "",
			tmpRows:  0,
			tmpSize:  0,
		},
		ste: &StatsError{
			Ft:         true,
			FtNotRetry: true,
		},
	}, nil
}

func (s *Sender) Name() string {
	return s.name
}

func (*Sender) SkipDeepCopy() bool { return true }

func (s *Sender) Send(datas []Data) error {

	if atomic.LoadInt32(&s.isListening) == config.StatusRunning {
		s.timer <- struct{}{}
	}
	if s.filename == "" {
		s.filename = strings.Replace(s.pattern.FormatString(time.Now()), string(filepath.Separator), "-", -1)
		fmt.Println("s.objectKey: ", s.objectKey)
		fmt.Println("s.filename: ", s.filename)
	}

	batchDatas := make([]Data, 0, len(datas))
	for i := 0; i < len(datas); i++ {
		s.tmpSize += uint64(len(datas[i]))
		s.tmpRows++
		batchDatas = append(batchDatas, datas[i])
		if s.meetSendConditions() {
			err := s.writeFile(batchDatas)
			if err != nil {
				s.ste.LastError = fmt.Sprintf("%s write data to file %s failed: %v", s.Name(), s.filename, err)
				continue
			}
			batchDatas = batchDatas[:0]
			err = s.sendFileToBucket()
			if err != nil {
				s.ste.LastError = fmt.Sprintf("%s send file %s to bucket %s failed: %v", s.Name(), s.filename, s.bucket, err)
				continue
			}
			s.resetStatus()
		}
	}
	if len(batchDatas) > 0 {
		err := s.writeFile(batchDatas)
		if err != nil {
			s.ste.LastError = fmt.Sprintf("%s write data to file %s failed: %v", s.Name(), s.filename, err)
		}
	}

	if atomic.CompareAndSwapInt32(&s.isListening, config.StatusStopped, config.StatusRunning) {
		go s.waitLastFile()
	}

	if s.ste.Errors > 0 {
		return s.ste
	}
	return nil
}

func (s *Sender) waitLastFile() {
	timer := time.NewTicker(time.Duration(s.internal))
	defer timer.Stop()
	for {
		if atomic.LoadInt32(&s.isListening) == config.StatusStopped {
			return
		}
		select {
		case <-s.timer:
			timer = time.NewTicker(time.Duration(s.internal))
		case <- s.stopChan:
			s.stop()
			return
		case <-timer.C:
			s.stop()
			return
		}
	}
}

func (s *Sender) stop() {
	atomic.StoreInt32(&s.isListening, config.StatusStopped)
	if s.tmpRows != 0 {
		err := s.sendFileToBucket()
		s.ste.LastError = fmt.Sprintf("%s send file %s to bucket %s failed: %v", s.Name(), s.filename, s.bucket, err)
		s.resetStatus()
	}
}

func (s *Sender) writeFile(datas []Data) error {
	datasBytes, err := s.marshalFunc(datas)
	if err != nil {
		log.Errorf("marshal datas bytes from marshalFunc failed: %v, datas length: %d", err, len(datas))
		return err
	}
	return ioutil.WriteFile(s.filename, datasBytes, os.FileMode(0644))
	/*_, err = s.writer.Write(s.filename, datasBytes)
	if err != nil {
		log.Errorf("write to file[%s] failed: %v, datas length: %d", s.filename, err, len(datas))
		time.Sleep(10 * time.Second)
		return err
	}
	return nil*/
}

func (s *Sender) Close() error {
	s.stopChan <- struct{}{}
	return nil
}

// jsonMarshalWithNewLineFunc 将数据序列化为 JSON 并且在末尾追加换行符
func jsonMarshalWithNewLineFunc(datas []Data) ([]byte, error) {
	bytes, err := jsoniter.Marshal(datas)
	if err != nil {
		return nil, err
	}
	return append(bytes, '\n'), nil
}

func writeRawFunc(datas []Data) ([]byte, error) {
	var buf bytes.Buffer
	for _, d := range datas {
		raw := d["raw"]
		switch bts := raw.(type) {
		case string:
			buf.Write([]byte(bts))
			if !strings.HasSuffix(bts, "\n") {
				buf.Write([]byte("\n"))
			}
		case []byte:
			buf.Write(bts)
			if !strings.HasSuffix(string(bts), "\n") {
				buf.Write([]byte("\n"))
			}
		}
	}
	return buf.Bytes(), nil
}

func (s *Sender) meetSendConditions() bool {
	if s.tmpRows >= s.maxRows || s.tmpSize >= s.maxSize {
		return true
	}
	return false
}

func (s *Sender) resetStatus() {
	s.senderStatus = &senderStatus{
		filename: strings.Replace(s.pattern.FormatString(time.Now()), string(filepath.Separator), "-", -1),
		tmpRows:  0,
		tmpSize:  0,
	}
}

func (s *Sender) sendFileToBucket() error {
	rg := aws.Regions[s.region]
	if s.endpoint == "" {
		s.endpoint = rg.S3Endpoint
	}

	var awsConfig *awssdk.Config
	if s.accessKey == "" || s.secretKey == "" {
		awsConfig = &awssdk.Config{
			Region:   awssdk.String(s.region),
			Endpoint: awssdk.String(s.endpoint),
		}
	} else {
		awsConfig = &awssdk.Config{
			Region:      awssdk.String(s.region),
			Endpoint:    awssdk.String(s.endpoint),
			Credentials: credentials.NewStaticCredentials(s.accessKey, s.secretKey, ""),
		}
	}

	sess := session.Must(session.NewSession(awsConfig))
	uploader := s3manager.NewUploader(sess)
	if s.compress {
		compressFilenam := s.filename + ".zip"
		err := utils.CompressFile(s.filename, compressFilenam)
		if err != nil {
			return fmt.Errorf("failed to compress file %q, dst file %q, %v", s.filename, compressFilenam, err)
		}
		s.filename = compressFilenam
	}

	f, err := os.Open(s.filename)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", s.filename, err)
	}
	defer f.Close()

	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: awssdk.String(s.bucket),
		Key:    awssdk.String(s.pattern.FormatString(time.Now())),
		Body:   f,
	})
	if err != nil {
		return err
	}
	log.Infof("send to s3 bucket success, file uploaded to %s", result.Location)
	_ = s.Close()
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.filename); err != nil {
		return fmt.Errorf("delete local s3 file %s failed, %v", s.filename, err)
	}
	return nil
}
