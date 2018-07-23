package file

import (
	"fmt"
	"time"

	"github.com/json-iterator/go"
	"github.com/lestrrat-go/strftime"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ sender.SkipDeepCopySender = &Sender{}
	_ sender.Sender             = &Sender{}
)

type Sender struct {
	name         string
	pattern      *strftime.Strftime
	timestampKey string
	marshalFunc  func([]Data) ([]byte, error)
	writers      *writerStore
}

func init() {
	sender.RegisterConstructor(sender.TypeFile, NewSender)
}

func newSender(name, pattern, timestampKey string, maxOpenFile int, marshalFunc func([]Data) ([]byte, error)) (*Sender, error) {
	p, err := strftime.New(pattern)
	if err != nil {
		return nil, err
	}

	// 如果没有指定 timestamp key 则表示同时只会写入一个文件，没必要维护更多的文件句柄
	if len(timestampKey) == 0 {
		maxOpenFile = 1
	}

	return &Sender{
		name:         name,
		pattern:      p,
		timestampKey: timestampKey,
		marshalFunc:  marshalFunc,
		writers:      newWriterStore(maxOpenFile),
	}, nil
}

// jsonMarshalWithNewLineFunc 将数据序列化为 JSON 并且在末尾追加换行符
func jsonMarshalWithNewLineFunc(datas []Data) ([]byte, error) {
	bytes, err := jsoniter.Marshal(datas)
	if err != nil {
		return nil, err
	}
	return append(bytes, '\n'), nil
}

func NewSender(conf conf.MapConf) (sender.Sender, error) {
	path, err := conf.GetString(sender.KeyFileSenderPath)
	if err != nil {
		return nil, err
	}
	name, _ := conf.GetStringOr(sender.KeyName, "fileSender:"+path)
	timestampKey, _ := conf.GetStringOr(sender.KeyFileSenderTimestampKey, "")
	maxOpenFile, _ := conf.GetIntOr(sender.KeyFileSenderMaxOpenFiles, defaultFileWriterPoolSize)
	s, err := newSender(name, path, timestampKey, maxOpenFile, jsonMarshalWithNewLineFunc)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Sender) Name() string {
	return s.name
}

func (_ *Sender) SkipDeepCopy() bool { return true }

func (s *Sender) Send(datas []Data) error {
	// 仅仅上报错误信息，但是日志会正常写出，所以不需要上层重试
	ste := &StatsError{
		Ft:         true,
		FtNotRetry: true,
	}
	nowStr := s.pattern.FormatString(time.Now())
	batchDatas := make(map[string][]Data, 1)

	// 如果没有设置 timestamp key 则直接赋值
	if len(s.timestampKey) == 0 {
		batchDatas[nowStr] = datas
	} else {
		var tStr string
		for i := range datas {
			key, ok := datas[i][s.timestampKey].(string)
			if ok {
				t, err := time.Parse(time.RFC3339Nano, key)
				if err != nil {
					ste.Errors++
					ste.LastError = fmt.Sprintf("%s parse timestamp key %q failed: %v", s.Name(), key, err)
					t = time.Now()
				}
				tStr = s.pattern.FormatString(t)
			} else {
				tStr = nowStr
			}

			batchDatas[tStr] = append(batchDatas[tStr], datas[i])
		}
	}

	// 分批写入不同文件
	for filename := range batchDatas {
		bytes, err := s.marshalFunc(batchDatas[filename])
		if err != nil {
			return reqerr.NewSendError(
				fmt.Sprintf("%s marshal data failed: %v", s.Name(), err),
				sender.ConvertDatasBack(datas),
				reqerr.TypeDefault,
			)
		}

		_, err = s.writers.Write(filename, bytes)
		if err != nil {
			return reqerr.NewSendError(
				fmt.Sprintf("%s write data to file failed: %v", s.Name(), err),
				sender.ConvertDatasBack(datas),
				reqerr.TypeDefault,
			)
		}
	}

	if ste.Errors > 0 {
		return ste
	}
	return nil
}

func (s *Sender) Close() error {
	return s.writers.Close()
}
