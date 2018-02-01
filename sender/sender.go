package sender

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
)

// Data store as use key/value map
// e.g sum -> 1.2, url -> qiniu.com
type Data map[string]interface{}

// NotAsyncSender return when sender is not async
var ErrNotAsyncSender = errors.New("This Sender does not support for Async Push")

// Sender send data to pandora, prometheus such different destinations
type Sender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
}

type StatsSender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
	Stats() utils.StatsInfo
	// 恢复 sender 停止之前的状态
	Restore(*utils.StatsInfo)
}

// Sender's conf keys
const (
	KeySenderType     = "sender_type"
	KeyFaultTolerant  = "fault_tolerant"
	KeyName           = "name"
	KeyRunnerName     = "runner_name"
	KeyLogkitSendTime = "logkit_send_time"
	KeyIsMetrics      = "is_metrics"
	KeyMetricTime     = "timestamp"
)

const UnderfinedRunnerName = "UnderfinedRunnerName"

// SenderType 发送类型
const (
	TypeFile              = "file"          // 本地文件
	TypePandora           = "pandora"       // pandora 打点
	TypeMongodbAccumulate = "mongodb_acc"   // mongodb 并且按字段聚合
	TypeInfluxdb          = "influxdb"      // influxdb
	TypeMock              = "mock"          // mock sender
	TypeDiscard           = "discard"       // discard sender
	TypeElastic           = "elasticsearch" // elastic
	TypeKafka             = "kafka"         // kafka
	TypeHttp              = "http"          // http sender
)

const (
	InnerUserAgent = "_useragent"
)

// Ft sender默认同步一次meta信息的数据次数
const DefaultFtSyncEvery = 10

// SenderRegistry sender 的工厂类。可以注册自定义sender
type SenderRegistry struct {
	senderTypeMap map[string]func(conf.MapConf) (Sender, error)
}

func NewSenderRegistry() *SenderRegistry {
	ret := &SenderRegistry{
		senderTypeMap: map[string]func(conf.MapConf) (Sender, error){},
	}
	ret.RegisterSender(TypeFile, NewFileSender)
	ret.RegisterSender(TypePandora, NewPandoraSender)
	ret.RegisterSender(TypeMongodbAccumulate, NewMongodbAccSender)
	ret.RegisterSender(TypeInfluxdb, NewInfluxdbSender)
	ret.RegisterSender(TypeElastic, NewElasticSender)
	ret.RegisterSender(TypeMock, NewMockSender)
	ret.RegisterSender(TypeDiscard, NewDiscardSender)
	ret.RegisterSender(TypeKafka, NewKafkaSender)
	ret.RegisterSender(TypeHttp, NewHttpSender)
	return ret
}

func (r *SenderRegistry) RegisterSender(senderType string, constructor func(conf.MapConf) (Sender, error)) error {
	_, exist := r.senderTypeMap[senderType]
	if exist {
		return errors.New("senderType " + senderType + " has been existed")
	}
	r.senderTypeMap[senderType] = constructor
	return nil
}

func (r *SenderRegistry) NewSender(conf conf.MapConf, ftSaveLogPath string) (sender Sender, err error) {
	sendType, err := conf.GetString(KeySenderType)
	if err != nil {
		return
	}
	constructor, exist := r.senderTypeMap[sendType]
	if !exist {
		return nil, fmt.Errorf("sender type unsupperted : %v", sendType)
	}
	sender, err = constructor(conf)
	if err != nil {
		return
	}
	faultTolerant, _ := conf.GetBoolOr(KeyFaultTolerant, true)
	if faultTolerant {
		sender, err = NewFtSender(sender, conf, ftSaveLogPath)
		if err != nil {
			return
		}
	}
	return sender, nil
}
