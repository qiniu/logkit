package registry

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender/common"
	"github.com/qiniu/logkit/sender/discard"
	"github.com/qiniu/logkit/sender/elasticsearch"
	"github.com/qiniu/logkit/sender/fault_tolerant"
	"github.com/qiniu/logkit/sender/file"
	"github.com/qiniu/logkit/sender/http"
	"github.com/qiniu/logkit/sender/influxdb"
	"github.com/qiniu/logkit/sender/kafka"
	"github.com/qiniu/logkit/sender/mock"
	"github.com/qiniu/logkit/sender/mongodb"
	"github.com/qiniu/logkit/sender/pandora"
	. "github.com/qiniu/logkit/utils/models"
)

// SenderRegistry sender 的工厂类。可以注册自定义sender
type SenderRegistry struct {
	senderTypeMap map[string]func(conf.MapConf) (common.Sender, error)
}

func NewSenderRegistry() *SenderRegistry {
	ret := &SenderRegistry{
		senderTypeMap: map[string]func(conf.MapConf) (common.Sender, error){},
	}
	ret.RegisterSender(TypeFile, file.NewFileSender)
	ret.RegisterSender(TypePandora, pandora.NewPandoraSender)
	ret.RegisterSender(TypeMongodbAccumulate, mongodb.NewMongodbAccSender)
	ret.RegisterSender(TypeInfluxdb, influxdb.NewInfluxdbSender)
	ret.RegisterSender(TypeElastic, elasticsearch.NewElasticSender)
	ret.RegisterSender(TypeMock, mock.NewMockSender)
	ret.RegisterSender(TypeDiscard, discard.NewDiscardSender)
	ret.RegisterSender(TypeKafka, kafka.NewKafkaSender)
	ret.RegisterSender(TypeHttp, http.NewHttpSender)
	return ret
}

func (registry *SenderRegistry) RegisterSender(senderType string, constructor func(conf.MapConf) (common.Sender, error)) error {
	_, exist := registry.senderTypeMap[senderType]
	if exist {
		return errors.New("senderType " + senderType + " has been existed")
	}
	registry.senderTypeMap[senderType] = constructor
	return nil
}

func (r *SenderRegistry) NewSender(conf conf.MapConf, ftSaveLogPath string) (sender common.Sender, err error) {
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
		sender, err = fault_tolerant.NewFtSender(sender, conf, ftSaveLogPath)
		if err != nil {
			return
		}
	}
	return sender, nil
}
