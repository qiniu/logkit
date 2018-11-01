package sender

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/sender/config"
	. "github.com/qiniu/logkit/utils/models"
)

// Sender send data to pandora, prometheus such different destinations
type Sender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
}

// SkipDeepCopySender 表示该 sender 不会对传入数据进行污染，凡是有次保证的 sender 需要实现该接口提升发送效率
type SkipDeepCopySender interface {
	// SkipDeepCopy 需要返回值是因为如果一个 sender 封装了其它 sender，需要根据实际封装的类型返回是否忽略深度拷贝
	SkipDeepCopy() bool
}

type StatsSender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
	Stats() StatsInfo
	// 恢复 sender 停止之前的状态
	Restore(*StatsInfo)
}

// SenderRegistry sender 的工厂类。可以注册自定义sender
type Registry struct {
	senderTypeMap map[string]func(conf.MapConf) (Sender, error)
}

type Constructor func(conf.MapConf) (Sender, error)

// registeredConstructors keeps a list of all available reader constructors can be registered by Registry.
var registeredConstructors = map[string]Constructor{}

// RegisterConstructor adds a new constructor for a given type of reader.
func RegisterConstructor(typ string, c Constructor) {
	registeredConstructors[typ] = c
}

func NewRegistry() *Registry {
	ret := &Registry{
		senderTypeMap: map[string]func(conf.MapConf) (Sender, error){},
	}

	for typ, c := range registeredConstructors {
		ret.RegisterSender(typ, c)
	}

	return ret
}

func (r *Registry) RegisterSender(senderType string, constructor func(conf.MapConf) (Sender, error)) error {
	_, exist := r.senderTypeMap[senderType]
	if exist {
		return errors.New("senderType " + senderType + " has been existed")
	}
	r.senderTypeMap[senderType] = constructor
	return nil
}

func (r *Registry) NewSender(conf conf.MapConf, ftSaveLogPath string) (sender Sender, err error) {
	sendType, err := conf.GetString(KeySenderType)
	if err != nil {
		return
	}
	constructor, exist := r.senderTypeMap[sendType]
	if !exist {
		return nil, fmt.Errorf("sender type unsupported : %v", sendType)
	}
	sender, err = constructor(conf)
	if err != nil {
		return
	}
	faultTolerant, _ := conf.GetBoolOr(KeyFaultTolerant, true)

	//如果是 PandoraSender，目前的依赖必须启用 ftsender,依赖Ftsender做key转换检查
	if faultTolerant || sendType == TypePandora {
		sender, err = NewFtSender(sender, conf, ftSaveLogPath)
		if err != nil {
			return
		}
	}
	return sender, nil
}

type TokenRefreshable interface {
	TokenRefresh(conf.MapConf) error
}

func ConvertDatas(ins []map[string]interface{}) []Data {
	var datas []Data
	for _, v := range ins {
		datas = append(datas, Data(v))
	}
	return datas
}
func ConvertDatasBack(ins []Data) []map[string]interface{} {
	var datas []map[string]interface{}
	for _, v := range ins {
		datas = append(datas, map[string]interface{}(v))
	}
	return datas
}
