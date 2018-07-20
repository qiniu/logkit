package kafka

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	reader.RegisterConstructor(reader.ModeKafka, NewReader)
}

type Reader struct {
	meta             *reader.Meta
	ConsumerGroup    string
	Topics           []string
	ZookeeperPeers   []string
	ZookeeperChroot  string
	ZookeeperTimeout time.Duration
	Whence           string

	Consumer *consumergroup.ConsumerGroup
	in       <-chan *sarama.ConsumerMessage
	errs     <-chan error
	curMsg   *sarama.ConsumerMessage

	mux *sync.Mutex

	curOffsets map[string]map[int32]int64
	stats      StatsInfo
	statsLock  *sync.RWMutex
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {

	whence, _ := conf.GetStringOr(reader.KeyWhence, reader.WhenceOldest)
	consumerGroup, err := conf.GetString(reader.KeyKafkaGroupID)
	if err != nil {
		return nil, err
	}
	topics, err := conf.GetStringList(reader.KeyKafkaTopic)
	if err != nil {
		return nil, err
	}
	zookeeperTimeout, _ := conf.GetIntOr(reader.KeyKafkaZookeeperTimeout, 1)

	zookeeper, err := conf.GetStringList(reader.KeyKafkaZookeeper)
	if err != nil {
		return nil, err
	}
	zkchroot, _ := conf.GetStringOr(reader.KeyKafkaZookeeperChroot, "")
	offsets := make(map[string]map[int32]int64)
	for _, v := range topics {
		offsets[v] = make(map[int32]int64)
	}
	sarama.Logger = log.Std
	kr := &Reader{
		meta:             meta,
		ConsumerGroup:    consumerGroup,
		ZookeeperPeers:   zookeeper,
		ZookeeperTimeout: time.Duration(zookeeperTimeout) * time.Second,
		ZookeeperChroot:  zkchroot,
		Topics:           topics,
		Whence:           whence,
		mux:              new(sync.Mutex),
		statsLock:        new(sync.RWMutex),
		curOffsets:       offsets,
	}

	config := consumergroup.NewConfig()
	config.Zookeeper.Chroot = kr.ZookeeperChroot
	config.Zookeeper.Timeout = kr.ZookeeperTimeout
	//config.Consumer.Return.Errors = true

	/*********************  kafka offset *************************/
	/* 这里设定的offset不影响原有的offset，因为kafka client会去获取   */
	/* zk之前的offset node记录，在没有offset node时才选择initial     */
	switch strings.ToLower(kr.Whence) {
	case "oldest", "":
		config.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Offsets.Initial = sarama.OffsetNewest
	default:
		log.Warnf("Runner[%v] WARNING: Kafka consumer invalid offset '%s', using 'oldest'\n", kr.meta.RunnerName, kr.Whence)
		config.Offsets.Initial = sarama.OffsetOldest
	}
	/*************************************************************/

	var consumerErr error
	kr.Consumer, consumerErr = consumergroup.JoinConsumerGroup(
		kr.ConsumerGroup,
		kr.Topics,
		kr.ZookeeperPeers,
		config,
	)
	if consumerErr != nil {
		err = fmt.Errorf("runner[%v] kafka reader join group err: %v", kr.meta.RunnerName, consumerErr)
		log.Error(err)
		return nil, err
	}
	kr.in = kr.Consumer.Messages()
	kr.errs = kr.Consumer.Errors()
	return kr, nil
}

func (kr *Reader) Name() string {
	return fmt.Sprintf("KafkaReader:[%s],[%s]", strings.Join(kr.Topics, ","), kr.ConsumerGroup)
}

func (kr *Reader) Status() StatsInfo {
	kr.statsLock.RLock()
	defer kr.statsLock.RUnlock()
	return kr.stats
}

func (kr *Reader) setStatsError(err string) {
	kr.statsLock.Lock()
	defer kr.statsLock.Unlock()
	kr.stats.LastError = err
}

func (kr *Reader) Source() string {
	return fmt.Sprintf("[%s],[%s]", strings.Join(kr.Topics, ","), kr.ConsumerGroup)
}

func (kr *Reader) ReadLine() (data string, err error) {
	timer := time.NewTimer(time.Second)
	select {
	case err = <-kr.errs:
		if err != nil {
			err = fmt.Errorf("runner[%v] Consumer Error: %s\n", kr.meta.RunnerName, err)
			log.Error(err)
			kr.setStatsError(err.Error())
		}
	case msg := <-kr.in:
		if msg != nil && msg.Value != nil && len(msg.Value) > 0 {
			data = string(msg.Value)
			kr.curMsg = msg
			kr.statsLock.Lock()
			if tp, ok := kr.curOffsets[msg.Topic]; ok {
				tp[msg.Partition] = msg.Offset
				kr.curOffsets[msg.Topic] = tp
			} else {
				tp := make(map[int32]int64)
				tp[msg.Partition] = msg.Offset
				kr.curOffsets[msg.Topic] = tp
			}
			kr.statsLock.Unlock()
		}
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (kr *Reader) Close() (err error) {
	kr.mux.Lock()
	err = kr.Consumer.Close()
	kr.mux.Unlock()
	return
}

func (kr *Reader) SyncMeta() {
	kr.mux.Lock()
	if kr.curMsg != nil {
		kr.Consumer.CommitUpto(kr.curMsg)
	}
	kr.mux.Unlock()
	return
}

func (kr *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("KafkaReader not support read mode")
}

func (kr *Reader) Lag() (rl *LagInfo, err error) {
	if kr.Consumer == nil {
		return nil, errors.New("kafka consumer is closed")
	}
	marks := kr.Consumer.HighWaterMarks()
	rl = &LagInfo{
		SizeUnit: "records",
		Size:     0,
	}
	for _, ptv := range marks {
		for _, v := range ptv {
			rl.Size += v
		}
	}
	kr.statsLock.RLock()
	for _, ptv := range kr.curOffsets {
		for _, v := range ptv {
			rl.Size -= v
		}
	}
	kr.statsLock.RUnlock()
	return
}
