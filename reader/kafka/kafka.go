package kafka

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.StatsReader = &Reader{}
	_ reader.LagReader   = &Reader{}
	_ reader.Reader      = &Reader{}
)

func init() {
	reader.RegisterConstructor(ModeKafka, NewReader)
}

type Reader struct {
	lock *sync.Mutex
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32

	readChan <-chan *sarama.ConsumerMessage
	errChan  <-chan error

	Consumer       *consumergroup.ConsumerGroup
	currentOffsets map[string]map[int32]int64 // <topic,<partition,offset>>

	stats     StatsInfo
	statsLock *sync.RWMutex

	ConsumerGroup    string
	Topics           []string
	ZookeeperPeers   []string
	ZookeeperChroot  string
	ZookeeperTimeout time.Duration
	Whence           string
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {

	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	consumerGroup, err := conf.GetString(KeyKafkaGroupID)
	if err != nil {
		return nil, err
	}
	topics, err := conf.GetStringList(KeyKafkaTopic)
	if err != nil {
		return nil, err
	}
	zookeeperTimeout, _ := conf.GetIntOr(KeyKafkaZookeeperTimeout, 1)
	maxProcessingTime, _ := conf.GetStringOr(KeyKafkaMaxProcessTime, "1s")
	maxProcessingTimeDur, err := time.ParseDuration(maxProcessingTime)
	if err != nil {
		return nil, fmt.Errorf("parse %s %s err %v", KeyKafkaMaxProcessTime, maxProcessingTime, err)
	}

	zookeeper, err := conf.GetStringList(KeyKafkaZookeeper)
	if err != nil {
		return nil, err
	}
	zkchroot, _ := conf.GetStringOr(KeyKafkaZookeeperChroot, "")
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
		lock:             new(sync.Mutex),
		statsLock:        new(sync.RWMutex),
		currentOffsets:   offsets,
	}

	config := consumergroup.NewConfig()
	config.Zookeeper.Chroot = kr.ZookeeperChroot
	config.Zookeeper.Timeout = kr.ZookeeperTimeout
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = maxProcessingTimeDur

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
	kr.readChan = kr.Consumer.Messages()
	kr.errChan = kr.Consumer.Errors()
	return kr, nil
}

func (r *Reader) startMarkOffset() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if r.isStopping() || r.hasStopped() {
				return
			}
			r.markOffset()
		}
	}
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *Reader) Name() string {
	return fmt.Sprintf("KafkaReader:[%s],[%s]", strings.Join(r.Topics, ","), r.ConsumerGroup)
}

func (*Reader) SetMode(_ string, _ interface{}) error {
	return errors.New("KafkaReader does not support read mode")
}

func (r *Reader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *Reader) Source() string {
	return fmt.Sprintf("[%s],[%s]", strings.Join(r.Topics, ","), r.ConsumerGroup)
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case msg := <-r.readChan:
		var line string
		if msg != nil && msg.Value != nil && len(msg.Value) > 0 {
			line = string(msg.Value)
			r.lock.Lock()
			if tp, ok := r.currentOffsets[msg.Topic]; ok {
				tp[msg.Partition] = msg.Offset
				r.currentOffsets[msg.Topic] = tp
			} else {
				tp := make(map[int32]int64)
				tp[msg.Partition] = msg.Offset
				r.currentOffsets[msg.Topic] = tp
			}
			r.lock.Unlock()
		} else {
			log.Debugf("runner[%v] Consumer read empty message: %v", r.meta.RunnerName, msg)
		}
		return line, nil
	case err := <-r.errChan:
		if err != nil {
			err = fmt.Errorf("runner[%v] Consumer Error: %s\n", r.meta.RunnerName, err)
			log.Error(err)
			r.setStatsError(err.Error())
		}
		return "", err
	case <-timer.C:
	}

	return "", nil
}

func (r *Reader) Status() StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

func (r *Reader) Start() error {
	go r.startMarkOffset()
	return nil
}

func (r *Reader) Lag() (*LagInfo, error) {
	if r.Consumer == nil {
		return nil, errors.New("kafka consumer is closed")
	}
	marks := r.Consumer.HighWaterMarks()
	rl := &LagInfo{
		SizeUnit: "records",
		Size:     0,
	}
	for _, ptv := range marks {
		for _, v := range ptv {
			rl.Size += v
		}
	}
	r.lock.Lock()
	for _, ptv := range r.currentOffsets {
		for _, v := range ptv {
			rl.Size -= v + 1 //HighWaterMarks 拿到的是下一个数据的Offset，所以实际在算size的时候多了1，要在现在扣掉。
		}
	}
	r.lock.Unlock()
	return rl, nil
}

func (r *Reader) SyncMeta() {
	r.markOffset()
}

func (r *Reader) markOffset() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for topic, partOffset := range r.currentOffsets {
		if partOffset == nil {
			continue
		}
		for partition, offset := range partOffset {
			r.Consumer.CommitUpto(&sarama.ConsumerMessage{
				Topic:     topic,
				Partition: partition,
				Offset:    offset,
			})
		}
	}
}

func (r *Reader) stop() error {
	r.markOffset()

	r.lock.Lock()
	defer r.lock.Unlock()

	err := r.Consumer.FlushOffsets()
	if err != nil {
		log.Errorf("Runner[%v] reader %q flush kafka offset error: %v", r.meta.RunnerName, r.Name(), err.Error())
	}

	err = r.Consumer.Close()
	if err != nil {
		log.Errorf("Runner[%v] reader %q close kafka consumer error: %v", r.meta.RunnerName, r.Name(), err.Error())
	}

	atomic.StoreInt32(&r.status, StatusStopped)
	return err
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	return r.stop()
}
