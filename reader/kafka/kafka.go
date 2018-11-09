package kafka

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"

	"github.com/qiniu/bytes"
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
	currentMsg     *sarama.ConsumerMessage
	currentOffsets map[string]map[int32]int64

	stats     StatsInfo
	statsLock *sync.RWMutex

	ConsumerGroup    string
	Topics           []string
	ZookeeperPeers   []string
	ZookeeperChroot  string
	ZookeeperTimeout time.Duration
	Whence           string
	Gzip             bool
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

	zookeeper, err := conf.GetStringList(KeyKafkaZookeeper)
	if err != nil {
		return nil, err
	}
	zkchroot, _ := conf.GetStringOr(KeyKafkaZookeeperChroot, "")
	offsets := make(map[string]map[int32]int64)
	for _, v := range topics {
		offsets[v] = make(map[int32]int64)
	}

	gzip, _ := conf.GetBoolOr(KeyKafkaUncompressGzip, false)

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
		Gzip:             gzip,
	}

	config := consumergroup.NewConfig()
	config.Zookeeper.Chroot = kr.ZookeeperChroot
	config.Zookeeper.Timeout = kr.ZookeeperTimeout
	config.Consumer.Return.Errors = true

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

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *Reader) Name() string {
	return fmt.Sprintf("KafkaReader:[%s],[%s]", strings.Join(r.Topics, ","), r.ConsumerGroup)
}

func (_ *Reader) SetMode(_ string, _ interface{}) error {
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
			if r.Gzip {
				gr, gerr := gzip.NewReader(bytes.NewReader(msg.Value))
				if gerr != nil {
					gerr = fmt.Errorf("runner[%v] Uncompress gzip Error: %s\n", r.meta.RunnerName, gerr)
					log.Error(gerr)
					return "", gerr
				}
				defer r.Close()
				bLine, rerr := ioutil.ReadAll(gr)
				if rerr != nil {
					rerr = fmt.Errorf("runner[%v] Uncompress gzip Error: %s\n", r.meta.RunnerName, rerr)
					log.Error(rerr)
					return "", rerr
				}
				line = string(bLine)
			} else {
				line = string(msg.Value)
			}
			r.currentMsg = msg
			r.statsLock.Lock()
			if tp, ok := r.currentOffsets[msg.Topic]; ok {
				tp[msg.Partition] = msg.Offset
				r.currentOffsets[msg.Topic] = tp
			} else {
				tp := make(map[int32]int64)
				tp[msg.Partition] = msg.Offset
				r.currentOffsets[msg.Topic] = tp
			}
			r.statsLock.Unlock()
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
	r.statsLock.RLock()
	for _, ptv := range r.currentOffsets {
		for _, v := range ptv {
			rl.Size -= v
		}
	}
	r.statsLock.RUnlock()
	return rl, nil
}

func (r *Reader) SyncMeta() {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.currentMsg != nil {
		r.Consumer.CommitUpto(r.currentMsg)
	}
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())

	r.lock.Lock()
	defer r.lock.Unlock()

	err := r.Consumer.Close()
	atomic.StoreInt32(&r.status, StatusStopped)
	return err
}
