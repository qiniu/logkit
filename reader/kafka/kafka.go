package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/qiniu/log"
	"github.com/wvanbergen/kafka/consumergroup"

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
	curMsg   *sarama.ConsumerMessage

	readChan chan json.RawMessage
	errs     <-chan error

	status   int32
	mux      *sync.Mutex
	startMux *sync.Mutex
	started  bool

	curOffsets map[string]map[int32]int64
	stats      StatsInfo
	statsLock  *sync.RWMutex
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (kr reader.Reader, err error) {

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
	kr = &Reader{
		meta:             meta,
		ConsumerGroup:    consumerGroup,
		ZookeeperPeers:   zookeeper,
		ZookeeperTimeout: time.Duration(zookeeperTimeout) * time.Second,
		ZookeeperChroot:  zkchroot,
		Topics:           topics,
		Whence:           whence,
		readChan:         make(chan json.RawMessage),
		errs:             make(chan error, 1000),
		status:           reader.StatusInit,
		mux:              new(sync.Mutex),
		startMux:         new(sync.Mutex),
		statsLock:        new(sync.RWMutex),
		curOffsets:       offsets,
		started:          false,
	}
	sarama.Logger = log.Std
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
	if !kr.started {
		kr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-kr.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (kr *Reader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&kr.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] %v stopping", kr.meta.RunnerName, kr.Name())
	} else {
		atomic.CompareAndSwapInt32(&kr.status, reader.StatusInit, reader.StatusStopped)
		close(kr.readChan)
	}
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

func (kr *Reader) Start() {
	kr.startMux.Lock()
	defer kr.startMux.Unlock()
	if kr.started {
		return
	}
	config := consumergroup.NewConfig()
	config.Zookeeper.Chroot = kr.ZookeeperChroot
	config.Zookeeper.Timeout = kr.ZookeeperTimeout
	switch strings.ToLower(kr.Whence) {
	case "oldest", "":
		config.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Offsets.Initial = sarama.OffsetNewest
	default:
		log.Warnf("Runner[%v] WARNING: Kafka consumer invalid offset '%s', using 'oldest'\n", kr.meta.RunnerName, kr.Whence)
		config.Offsets.Initial = sarama.OffsetOldest
	}
	if kr.Consumer == nil || kr.Consumer.Closed() {
		var consumerErr error
		kr.Consumer, consumerErr = consumergroup.JoinConsumerGroup(
			kr.ConsumerGroup,
			kr.Topics,
			kr.ZookeeperPeers,
			config,
		)
		if consumerErr != nil {
			log.Errorf("%v", consumerErr)
			return
		}
		kr.in = kr.Consumer.Messages()
		kr.errs = kr.Consumer.Errors()
	}
	go kr.run()
	kr.started = true
	log.Infof("Runner[%v] %v pull data daemon started", kr.meta.RunnerName, kr.Name())
}

func (kr *Reader) run() {
	// 防止并发run
	for {
		if atomic.LoadInt32(&kr.status) == reader.StatusStopped || atomic.LoadInt32(&kr.status) == reader.StatusStopping {
			return
		}
		if atomic.CompareAndSwapInt32(&kr.status, reader.StatusInit, reader.StatusRunning) {
			break
		}
	}
	//double check
	if atomic.LoadInt32(&kr.status) == reader.StatusStopped || atomic.LoadInt32(&kr.status) == reader.StatusStopping {
		return
	}
	// running时退出 状态改为Init，以便 cron 调度下次运行
	// stopping时推出改为 stopped，不再运行
	defer func() {
		atomic.CompareAndSwapInt32(&kr.status, reader.StatusRunning, reader.StatusInit)
		if atomic.CompareAndSwapInt32(&kr.status, reader.StatusStopping, reader.StatusStopped) {
			close(kr.readChan)
			go func() {
				kr.mux.Lock()
				defer kr.mux.Unlock()
				err := kr.Consumer.Close()
				if err != nil {
					log.Infof("Runner[%v] %v stop failed error: %v", kr.meta.RunnerName, kr.Name(), err)
				}
			}()
		}
		log.Infof("Runner[%v] %v successfully finished", kr.meta.RunnerName, kr.Name())
	}()
	// 开始work逻辑
	for {
		if atomic.LoadInt32(&kr.status) == reader.StatusStopping {
			log.Warnf("Runner[%v] %v stopped from running", kr.meta.RunnerName, kr.Name())
			return
		}
		select {
		case err := <-kr.errs:
			if err != nil {
				log.Errorf("Runner[%v] Consumer Error: %s\n", kr.meta.RunnerName, err)
				kr.setStatsError("Runner[" + kr.meta.RunnerName + "] Consumer Error: " + err.Error())
			}
		case msg := <-kr.in:
			if msg != nil && msg.Value != nil && len(msg.Value) > 0 {
				kr.readChan <- msg.Value
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
		default:
			time.Sleep(time.Second)
		}
	}
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
