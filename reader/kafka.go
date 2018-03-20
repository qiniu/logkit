package reader

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
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/utils/models"
	"github.com/wvanbergen/kafka/consumergroup"
)

type KafkaReader struct {
	meta             *Meta
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
	stats      utils.StatsInfo
	statsLock  *sync.RWMutex
}

func NewKafkaReader(meta *Meta, consumerGroup string,
	topics []string, zookeeper []string, zkchroot string, zookeeperTimeout time.Duration, whence string) (kr *KafkaReader, err error) {
	offsets := make(map[string]map[int32]int64)
	for _, v := range topics {
		offsets[v] = make(map[int32]int64)
	}
	kr = &KafkaReader{
		meta:             meta,
		ConsumerGroup:    consumerGroup,
		ZookeeperPeers:   zookeeper,
		ZookeeperTimeout: zookeeperTimeout,
		ZookeeperChroot:  zkchroot,
		Topics:           topics,
		Whence:           whence,
		readChan:         make(chan json.RawMessage),
		errs:             make(chan error, 1000),
		status:           StatusInit,
		mux:              new(sync.Mutex),
		startMux:         new(sync.Mutex),
		statsLock:        new(sync.RWMutex),
		curOffsets:       offsets,
		started:          false,
	}
	return kr, nil
}

func (kr *KafkaReader) Name() string {
	return fmt.Sprintf("KafkaReader:[%s],[%s]", strings.Join(kr.Topics, ","), kr.ConsumerGroup)
}

func (kr *KafkaReader) Status() utils.StatsInfo {
	kr.statsLock.RLock()
	defer kr.statsLock.RUnlock()
	return kr.stats
}

func (kr *KafkaReader) setStatsError(err string) {
	kr.statsLock.Lock()
	defer kr.statsLock.Unlock()
	kr.stats.LastError = err
}

func (kr *KafkaReader) Source() string {
	return fmt.Sprintf("[%s],[%s]", strings.Join(kr.Topics, ","), kr.ConsumerGroup)
}

func (kr *KafkaReader) ReadLine() (data string, err error) {
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

func (kr *KafkaReader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&kr.status, StatusRunning, StatusStopping) {
		log.Infof("Runner[%v] %v stopping", kr.meta.RunnerName, kr.Name())
	} else {
		close(kr.readChan)
	}
	return
}

func (kr *KafkaReader) SyncMeta() {
	kr.mux.Lock()
	if kr.curMsg != nil {
		kr.Consumer.CommitUpto(kr.curMsg)
	}
	kr.mux.Unlock()
	return
}

func (kr *KafkaReader) Start() {
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
	}
	go kr.run()
	kr.started = true
	log.Infof("Runner[%v] %v pull data daemon started", kr.meta.RunnerName, kr.Name())
}

func (kr *KafkaReader) run() {
	// 防止并发run
	for {
		if atomic.LoadInt32(&kr.status) == StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&kr.status, StatusInit, StatusRunning) {
			break
		}
	}
	// running时退出 状态改为Init，以便 cron 调度下次运行
	// stopping时推出改为 stopped，不再运行
	defer func() {
		atomic.CompareAndSwapInt32(&kr.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&kr.status, StatusStopping, StatusStopped) {
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
		if atomic.LoadInt32(&kr.status) == StatusStopping {
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

func (kr *KafkaReader) SetMode(mode string, v interface{}) error {
	return errors.New("KafkaReader not support read mode")
}

func (kr *KafkaReader) Lag() (rl *models.LagInfo, err error) {
	if kr.Consumer == nil {
		return nil, errors.New("kafka consumer is closed")
	}
	marks := kr.Consumer.HighWaterMarks()
	rl = &models.LagInfo{
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
