package reader

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fmt"

	"github.com/Shopify/sarama"
	"github.com/qiniu/log"
	"github.com/wvanbergen/kafka/consumergroup"
)

type KafkaReader struct {
	meta            *Meta
	ConsumerGroup   string
	Topics          []string
	ZookeeperPeers  []string
	ZookeeperChroot string
	Whence          string

	Consumer *consumergroup.ConsumerGroup
	in       <-chan *sarama.ConsumerMessage
	curMsg   *sarama.ConsumerMessage

	readChan chan json.RawMessage
	errs     <-chan error

	status  int32
	mux     sync.Mutex
	started bool
}

func NewKafkaReader(meta *Meta, consumerGroup string,
	topics []string, zookeeper []string, whence string) (kr *KafkaReader, err error) {
	kr = &KafkaReader{
		meta:           meta,
		ConsumerGroup:  consumerGroup,
		ZookeeperPeers: zookeeper,
		Topics:         topics,
		Whence:         whence,
		readChan:       make(chan json.RawMessage),
		errs:           make(chan error, 1000),
		status:         StatusInit,
		mux:            sync.Mutex{},
		started:        false,
	}
	return kr, nil
}
func (kr *KafkaReader) Name() string {
	return fmt.Sprintf("KafkaReader:[%s],[%s]", strings.Join(kr.Topics, ","), kr.ConsumerGroup)
}
func (kr *KafkaReader) Source() string {
	return fmt.Sprintf("[%s],[%s]", strings.Join(kr.Topics, ","), kr.ConsumerGroup)
}
func (kr *KafkaReader) ReadLine() (data string, err error) {
	if !kr.started {
		kr.Start()
	}
	timer := time.NewTicker(time.Millisecond)
	select {
	case dat := <-kr.readChan:
		data = string(dat)
	case <-timer.C:
	}
	return

}
func (kr *KafkaReader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&kr.status, StatusRunning, StatusStoping) {
		log.Infof("%v stopping", kr.Name())
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
	config := consumergroup.NewConfig()
	config.Zookeeper.Chroot = ""
	switch strings.ToLower(kr.Whence) {
	case "oldest", "":
		config.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Offsets.Initial = sarama.OffsetNewest
	default:
		log.Printf("I! WARNING: Kafka consumer invalid offset '%s', using 'oldest'\n",
			kr.Whence)
		config.Offsets.Initial = sarama.OffsetNewest
	}
	if kr.Consumer == nil {
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
	log.Printf("%v pull data deamon started", kr.Name())
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
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&kr.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&kr.status, StatusStoping, StatusStopped) {
			close(kr.readChan)
		}
		log.Infof("%v successfully finnished", kr.Name())
	}()
	// 开始work逻辑
	for {
		if atomic.LoadInt32(&kr.status) == StatusStoping {
			log.Warnf("%v stopped from running", kr.Name())
			return
		}
		select {
		case err := <-kr.errs:
			if err != nil {
				log.Errorf("Consumer Error: %s\n", err)
			}
		case msg := <-kr.in:
			kr.readChan <- msg.Value
			kr.curMsg = msg
		}
	}
}
