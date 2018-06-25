package queuev2

import (
	"sync"
	"github.com/Shopify/sarama"
	"os"
	"time"
	"errors"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/qiniu/log"
	"github.com/wvanbergen/kazoo-go"
)

const(
	Topic = "edge_topic"
	Group = "edge_group"

	StatusInit int32 = iota
	StatusClosed
)

type kafkaQueue struct {
	name     string
	channel  chan []byte
	mux      sync.Mutex
	status   int32
	quit     chan bool
	topic    string
	proudcer sarama.SyncProducer
	consumer *consumergroup.ConsumerGroup
	broker   *sarama.Broker
	in       <-chan *sarama.ConsumerMessage
	curMsg   *sarama.ConsumerMessage
	exitChan chan struct{}
}

func NewKafkaQueue(name string, hosts []string, exitChan chan struct{}) BackendQueue {
	producer, err := createProducer(hosts)
	if err != nil {
		log.Error("kafka queue: can not create producer, err : " + err.Error())
		return nil
	}

	consumer, err := createConsumer(hosts)
	if err != nil {
		log.Error("kafka queue: can not create consumer, err :" + err.Error())
		return nil
	}


	kq :=  &kafkaQueue{
		name:     name,
		channel:  make(chan []byte),
		mux:      sync.Mutex{},
		status:   StatusInit,
		quit:     make(chan bool),
		proudcer: producer,
		consumer: consumer,
		in:       consumer.Messages(),
		exitChan: exitChan,
		topic:    Topic,
	}

	go kq.startConsumer()

	return kq
}

func createProducer(hosts []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "getHostnameErr:" + err.Error()
		err = nil
	}
	cfg.ClientID = hostName
	cfg.Producer.Retry.Max = 3
	cfg.Net.DialTimeout, err = time.ParseDuration("30s")
	if err != nil {
		return nil, errors.New(" invalid cfg net diatimeout")
	}
	cfg.Net.KeepAlive, err = time.ParseDuration("0")
	if err != nil {
		return nil, errors.New(" invalid cfg net keepalive")
	}
	cfg.Producer.MaxMessageBytes = 4*1024*1024

	var kz *kazoo.Kazoo
	if kz, err = kazoo.NewKazoo(hosts, nil); err != nil {
		return nil, errors.New(" failed to get brokers, err: " + err.Error())
	}

	brokers, err := kz.BrokerList()
	if err != nil {
		kz.Close()
		return nil, errors.New(" failed to get brokers, err: " + err.Error())
	}

	log.Info("kafkaqueue: broker list : ", brokers)

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, errors.New(" producer is invalid, err: " + err.Error())
	}
	return  producer, nil
}

func createConsumer(hosts []string) (*consumergroup.ConsumerGroup, error) {
	config := consumergroup.NewConfig()
	config.Zookeeper.Chroot = ""
	config.Zookeeper.Timeout = time.Duration(30) * time.Second
	config.Offsets.Initial = sarama.OffsetOldest
	topic := []string{Topic,}

	Consumer, consumerErr := consumergroup.JoinConsumerGroup(
		Group,
		topic,
		hosts,
		config,
	)

	if consumerErr != nil {
		return nil, consumerErr
	}

	return Consumer, nil
}

func (kq *kafkaQueue) Name() string {
	return kq.name
}

func (kq *kafkaQueue) Put(msg []byte) error {
	kq.mux.Lock()
	defer kq.mux.Unlock()

	pm := &sarama.ProducerMessage{
		Topic: kq.topic,
		Value: sarama.StringEncoder(msg),
	}

	_, _ ,err := kq.proudcer.SendMessage(pm)
	if err != nil {
		return err
	}
	return nil
}

func (kq *kafkaQueue) sendToChan() {

}

func (kq *kafkaQueue) ReadChan() <-chan []byte {
	return kq.channel
}

func (kq *kafkaQueue) SyncMeta() {
	kq.mux.Lock()
	if kq.curMsg != nil {
		kq.consumer.CommitUpto(kq.curMsg)
	}
	kq.mux.Unlock()
	return
}

func (kq *kafkaQueue) Close() error {
	close(kq.quit)

	kq.mux.Lock()
	defer kq.mux.Unlock()
	kq.status = StatusClosed
	close(kq.channel)
	return nil
}

func (kq *kafkaQueue) Delete() error {
	return kq.Close()
}

func (kq *kafkaQueue) Depth() int64 {
	return 0
}

func (kq *kafkaQueue) Empty() error {
	return nil
}

func (kq *kafkaQueue) startConsumer() {
	// 开始work逻辑
	for {
		select {
		case <-kq.exitChan:
			break
			log.Info("Kafka Queue Consumer is end")
		case msg := <-kq.in:
			if msg != nil && msg.Value != nil && len(msg.Value) > 0 {
				kq.channel <- msg.Value
				kq.curMsg = msg
			}
		default:
			time.Sleep(time.Second)
		}
	}
}
