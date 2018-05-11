package kafka

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/qiniu/log"

	"github.com/Shopify/sarama"
	"github.com/json-iterator/go"
)

type KafkaSender struct {
	name  string
	hosts []string
	topic []string
	cfg   *sarama.Config

	producer sarama.SyncProducer
}

var (
	compressionModes = map[string]sarama.CompressionCodec{
		sender.KeyKafkaCompressionNone:   sarama.CompressionNone,
		sender.KeyKafkaCompressionGzip:   sarama.CompressionGZIP,
		sender.KeyKafkaCompressionSnappy: sarama.CompressionSnappy,
	}
)

func NewKafkaSender(conf conf.MapConf) (kafkaSender sender.Sender, err error) {
	hosts, err := conf.GetStringList(sender.KeyKafkaHost)
	if err != nil {
		return
	}
	topic, err := conf.GetStringList(sender.KeyKafkaTopic)
	if err != nil {
		return
	}
	topic, err = ExtractField(topic)
	if err != nil {
		return
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "getHostnameErr:" + err.Error()
		err = nil
	}
	clientID, _ := conf.GetStringOr(sender.KeyKafkaClientId, hostName)
	//num, _ := conf.GetIntOr(KeyKafkaFlushNum, 200)
	//frequency, _ := conf.GetIntOr(KeyKafkaFlushFrequency, 5)
	retryMax, _ := conf.GetIntOr(sender.KeyKafkaRetryMax, 3)
	compression, _ := conf.GetStringOr(sender.KeyKafkaCompression, sender.KeyKafkaCompressionNone)
	timeout, _ := conf.GetStringOr(sender.KeyKafkaTimeout, "30s")
	keepAlive, _ := conf.GetStringOr(sender.KeyKafkaKeepAlive, "0")
	maxMessageBytes, _ := conf.GetIntOr(sender.KeyMaxMessageBytes, 4*1024*1024)

	name, _ := conf.GetStringOr(sender.KeyName, fmt.Sprintf("kafkaSender:(kafkaUrl:%s,topic:%s)", hosts, topic))
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	//cfg.Producer.Return.Successes = false
	//cfg.Producer.Return.Errors = false
	//客户端ID
	cfg.ClientID = clientID
	//批量发送条数
	//cfg.Producer.Flush.Messages = num
	//批量发送间隔
	//cfg.Producer.Flush.Frequency =  time.Duration(frequency) * time.Second
	cfg.Producer.Retry.Max = retryMax
	compressionMode, ok := compressionModes[strings.ToLower(compression)]
	if !ok {
		return nil, fmt.Errorf("unknown compression mode: '%v'", compression)
	}
	cfg.Producer.Compression = compressionMode
	cfg.Net.DialTimeout, err = time.ParseDuration(timeout)
	if err != nil {
		return
	}
	cfg.Net.KeepAlive, err = time.ParseDuration(keepAlive)
	if err != nil {
		return
	}
	cfg.Producer.MaxMessageBytes = maxMessageBytes

	producer, err := sarama.NewSyncProducer(hosts, cfg)
	if err != nil {
		return
	}

	kafkaSender = newKafkaSender(name, hosts, topic, cfg, producer)
	return
}

func newKafkaSender(name string, hosts []string, topic []string, cfg *sarama.Config, producer sarama.SyncProducer) (k *KafkaSender) {
	k = &KafkaSender{
		name:     name,
		hosts:    hosts,
		topic:    topic,
		cfg:      cfg,
		producer: producer,
	}
	return
}

func (this *KafkaSender) Name() string {
	return this.name
}

func (this *KafkaSender) Send(data []Data) error {
	producer := this.producer
	var msgs []*sarama.ProducerMessage
	ss := &StatsError{}
	var lastErr error
	for _, doc := range data {
		message, err := this.getEventMessage(doc)
		if err != nil {
			log.Debugf("Dropping event: %v", err)
			ss.AddErrors()
			lastErr = err
			continue
		}
		msgs = append(msgs, message)
	}
	err := producer.SendMessages(msgs)
	if err != nil {
		ss.AddErrors()
		ss.ErrorDetail = err
		return ss
	}
	ss.AddSuccess()
	if lastErr != nil {
		ss.LastError = lastErr.Error()
	}
	return ss
}

func (kf *KafkaSender) getEventMessage(event map[string]interface{}) (pm *sarama.ProducerMessage, err error) {
	var topic string
	if len(kf.topic) == 2 {
		if event[kf.topic[0]] == nil || event[kf.topic[0]] == "" {
			topic = kf.topic[1]
		} else {
			if mytopic, ok := event[kf.topic[0]].(string); ok {
				topic = mytopic
			} else {
				topic = kf.topic[1]
			}
		}
	} else {
		topic = kf.topic[0]
	}
	value, err := jsoniter.Marshal(event)
	if err != nil {
		return
	}
	pm = &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(value)),
	}
	return
}

func (this *KafkaSender) Close() (err error) {
	log.Infof("kafka sender was closed")
	this.producer.Close()
	this.producer = nil
	return nil
}

func init() {
	sender.Add(sender.TypeKafka, NewKafkaSender)
}
