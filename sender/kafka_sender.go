package sender

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"

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

const (
	KeyKafkaCompressionNone   = "none"
	KeyKafkaCompressionGzip   = "gzip"
	KeyKafkaCompressionSnappy = "snappy"
)

const (
	KeyKafkaHost     = "kafka_host"      //主机地址,可以有多个
	KeyKafkaTopic    = "kafka_topic"     //topic 1.填一个值,则topic为所填值 2.天两个值: %{[字段名]}, defaultTopic :根据每条event,以指定字段值为topic,若无,则用默认值
	KeyKafkaClientId = "kafka_client_id" //客户端ID
	//KeyKafkaFlushNum = "kafka_flush_num"				//缓冲条数
	//KeyKafkaFlushFrequency = "kafka_flush_frequency"	//缓冲频率
	KeyKafkaRetryMax    = "kafka_retry_max"   //最大重试次数
	KeyKafkaCompression = "kafka_compression" //压缩模式,有none, gzip, snappy
	KeyKafkaTimeout     = "kafka_timeout"     //连接超时时间
	KeyKafkaKeepAlive   = "kafka_keep_alive"  //保持连接时长
	KeyMaxMessageBytes  = "max_message_bytes" //每条消息最大字节数
)

var (
	compressionModes = map[string]sarama.CompressionCodec{
		KeyKafkaCompressionNone:   sarama.CompressionNone,
		KeyKafkaCompressionGzip:   sarama.CompressionGZIP,
		KeyKafkaCompressionSnappy: sarama.CompressionSnappy,
	}
)

func NewKafkaSender(conf conf.MapConf) (sender Sender, err error) {
	hosts, err := conf.GetStringList(KeyKafkaHost)
	if err != nil {
		return
	}
	topic, err := conf.GetStringList(KeyKafkaTopic)
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
	clientID, _ := conf.GetStringOr(KeyKafkaClientId, hostName)
	//num, _ := conf.GetIntOr(KeyKafkaFlushNum, 200)
	//frequency, _ := conf.GetIntOr(KeyKafkaFlushFrequency, 5)
	retryMax, _ := conf.GetIntOr(KeyKafkaRetryMax, 3)
	compression, _ := conf.GetStringOr(KeyKafkaCompression, KeyKafkaCompressionNone)
	timeout, _ := conf.GetStringOr(KeyKafkaTimeout, "30s")
	keepAlive, _ := conf.GetStringOr(KeyKafkaKeepAlive, "0")
	maxMessageBytes, _ := conf.GetIntOr(KeyMaxMessageBytes, 4*1024*1024)

	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("kafkaSender:(kafkaUrl:%s,topic:%s)", hosts, topic))
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

	sender = newKafkaSender(name, hosts, topic, cfg, producer)
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
	var errorNum int
	producer := this.producer
	var msgs []*sarama.ProducerMessage
	se := &StatsError{}
	var lastErr error
	for _, doc := range data {
		message, err := this.getEventMessage(doc)
		if err != nil {
			log.Debugf("Dropping event: %v", err)
			errorNum++
			lastErr = err
			continue
		}
		msgs = append(msgs, message)
	}
	err := producer.SendMessages(msgs)
	if err != nil {
		errorNum = len(data)
		se.Errors += int64(errorNum)
		se.ErrorDetail = err
		log.Error(err)
		return se
	}
	se.Errors += int64(errorNum)
	se.Success += int64(len(data) - errorNum)
	if lastErr != nil {
		se.LastError = lastErr.Error()
	}
	return se
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
