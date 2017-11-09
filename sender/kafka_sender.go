package sender

import (
	"github.com/qiniu/logkit/conf"
	"github.com/Shopify/sarama"
	"time"
	"fmt"
	"github.com/qiniu/log"
	"encoding/json"
	"github.com/qiniu/logkit/utils"
	"os"
	"strings"
)

type KafkaSender struct {
	name  			string
	hosts  			[]string
	topic 			[]string
	cfg 			*sarama.Config

	producer 		sarama.SyncProducer
}

const (
	KeyKafkaHost = "kafka_host"                  		//主机地址,可以有多个
	KeyKafkaTopic = "kafka_topic"						//topic 1.填一个值,则topic为所填值 2.天两个值: %{[字段名]}, defaultTopic :根据每条event,以指定字段值为topic,若无,则用默认值
	KeyKafkaClientId = "kafka_client_id"				//客户端ID
	//KeyKafkaFlushNum = "kafka_flush_num"				//缓冲条数
	//KeyKafkaFlushFrequency = "kafka_flush_frequency"	//缓冲频率
	KeyKafkaRetryMax = "kafka_retry_max"				//最大重试次数
	KeyKafkaCompression = "kafka_compression"			//压缩模式,有none, gzip, snappy
	KeyKafkaTimeout = "kafka_timeout"					//连接超时时间
	KeyKafkaKeepAlive = "kafka_keepAlive"				//保持连接时长
	KeyMaxMessageBytes = "max_message_bytes"			//每条消息最大字节数
)

var (
	compressionModes = map[string]sarama.CompressionCodec{
		"none":   sarama.CompressionNone,
		"gzip":   sarama.CompressionGZIP,
		"snappy": sarama.CompressionSnappy,
	}
)

const (
	// KeyFtStrategyBackupOnly 只在失败的时候进行容错
	KeyKafkaCompressionNone = "none"
	// KeyFtStrategyAlwaysSave 所有数据都进行容错
	KeyKafkaCompressionGzip = "gzip"
	// KeyFtStrategyConcurrent 适合并发发送数据，只在失败的时候进行容错
	KeyKafkaCompressionSnappy = "snappy"
)

func NewKafkaSender(conf conf.MapConf) (sender Sender, err error){
	hosts, err := conf.GetStringList(KeyKafkaHost)
	if err != nil {
		return
	}
	topic, err := conf.GetStringList(KeyKafkaTopic)
	if err != nil{
		return
	}
	topic, err = utils.ExtractField(topic)
	if err != nil {
		return
	}
	hostName, _ := os.Hostname()
	clientID, _ := conf.GetStringOr(KeyKafkaClientId, hostName)
	//num, _ := conf.GetIntOr(KeyKafkaFlushNum, 200)
	//frequency, _ := conf.GetIntOr(KeyKafkaFlushFrequency, 5)
	retryMax, _ := conf.GetIntOr(KeyKafkaRetryMax, 3)
	compression, _ := conf.GetStringOr(KeyKafkaCompression, "none")
	timeout, _ := conf.GetIntOr(KeyKafkaTimeout, 30)
	keepAlive, _ := conf.GetIntOr(KeyKafkaKeepAlive, 0)
	maxMessageBytes, _ := conf.GetIntOr(KeyMaxMessageBytes, 1000000)

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
	//失败重试次数
	cfg.Producer.Retry.Max = retryMax
	//压缩模式
	compressionMode, ok := compressionModes[strings.ToLower(compression)]
	if !ok {
		return nil, fmt.Errorf("Unknown compression mode: '%v'", compression)
	}
	cfg.Producer.Compression = compressionMode
	//连接超时时间
	cfg.Net.DialTimeout = time.Duration(timeout) * time.Second
	//保持连接的时间
	cfg.Net.KeepAlive = time.Duration(keepAlive) * time.Second
	//每条消息最大字节数
	cfg.Producer.MaxMessageBytes = maxMessageBytes
	//每批最大字节数

	producer, err := sarama.NewSyncProducer(hosts, cfg)
	if err != nil {
		return
	}

	sender, err = newKafkaSender(name, hosts, topic, cfg, producer)
	if err != nil {
		return
	}

	return
}

func newKafkaSender(name string, hosts []string, topic []string, cfg *sarama.Config, producer sarama.SyncProducer) (k *KafkaSender, err error){
	k = &KafkaSender{
		name:		name,
		hosts: 		hosts,
		topic:		topic,
		cfg:		cfg,
		producer:	producer,
	}
	return
}

func (this *KafkaSender) Name() string {
	return this.name
}

func (this *KafkaSender) Send(data []Data) (err error){
	producer := this.producer
	var msgs []*sarama.ProducerMessage
	for _, doc := range data {
		message, err := this.getEventMessage(doc)
		if err != nil {
			log.Debugf("Dropping event: %v", err)
			continue
		}
		msgs = append(msgs, message)
	}
	err = producer.SendMessages(msgs)
	if err != nil {
		return err
	}
	return
}

func (kf *KafkaSender) getEventMessage(event map[string]interface{}) (pm *sarama.ProducerMessage, err error){
	var topic string
	if len(kf.topic) == 2 {
		if event[kf.topic[0]] == nil || event[kf.topic[0]] == ""{
			topic = kf.topic[1]
		} else {
			topic = event[kf.topic[0]].(string)
		}
	} else {
		topic = kf.topic[0]
	}
	value, _ := json.Marshal(event)

	pm = &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(value)),
	}
	return
}

func (this *KafkaSender) Close() (err error){
	log.Debugf("closed kafka sender")
	this.producer.Close()
	this.producer = nil
	return nil
}
