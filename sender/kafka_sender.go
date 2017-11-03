package sender

import (
	"github.com/qiniu/logkit/conf"
	"github.com/Shopify/sarama"
	"time"
	"fmt"
	"github.com/qiniu/log"
	"encoding/json"
	"github.com/qiniu/logkit/utils"
	"sync"
	"regexp"
	"os"
	"strings"
)

type KafkaSender struct {
	name  			string
	hosts  			[]string
	topic 			[]string
	cfg 			*sarama.Config

	wg				sync.WaitGroup

	stats 			utils.StatsInfo

	producer 		sarama.AsyncProducer
}

const (
	KeyKafkaHost = "kafka_host"                  		//主机地址,可以有多个
	KeyKafkaTopic = "kafka_topic"						//topic 1.填一个值,则topic为所填值 2.天两个值: %{[字段名]}, defaultTopic :根据每条event,以指定字段值为topic,若无,则用默认值
	KeyKafkaClientId = "kafka_client_id"				//客户端ID
	KeyKafkaFlushNum = "kafka_flush_num"				//缓冲条数
	KeyKafkaFlushFrequency = "kafka_flush_frequency"	//缓冲频率
	KeyKafkaRetryMax = "kafka_retry_max"				//最大重试次数
	KeyKafkaCompression = "kafka_compression"			//压缩模式,有none, gzip, snappy
	KeyKafkaTimeout = "kafka_timeout"					//连接超时时间
	KeyKafkaKeepAlive = "kafka_keepAlive"				//保持连接时长

)

var (
	compressionModes = map[string]sarama.CompressionCodec{
		"none":   sarama.CompressionNone,
		"gzip":   sarama.CompressionGZIP,
		"snappy": sarama.CompressionSnappy,
	}
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
	num, _ := conf.GetIntOr(KeyKafkaFlushNum, 200)
	frequency, _ := conf.GetIntOr(KeyKafkaFlushFrequency, 5)
	retryMax, _ := conf.GetIntOr(KeyKafkaRetryMax, 3)
	compression, _ := conf.GetStringOr(KeyKafkaCompression, "none")
	timeout, _ := conf.GetIntOr(KeyKafkaTimeout, 30)
	keepAlive, _ := conf.GetIntOr(KeyKafkaKeepAlive, 0)


	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("kafkaSender:(kafkaUrl:%s,topic:%s)", hosts, topic))
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	//客户端ID
	cfg.ClientID = clientID
	//批量发送条数
	cfg.Producer.Flush.Messages = num
	//批量发送间隔
	cfg.Producer.Flush.Frequency =  time.Duration(frequency) * time.Second
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

	producer, err := sarama.NewAsyncProducer(hosts, cfg)
	if err != nil {
		return
	}

	sender, err = newKafkaSender(name, hosts, topic, cfg, producer)
	if err != nil {
		return
	}

	return
}

func newKafkaSender(name string, hosts []string, topic []string, cfg *sarama.Config, producer sarama.AsyncProducer) (k *KafkaSender, err error){
	k = &KafkaSender{
		name:		name,
		hosts: 		hosts,
		topic:		topic,
		cfg:		cfg,
		producer:	producer,
	}
	k.wg.Add(2)
	go k.successWorker(producer.Successes())
	go k.errorWorker(producer.Errors())
	return
}

func (ks *KafkaSender) successWorker(ch <-chan *sarama.ProducerMessage)	{
	defer ks.wg.Done()
	defer log.Debugf("Stop kafka ack worker")
	for range ch {
		ks.stats.Success ++
	}
}

func (ks *KafkaSender) errorWorker(ch <-chan *sarama.ProducerError){
	defer ks.wg.Done()
	defer log.Debugf("Stop kafka error handler")

	for errMsg := range ch{
		ks.stats.Errors ++
		ks.stats.LastError = errMsg.Error()
		log.Debug(errMsg.Error())
	}
}

func (this *KafkaSender) Name() string {
	return this.name
}

func (this *KafkaSender) Send(data []Data) (err error){
	producer := this.producer
	for _, doc := range data {
		message, err := this.getEventMessage(doc)
		if err != nil {
			log.Debugf("Dropping event: %v", err)
			continue
		}
		producer.Input() <- message
	}
	return nil
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
	regexp.Compile("")
	value, _ := json.Marshal(event)

	pm = &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(value)),
	}
	return
}

func (this *KafkaSender) Close() (err error){
	log.Debugf("closed kafka sender")
	this.wg.Wait()
	this.producer.AsyncClose()
	this.producer = nil
	return nil
}

func (ks *KafkaSender) Stats() utils.StatsInfo{
	return ks.stats
}

