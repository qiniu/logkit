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
)

type KafkaSender struct {
	name  			string
	hosts  			[]string
	topic 			string
	cfg 			*sarama.Config

	wg				sync.WaitGroup

	stats 			utils.StatsInfo

	producer 		sarama.AsyncProducer
}

const (
	KeyKafkaHost = "kafka_host"
	KeyKafkaTopic = "kafka_topic"
	KeyKafkaClientId = "kafka_client_id"
	KeyKafkaFlushNum = "kafka_flush_num"
	KeyKafkaFlushFrequency = "kafka_flush_frequency"
	KeyKafkaRetryMax = "kafka_retry_max"
)

func NewKafkaSender(conf conf.MapConf) (sender Sender, err error){
	hosts, err := conf.GetStringList(KeyKafkaHost)
	if err != nil {
		return
	}

	topic, err := conf.GetString(KeyKafkaTopic)
	if err != nil{
		return
	}

	num, _ := conf.GetIntOr(KeyKafkaFlushNum, 10)


	frequency, _ := conf.GetIntOr(KeyKafkaFlushFrequency, 5)

	retryMax, _ := conf.GetIntOr(KeyKafkaRetryMax, 3)

	clientIp, _ := conf.GetStringOr(KeyKafkaClientId, "sarama")

	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("kafkaSender:(kafkaUrl:%s,topic:%s)", hosts, topic))
	cfg := sarama.NewConfig()
	cfg.ClientID = clientIp
	cfg.Producer.Return.Successes = true //必须有这个选项
	cfg.Producer.Timeout = 5 * time.Second
	//批量发送条数
	cfg.Producer.Flush.Messages = num
	//批量发送间隔
	cfg.Producer.Flush.Frequency =  time.Duration(frequency) * time.Second
	//失败重试次数
	cfg.Producer.Retry.Max = retryMax

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

func newKafkaSender(name string, hosts []string, topic string, cfg *sarama.Config, producer sarama.AsyncProducer) (k *KafkaSender, err error){
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
	return "//" + this.topic
}

func (this *KafkaSender) Send(data []Data) (err error){
	producer := this.producer
	for _, doc := range data {
		message, err := this.getEventMessage(doc)
		if err != nil {
			log.Debug(err)
			continue
		}
		producer.Input() <- message
	}
	return nil
}

func (kf *KafkaSender) getEventMessage(event map[string]interface{}) (pm *sarama.ProducerMessage, err error){
	value, _ := json.Marshal(event)
	pm = &sarama.ProducerMessage{
		Topic: kf.topic,
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

