package sender

import (
	"github.com/qiniu/logkit/conf"
	"github.com/Shopify/sarama"
	"time"
	"fmt"
	"github.com/qiniu/log"
	"encoding/json"
)

type KafkaSender struct {
	name  			string
	host  			[]string
	topic 			string
	successCount 	int64
	failedCount		int64

	interrupt 		chan string

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


	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("kafkaSender:(kafkaUrl:%s,topic:%s)", hosts, topic))
	config := sarama.NewConfig()
	config.ClientID = KeyKafkaClientId
	config.Producer.Return.Successes = true //必须有这个选项
	config.Producer.Timeout = 5 * time.Second
	//批量发送条数
	config.Producer.Flush.Messages = num
	//批量发送间隔
	config.Producer.Flush.Frequency =  time.Duration(frequency) * time.Second
	//失败重试次数
	config.Producer.Retry.Max = retryMax

	producer, err := sarama.NewAsyncProducer(hosts, config)

	sender = &KafkaSender{
		name:     		name,
		host:     		hosts,
		topic:    		topic,
		producer: 		producer,
		successCount: 	0,
		failedCount:  	0,
		interrupt:      make(chan string, 1),
	}
	go func(p sarama.AsyncProducer, kafkaSender *KafkaSender) {
		errors := p.Errors()
		success := p.Successes()
		close := kafkaSender.interrupt
		for {
			select {
			case err := <-errors:
				if err != nil {
					kafkaSender.failedCount++
					log.Errorf("send to kafka failed: %s", err)
				}
			case <-success:
				kafkaSender.successCount++
			case <-close:
				//log.Error("------------------------------------------close-----------------------------------------------------")
				return
			}
		}
	}(producer, sender.(*KafkaSender))
	return
}

func (this *KafkaSender) Name() string {
	return "//" + this.topic
}

func (this *KafkaSender) Send(data []Data) (err error){
	producer := this.producer
	topic := this.topic

	for _, doc := range data {
		/*for k, v := range doc {
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(k),Value: sarama.StringEncoder(v.(string))}
		}*/
		value, _ := json.Marshal(doc)
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(string(value))}

	}

	return nil
}

func (this *KafkaSender) Close() (err error){
	this.producer.Close()
	this.interrupt <- "close"
	return nil
}

