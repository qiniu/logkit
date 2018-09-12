package kafka

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

var _ sender.SkipDeepCopySender = &Sender{}

type Sender struct {
	name  string
	hosts []string
	topic []string
	cfg   *sarama.Config

	lastError error //用于防止所有的错误都被 kafka熔断的错误提示刷掉
	producer  sarama.SyncProducer
}

var (
	compressionModes = map[string]sarama.CompressionCodec{
		sender.KeyKafkaCompressionNone:   sarama.CompressionNone,
		sender.KeyKafkaCompressionGzip:   sarama.CompressionGZIP,
		sender.KeyKafkaCompressionSnappy: sarama.CompressionSnappy,
	}
)

func init() {
	sender.RegisterConstructor(sender.TypeKafka, NewSender)
}

// kafka sender
func NewSender(conf conf.MapConf) (kafkaSender sender.Sender, err error) {
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

	kafkaSender = newSender(name, hosts, topic, cfg, producer)
	return
}

func newSender(name string, hosts []string, topic []string, cfg *sarama.Config, producer sarama.SyncProducer) (k *Sender) {
	k = &Sender{
		name:     name,
		hosts:    hosts,
		topic:    topic,
		cfg:      cfg,
		producer: producer,
	}
	return
}

func (this *Sender) Name() string {
	return this.name
}

func (this *Sender) Send(data []Data) error {
	var (
		producer        = this.producer
		msgs            []*sarama.ProducerMessage
		ss              = &StatsError{}
		ignoreDataCount int
	)
	for _, doc := range data {
		message, err := this.getEventMessage(doc)
		if err != nil {
			log.Debugf("Dropping event: %v", err)
			ss.AddErrors()
			ss.LastError = err.Error()
			ignoreDataCount++
			continue
		}
		msgs = append(msgs, message)
	}
	err := producer.SendMessages(msgs)
	if err != nil {
		ss.AddErrorsNum(len(msgs))
		pde, ok := err.(sarama.ProducerErrors)
		if !ok {
			if ss.LastError != "" {
				ss.LastError = fmt.Sprintf("ignore %d datas, last error: %s", ignoreDataCount, ss.LastError) + "\n"
			}
			ss.LastError += err.Error()
			ss.ErrorDetail = err
			return ss
		}

		var allcir = true
		for _, v := range pde {
			//对于熔断的错误提示，没有任何帮助，过滤掉
			if strings.Contains(v.Error(), "circuit breaker is open") {
				continue
			}
			allcir = false
			ss.ErrorDetail = fmt.Errorf("%v detail: %v", ss.ErrorDetail, v.Error())
			this.lastError = v
			//发送错误为message too large时，启用二分策略重新发送
			if v.Err == sarama.ErrMessageSizeTooLarge {
				ss.ErrorDetail = reqerr.NewSendError("Sender[Kafka]:Message was too large, server rejected it to avoid allocation error", sender.ConvertDatasBack(data), reqerr.TypeBinaryUnpack)
			}
			break
		}

		if allcir {
			ss.ErrorDetail = fmt.Errorf("%v, all error is circuit breaker is open , last error %v", err, this.lastError)
		}
		if ss.LastError != "" {
			ss.LastError = fmt.Sprintf("ignore %d datas, last error: %s", ignoreDataCount, ss.LastError) + "\n"
		}
		ss.LastError = ss.ErrorDetail.Error()
		return ss
	}
	ss.AddSuccessNum(len(msgs))
	//本次发送成功, lastError 置为 nil
	this.lastError = nil
	return ss
}

func (kf *Sender) getEventMessage(event map[string]interface{}) (pm *sarama.ProducerMessage, err error) {
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

func (this *Sender) Close() (err error) {
	log.Infof("kafka sender was closed")
	this.producer.Close()
	this.producer = nil
	return nil
}

func (_ *Sender) SkipDeepCopy() bool { return true }
