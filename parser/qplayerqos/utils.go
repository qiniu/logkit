package qplayerqos

import (
	"fmt"
	"strconv"
	"time"

	"github.com/linkedin/goavro"
)

// Message kafka中的一条消息。
type Message struct {
	Service     string
	Topic       string
	TimestampMS int64
	Body        []byte
}

const (
	avroSchema = `{"type":"record","name":"AvroFlumeEvent","namespace":"org.apache.flume.source.avro","fields":[{"name":"headers","type":{"type":"map","values":"string"}},{"name":"body","type":"bytes"}]}`
)

var avroCodec *goavro.Codec

func decodeMessage(rawMsg []byte) (*Message, error) {

	record, _, err := avroCodec.NativeFromBinary(rawMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode from avro, error %v", err)
	}

	recordMap, ok := record.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not a record")
	}
	headers, ok := recordMap["headers"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("headers field is not a map: value %v, type %T", recordMap["headers"], recordMap["headers"])
	}
	service, ok := headers["service"].(string)
	if !ok {
		return nil, fmt.Errorf("headers: value for 'service' is %#v, type %T", headers["service"], headers["service"])
	}
	topic, ok := headers["topic"].(string)
	if !ok {
		return nil, fmt.Errorf("headers: value for 'topic' is %#v, type %T", headers["topic"], headers["topic"])
	}
	timestamp, ok := headers["timestamp"].(string)
	if !ok {
		return nil, fmt.Errorf("headers: value for 'timestamp' is %v, type %T", headers["timestamp"], headers["timestamp"])
	}
	timestampMS, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("timestamp %s is not an integer", timestamp)
	}

	body, ok := recordMap["body"].([]byte)
	if !ok {
		return nil, fmt.Errorf("'body' field is not in type bytes: value %#v, type %T", recordMap["body"], recordMap["body"])
	}
	return &Message{
		Service:     service,
		Topic:       topic,
		TimestampMS: timestampMS,
		Body:        body,
	}, nil
}

func buildTime(s string) time.Time {
	var ret time.Time
	t, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		switch len(s) {
		case 10:
			// 10位，按秒为单位计算。
			ret = time.Unix(t, 0)
		case 13:
			// 13位，按毫秒为单位计算。
			ret = time.Unix(t/1000, (t%1000)*1000*1000)
		case 16:
			// 16位，按微秒位单位计算。
			ret = time.Unix(t/(1000*1000), t%(1000*1000)*1000)
		case 19:
			// 19位，按纳秒单位计算。
			ret = time.Unix(t/(1000*1000*1000), t%(1000*1000*1000))
		}
	}
	return ret
}
