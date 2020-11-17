package qplayerqos

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkedin/goavro"
	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	parserconfig "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/utils/models"
)

func init() {
	var err error
	avroCodec, err = goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatal("create avro codec failed:", err)
	}
	parser.RegisterConstructor(parserconfig.TypeQPlayerQos, NewParser)
	log.Infof("parser type %s registered", parserconfig.TypeQPlayerQos)
}

// KafkaQPlayerQosParser 解析kafka 中播放器中上报的Qos数据。
type KafkaQPlayerQosParser struct {
	name         string
	outputFormat string
	// kvFieldSplitter KV格式输出时，字段之间的分隔符。
	kvFieldSplitter string
	// kvValueSplitter KV格式输出时，key与value之间的分隔符。
	kvValueSplitter string
	decodeBuf       []byte
	eventSpecs      map[string]*EventSchema
}

// 配置文件中使用的key.
const (
	KeyEventTypes   = "event_types"
	KeyMinFieldNum  = "min_field_num"
	KeyMaxFieldNum  = "max_field_num"
	KeyFieldSpecs   = "fields"
	KeyOutputFormat = "output_format"
	// KV格式输出时，字段之间的分隔符。
	KeyOutputKVFieldSplitter = "output_kv_field_splitter"
	// KV格式输出时，key与value之间的分隔符。
	KeyOutputKVValueSplitter = "output_kv_value_splitter"
)

// 日志中内容的输出格式。
const (
	// OutputJSON 直接以JSON格式输出。
	OutputJSON = "json"
	// OutputRawJSON JSON序列化之后填入_raw字段。
	OutputRawJSON = "raw_json"
	// OutputRawKV 按照Key-Value格式序列化后填入raw字段。
	OutputRawKV = "raw_kv"
)

// 默认参数。
const (
	DefaultMinFields       = 2
	DefaultMaxFields       = 100
	DefaultKVFieldSplitter = "\t"
	DefaultKVValueSplitter = "="
)

// NewParser 创建解析器。
func NewParser(c conf.MapConf) (parser.Parser, error) {
	return NewKafkaQplayerQosParser(c)
}

// NewKafkaQplayerQosParser 创建解析器，解析从kafka读取的播放器qos数据。
func NewKafkaQplayerQosParser(c conf.MapConf) (*KafkaQPlayerQosParser, error) {
	name, _ := c.GetStringOr(parserconfig.KeyParserName, "")
	eventTypes, _ := c.GetStringListOr(KeyEventTypes, DefaultEventTypes)
	log.Debugf("===event types: %v===", eventTypes)
	p := &KafkaQPlayerQosParser{
		name:       name,
		eventSpecs: map[string]*EventSchema{},
	}
	// load event specs from default event specs.
	for _, eventType := range eventTypes {
		eventSchema, ok := DefaultEventSpecs[eventType]
		if ok {
			log.Infof("load event spec %s from default event specs", eventType)
			p.eventSpecs[eventType] = eventSchema
		}
	}
	// load event specs from config.
	for _, eventType := range eventTypes {
		if p.eventSpecs[eventType] == nil {
			p.eventSpecs[eventType] = &EventSchema{
				Type:        eventType,
				MinFieldNum: DefaultMinFields,
				MaxFieldNum: DefaultMaxFields,
				FieldSpecs:  map[int]*FieldSpec{},
			}
		}
		minFieldNum, err := c.GetInt(eventType + "." + KeyMinFieldNum)
		if err == nil {
			p.eventSpecs[eventType].MinFieldNum = minFieldNum
		}
		maxFieldNum, err := c.GetInt(eventType + "." + KeyMaxFieldNum)
		if err == nil {
			p.eventSpecs[eventType].MaxFieldNum = maxFieldNum
		}
		if p.eventSpecs[eventType].MaxFieldNum < p.eventSpecs[eventType].MinFieldNum {
			return nil, fmt.Errorf("event type %s: max field num < min field num", eventType)
		}
		// field spec format: <column_number>:<key_name>=<value_type>[,<column_number>:<key_name>=<value_type>]
		// value_type can be: string, bool, int, float, timestamp (string can be omitted)
		// example 0:k0,1:k1,2:k2=int
		fields, _ := c.GetStringListOr(eventType+"."+KeyFieldSpecs, []string{})
		for _, field := range fields {
			parts := strings.SplitN(field, ":", 2)
			if len(parts) != 2 {
				log.Debugf("invalid field spec format:%s", field)
				continue
			}
			colNum, err := strconv.Atoi(parts[0])
			if err != nil {
				log.Debugf("invalid column number %s in field spec format:%s", parts[0], field)
				continue
			}
			var fieldKey string
			var fieldType FieldType
			fieldKVParts := strings.SplitN(parts[1], "=", 2)
			if len(fieldKVParts) == 2 {
				fieldKey = fieldKVParts[0]
				fieldType = FieldType(fieldKVParts[1])
			} else {
				fieldKey = fieldKVParts[0]
				fieldType = FieldTypeString
			}
			p.eventSpecs[eventType].FieldSpecs[colNum] = &FieldSpec{
				Key:  fieldKey,
				Type: fieldType,
			}
			log.Debugf("event type %s, colums %d: key name %s, type %s", eventType, colNum, fieldKey, fieldType)
		}
	}

	for eventType, eventSchema := range p.eventSpecs {
		log.Infof("event type %s, min fields %d, max fields %d, %d fields specified", eventType, eventSchema.MinFieldNum, eventSchema.MaxFieldNum, len(eventSchema.FieldSpecs))
	}
	// get output config.
	p.outputFormat, _ = c.GetStringOr(KeyOutputFormat, OutputJSON)
	if p.outputFormat == OutputRawKV {
		p.kvFieldSplitter, _ = c.GetStringOr(KeyOutputKVFieldSplitter, "\t")
		p.kvValueSplitter, _ = c.GetStringOr(KeyOutputKVValueSplitter, "=")
	}
	return p, nil
}

// FieldType 字段的类型。
type FieldType string

// 可支持的字段类型。包括字符串、整数、浮点数、布尔型、时间戳。
const (
	FieldTypeString FieldType = "string"
	FieldTypeBool   FieldType = "bool"
	FieldTypeInt    FieldType = "int"
	FieldTypeFloat  FieldType = "float"
	// FieldTypeTimestamp 时间戳格式。这种格式将会尝试解析时间戳，并以RFC 3339输出。
	FieldTypeTimestamp FieldType = "timestamp"
)

// FieldSpec 播放器上报字段的格式。
type FieldSpec struct {
	// Key 该字段对应的key名称。若为空，该字段不被上报。
	Key string `json:"key"`
	// Type 该字段对应的类型。支持的类型包括：字符串、整数、浮点数、时间。
	Type FieldType `json:"type"`
}

// EventSchema 播放器上报事件的格式。
type EventSchema struct {
	// 事件的类型。
	Type string `json:"type"`
	// MinFieldNum 至少应具有的字段数。
	MinFieldNum int `json:"min_field_num"`
	// MaxFieldNum 至多的字段数，多出的字段将被丢弃。
	MaxFieldNum int `json:"max_field_num"`
	// FieldSpecs 各个字段对应的描述，包括key与字段类型。
	FieldSpecs map[int]*FieldSpec `json:"fields"`
}

// Name 返回parser的名称。
func (p *KafkaQPlayerQosParser) Name() string {
	return p.name
}

// Parse 解析成结构化的结果。
func (p *KafkaQPlayerQosParser) Parse(lines []string) (datas []models.Data, err error) {
	for _, line := range lines {
		p.decodeBuf = append(p.decodeBuf, []byte(line)...)
		msg, err := decodeMessage(p.decodeBuf)
		if err != nil {
			log.Debugf("decode from avro failed, error %v", err)
			continue
		}
		p.decodeBuf = []byte{}
		bodyParts := strings.Split(string(msg.Body), "\t")
		if len(bodyParts) < 2 {
			log.Debugf("line has %d parts, at least 2", len(bodyParts))
			continue
		}
		eventType := bodyParts[1]
		eventSchema, ok := p.eventSpecs[eventType]
		if !ok {
			// 未注册的事件类型，忽略此行。
			log.Debugf("unsupported event type %s", eventType)
			continue
		}
		if len(bodyParts) < eventSchema.MinFieldNum {
			// 列数少于事件的最少字段数，忽略此行。
			log.Debugf("columns %d, at least %d for event type %s", len(bodyParts), eventSchema.MinFieldNum, eventType)
			continue
		}
		data := models.Data{}
		receiveTime := time.Unix(msg.TimestampMS/1000, (msg.TimestampMS%1000)*1000*1000)
		data["server_time"] = receiveTime.Format(time.RFC3339)
		for i, value := range bodyParts {
			if i >= eventSchema.MaxFieldNum {
				// 列数超出指定的最大字段数，忽略之后的所有列。
				break
			}
			fieldSpec := eventSchema.FieldSpecs[i]
			if fieldSpec == nil || fieldSpec.Key == "" {
				// 未给此列指定字段对应的key，忽略此列。
				continue
			}

			key := fieldSpec.Key
			switch fieldSpec.Type {
			case FieldTypeString:
				data[key] = value
			case FieldTypeBool:
				if value == "true" {
					data[key] = true
				} else if value == "false" {
					data[key] = false
				}
			case FieldTypeInt:
				valueInt, _ := strconv.ParseInt(value, 10, 64)
				data[key] = valueInt
			case FieldTypeFloat:
				valueFloat, _ := strconv.ParseFloat(value, 64)
				data[key] = valueFloat
			case FieldTypeTimestamp:
				valueTime := buildTime(value)
				data[key] = valueTime.Format(time.RFC3339)
			}
		}
		// output.
		switch p.outputFormat {
		case OutputRawJSON:
			buf, err := json.Marshal(data)
			if err == nil {
				datas = append(datas, models.Data{"_raw": string(buf)})
			}
		case OutputRawKV:
			fields := make([]string, 0, len(data))
			for k, v := range data {
				fields = append(fields, fmt.Sprintf("%s%s%v", k, p.kvValueSplitter, v))
			}
			datas = append(datas, models.Data{"_raw": strings.Join(fields, p.kvFieldSplitter)})
		case OutputJSON:
			datas = append(datas, data)
		default:
			datas = append(datas, data)
		}
	}
	return datas, nil
}
