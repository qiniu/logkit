package sender

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

// pandora
const (
	// pandora key, 可选参数 当sender_type 为pandora 的时候，需要必填的字段
	KeyPandoraAk                   = "pandora_ak"
	KeyPandoraSk                   = "pandora_sk"
	KeyPandoraHost                 = "pandora_host"
	KeyPandoraWorkflowName         = "pandora_workflow_name"
	KeyPandoraRepoName             = "pandora_repo_name"
	KeyPandoraRegion               = "pandora_region"
	KeyPandoraSchema               = "pandora_schema"
	KeyPandoraSchemaUpdateInterval = "pandora_schema_update_interval"
	KeyPandoraAutoCreate           = "pandora_auto_create"
	KeyPandoraSchemaFree           = "pandora_schema_free"
	KeyPandoraExtraInfo            = "pandora_extra_info"

	KeyPandoraEnableLogDB   = "pandora_enable_logdb"
	KeyPandoraLogDBName     = "pandora_logdb_name"
	KeyPandoraLogDBHost     = "pandora_logdb_host"
	KeyPandoraLogDBAnalyzer = "pandora_logdb_analyzer"

	KeyPandoraEnableTSDB     = "pandora_enable_tsdb"
	KeyPandoraTSDBName       = "pandora_tsdb_name"
	KeyPandoraTSDBSeriesName = "pandora_tsdb_series_name"
	KeyPandoraTSDBSeriesTags = "pandora_tsdb_series_tags"
	KeyPandoraTSDBHost       = "pandora_tsdb_host"
	KeyPandoraTSDBTimeStamp  = "pandora_tsdb_timestamp"

	KeyPandoraEnableKodo         = "pandora_enable_kodo"
	KeyPandoraKodoBucketName     = "pandora_bucket_name"
	KeyPandoraKodoFilePrefix     = "pandora_kodo_prefix"
	KeyPandoraKodoLowFreqFile    = "pandora_kodo_low_frequency_file"
	KeyPandoraKodoCompressPrefix = "pandora_kodo_compress"
	KeyPandoraKodoGzip           = "pandora_kodo_gzip"
	KeyPandoraKodoRotateStrategy = "pandora_kodo_rotate_strategy"
	KeyPandoraKodoRotateInterval = "pandora_kodo_rotate_interval"
	KeyPandoraKodoRotateSize     = "pandora_kodo_rotate_size"
	KeyPandoraKodoFileRetention  = "pandora_kodo_file_retention"

	KeyPandoraEmail = "qiniu_email"

	KeyRequestRateLimit       = "request_rate_limit"
	KeyFlowRateLimit          = "flow_rate_limit"
	KeyPandoraGzip            = "pandora_gzip"
	KeyPandoraUUID            = "pandora_uuid"
	KeyPandoraWithIP          = "pandora_withip"
	KeyForceMicrosecond       = "force_microsecond"
	KeyForceDataConvert       = "pandora_force_convert"
	KeyNumberUseFloat         = "number_use_float"
	KeyPandoraAutoConvertDate = "pandora_auto_convert_date"
	KeyIgnoreInvalidField     = "ignore_invalid_field"
	KeyPandoraUnescape        = "pandora_unescape"
	KeyPandoraSendType        = "pandora_send_type"
	KeyInsecureServer         = "insecure_server"
	KeyTimeout                = "pandora_send_timeout"
	KeyPandoraDescription     = "pandora_description"

	PandoraUUID = "Pandora_UUID"

	TimestampPrecision = 19

	// Sender's conf keys
	KeySenderType        = "sender_type"
	KeyFaultTolerant     = "fault_tolerant"
	KeyName              = "name"
	KeyLogkitSendTime    = "logkit_send_time"
	KeyIsMetrics         = "is_metrics"
	KeyMetricTime        = "timestamp"
	UnderfinedRunnerName = "UnderfinedRunnerName"

	// SenderType 发送类型
	TypeFile              = "file"          // 本地文件
	TypePandora           = "pandora"       // pandora 打点
	TypeMongodbAccumulate = "mongodb_acc"   // mongodb 并且按字段聚合
	TypeInfluxdb          = "influxdb"      // influxdb
	TypeMock              = "mock"          // mock sender
	TypeDiscard           = "discard"       // discard sender
	TypeElastic           = "elasticsearch" // elastic
	TypeKafka             = "kafka"         // kafka
	TypeHttp              = "http"          // http sender
	TypeMySQL             = "mysql"
	TypeCSV               = "csv"
	TypeSQLFile           = "sqlfile"

	InnerUserAgent = "_useragent"
)

const (
	// General
	KeyAuthUsername            = "auth_username"
	KeyAuthPassword            = "auth_password"
	KeyEnableGzip              = "enable_gzip"
	DefaultJJHPipelineEndpoint = "http://jjh-pipeline.qiniuapi.com"
	DefaultJJHLogDBEndpoint    = "http://jjh-insight.qiniuapi.com"
	NBRegion                   = "nb"
	JJHRegion                  = "jjh"

	// Elastic
	KeyElasticHost          = "elastic_host"
	KeyElasticVersion       = "elastic_version"
	KeyElasticIndex         = "elastic_index"
	KeyElasticType          = "elastic_type"
	KeyElasticAlias         = "elastic_keys"
	KeyElasticIndexStrategy = "elastic_index_strategy"
	KeyElasticTimezone      = "elastic_time_zone"

	KeyDefaultIndexStrategy = "default"
	KeyYearIndexStrategy    = "year"
	KeyMonthIndexStrategy   = "month"
	KeyDayIndexStrategy     = "day"

	// ElasticVersion3 v3.x
	ElasticVersion3 = "3.x"
	// ElasticVersion5 v5.x
	ElasticVersion5 = "5.x"
	// ElasticVersion6 v6.x
	ElasticVersion6 = "6.x"

	//timeZone
	KeylocalTimezone = "Local"
	KeyUTCTimezone   = "UTC"
	KeyPRCTimezone   = "PRC"

	KeySendTime = "sendTime"

	// fault_tolerant
	// 可选参数 fault_tolerant 为true的话，以下必填
	KeyFtSyncEvery         = "ft_sync_every"          // 该参数设置多少次写入会同步一次offset log
	KeyFtSaveLogPath       = "ft_save_log_path"       // disk queue 数据日志路径
	KeyFtWriteLimit        = "ft_write_limit"         // 写入速度限制，单位MB
	KeyFtStrategy          = "ft_strategy"            // ft 的策略
	KeyFtDiscardErr        = "ft_discard_failed_data" //是否丢弃错误数据
	KeyFtMemoryChannel     = "ft_memory_channel"
	KeyFtMemoryChannelSize = "ft_memory_channel_size"
	KeyFtLongDataDiscard   = "ft_long_data_discard"

	// queue
	KeyMaxDiskUsedBytes = "max_disk_used_bytes"
	KeyMaxSizePerFile   = "max_size_per_file"

	// ft 策略
	// KeyFtStrategyBackupOnly 只在失败的时候进行容错
	KeyFtStrategyBackupOnly = "backup_only"
	// KeyFtStrategyAlwaysSave 所有数据都进行容错
	KeyFtStrategyAlwaysSave = "always_save"
	// KeyFtStrategyConcurrent 适合并发发送数据，只在失败的时候进行容错
	KeyFtStrategyConcurrent = "concurrent"

	// Ft sender默认同步一次meta信息的数据次数
	DefaultFtSyncEvery = 10

	// file
	// 可选参数 当sender_type 为file 的时候
	KeyFileSenderPath         = "file_send_path"
	KeyFileSenderTimestampKey = "file_send_timestamp_key"
	KeyFileSenderMaxOpenFiles = "file_send_max_open_files"

	// http
	KeyHttpSenderUrl      = "http_sender_url"
	KeyHttpSenderGzip     = "http_sender_gzip"
	KeyHttpSenderProtocol = "http_sender_protocol"
	KeyHttpTimeout        = "http_sender_timeout"
	KeyHttpSenderCsvHead  = "http_sender_csv_head"
	KeyHttpSenderCsvSplit = "http_sender_csv_split"
	SendProtocolCSV       = "csv"
	SendProtocolJson      = "json"
	SendProtocolWholeJson = "body_json"
	SendProtocolRaw       = "raw"

	// Influxdb sender 的可配置字段
	KeyInfluxdbHost                  = "influxdb_host"
	KeyInfluxdbDB                    = "influxdb_db"
	KeyInfluxdbAutoCreate            = "influxdb_autoCreate"
	KeyInfluxdbRetetion              = "influxdb_retention"
	KeyInfluxdbRetetionDuration      = "influxdb_retention_duration"
	KeyInfluxdbMeasurement           = "influxdb_measurement"
	KeyInfluxdbTags                  = "influxdb_tags"
	KeyInfluxdbFields                = "influxdb_fields"                  // influxdb
	KeyInfluxdbTimestamp             = "influxdb_timestamp"               // 可选 nano时间戳字段
	KeyInfluxdbTimestampPrecision    = "influxdb_timestamp_precision"     // 时间戳字段的精度，代表时间戳1个单位代表多少纳秒
	KeyInfluxdbIgnoreBeyondRetention = "influxdb_ignore_beyond_retention" // 开启后将忽略超出 retention 时间的点

	// Kafka
	KeyKafkaCompressionNone   = "none"
	KeyKafkaCompressionGzip   = "gzip"
	KeyKafkaCompressionSnappy = "snappy"
	KeyKafkaCompressionLZ4    = "lz4"

	KeyKafkaHost     = "kafka_host"      //主机地址,可以有多个
	KeyKafkaTopic    = "kafka_topic"     //topic 1.填一个值,则topic为所填值 2.天两个值: %{[字段名]}, defaultTopic :根据每条event,以指定字段值为topic,若无,则用默认值
	KeyKafkaClientId = "kafka_client_id" //客户端ID
	//KeyKafkaFlushNum = "kafka_flush_num"				//缓冲条数
	//KeyKafkaFlushFrequency = "kafka_flush_frequency"	//缓冲频率
	KeyKafkaRetryMax                  = "kafka_retry_max"        //最大重试次数
	KeyKafkaCompression               = "kafka_compression"      //压缩模式,有none, gzip, snappy
	KeyKafkaTimeout                   = "kafka_timeout"          //连接超时时间
	KeyKafkaKeepAlive                 = "kafka_keep_alive"       //保持连接时长
	KeyMaxMessageBytes                = "max_message_bytes"      //每条消息最大字节数
	KeyGZIPCompressionLevel           = "gzip_compression_level" //GZIP压缩日志的策略
	KeyGZIPCompressionNo              = "仅打包不压缩"
	KeyGZIPCompressionBestSpeed       = "最快压缩速度"
	KeyGZIPCompressionBestCompression = "最高压缩比"
	KeyGZIPCompressionDefault         = "默认压缩比"
	KeyGZIPCompressionHuffmanOnly     = "哈夫曼压缩"

	// Mongodb
	// 可选参数 当sender_type 为mongodb_* 的时候，需要必填的字段
	KeyMongodbHost       = "mongodb_host"
	KeyMongodbDB         = "mongodb_db"
	KeyMongodbCollection = "mongodb_collection"

	// 可选参数 当sender_type 为mongodb_acc 的时候，需要必填的字段
	KeyMongodbUpdateKey = "mongodb_acc_updkey"
	KeyMongodbAccKey    = "mongodb_acc_acckey"

	KeyMaxSendRate = "max_send_rate"

	KeyMySQLDataSource = "mysql_datasource"
	KeyMySQLTable      = "mysql_table"

	KeyCSVFields     = "csv_fields"
	KeyCSVDelimiter  = "csv_delimiter"
	KeyCSVRotateSize = "csv_rotate_size"
	KeyCSVPathPrefix = "csv_path_prefix"

	KeySQLFileRotateSize = "sqlfile_rotate_size"
	KeySQLFileTable      = "sqlfile_table"
	KeySQLFilePathPrefix = "sqlfile_path_prefix"
)

// NotAsyncSender return when sender is not async
var ErrNotAsyncSender = errors.New("sender does not support for Async Push")

// Sender send data to pandora, prometheus such different destinations
type Sender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
}

// SkipDeepCopySender 表示该 sender 不会对传入数据进行污染，凡是有次保证的 sender 需要实现该接口提升发送效率
type SkipDeepCopySender interface {
	// SkipDeepCopy 需要返回值是因为如果一个 sender 封装了其它 sender，需要根据实际封装的类型返回是否忽略深度拷贝
	SkipDeepCopy() bool
}

type StatsSender interface {
	Name() string
	// send data, error if failed
	Send([]Data) error
	Close() error
	Stats() StatsInfo
	// 恢复 sender 停止之前的状态
	Restore(*StatsInfo)
}

// SenderRegistry sender 的工厂类。可以注册自定义sender
type Registry struct {
	senderTypeMap map[string]func(conf.MapConf) (Sender, error)
}

type Constructor func(conf.MapConf) (Sender, error)

// registeredConstructors keeps a list of all available reader constructors can be registered by Registry.
var registeredConstructors = map[string]Constructor{}

// RegisterConstructor adds a new constructor for a given type of reader.
func RegisterConstructor(typ string, c Constructor) {
	registeredConstructors[typ] = c
}

func NewRegistry() *Registry {
	ret := &Registry{
		senderTypeMap: map[string]func(conf.MapConf) (Sender, error){},
	}

	for typ, c := range registeredConstructors {
		ret.RegisterSender(typ, c)
	}

	return ret
}

func (r *Registry) RegisterSender(senderType string, constructor func(conf.MapConf) (Sender, error)) error {
	_, exist := r.senderTypeMap[senderType]
	if exist {
		return errors.New("senderType " + senderType + " has been existed")
	}
	r.senderTypeMap[senderType] = constructor
	return nil
}

func (r *Registry) NewSender(conf conf.MapConf, ftSaveLogPath string) (sender Sender, err error) {
	sendType, err := conf.GetString(KeySenderType)
	if err != nil {
		return
	}
	constructor, exist := r.senderTypeMap[sendType]
	if !exist {
		return nil, fmt.Errorf("sender type unsupported : %v", sendType)
	}
	sender, err = constructor(conf)
	if err != nil {
		return
	}
	faultTolerant, _ := conf.GetBoolOr(KeyFaultTolerant, true)

	//如果是 PandoraSender，目前的依赖必须启用 ftsender,依赖Ftsender做key转换检查
	if faultTolerant || sendType == TypePandora {
		sender, err = NewFtSender(sender, conf, ftSaveLogPath)
		if err != nil {
			return
		}
	}
	return sender, nil
}

type TokenRefreshable interface {
	TokenRefresh(conf.MapConf) error
}

func ConvertDatas(ins []map[string]interface{}) []Data {
	var datas []Data
	for _, v := range ins {
		datas = append(datas, Data(v))
	}
	return datas
}
func ConvertDatasBack(ins []Data) []map[string]interface{} {
	var datas []map[string]interface{}
	for _, v := range ins {
		datas = append(datas, map[string]interface{}(v))
	}
	return datas
}
