package models

const (
	// pandora_sender key, 可选参数 当sender_type 为pandora 的时候，需要必填的字段
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

	KeyPandoraEnableLogDB = "pandora_enable_logdb"
	KeyPandoraLogDBName   = "pandora_logdb_name"
	KeyPandoraLogDBHost   = "pandora_logdb_host"

	KeyPandoraEnableTSDB     = "pandora_enable_tsdb"
	KeyPandoraTSDBName       = "pandora_tsdb_name"
	KeyPandoraTSDBSeriesName = "pandora_tsdb_series_name"
	KeyPandoraTSDBSeriesTags = "pandora_tsdb_series_tags"
	KeyPandoraTSDBHost       = "pandora_tsdb_host"
	KeyPandoraTSDBTimeStamp  = "pandora_tsdb_timestamp"

	KeyPandoraEnableKodo         = "pandora_enable_kodo"
	KeyPandoraKodoBucketName     = "pandora_bucket_name"
	KeyPandoraKodoFilePrefix     = "pandora_kodo_prefix"
	KeyPandoraKodoCompressPrefix = "pandora_kodo_compress"

	KeyPandoraEmail = "qiniu_email"

	KeyRequestRateLimit       = "request_rate_limit"
	KeyFlowRateLimit          = "flow_rate_limit"
	KeyPandoraGzip            = "pandora_gzip"
	KeyPandoraUUID            = "pandora_uuid"
	KeyPandoraWithIP          = "pandora_withip"
	KeyForceMicrosecond       = "force_microsecond"
	KeyForceDataConvert       = "pandora_force_convert"
	KeyPandoraAutoConvertDate = "pandora_auto_convert_date"
	KeyIgnoreInvalidField     = "ignore_invalid_field"
	KeyPandoraUnescape        = "pandora_unescape"

	PandoraUUID = "Pandora_UUID"

	TimestampPrecision = 19

	// Sender's conf keys
	KeySenderType        = "sender_type"
	KeyFaultTolerant     = "fault_tolerant"
	KeyName              = "name"
	KeyRunnerName        = "runner_name"
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

	InnerUserAgent = "_useragent"
)
