package config

var DefaultIgnoreFileSuffixes = []string{
	".pid", ".swap", ".go", ".conf", ".tar.gz", ".tar", ".zip",
	".a", ".o", ".so"}

// FileReader's conf keys
const (
	// General
	KeyAuthUsername = "auth_username"
	KeyAuthPassword = "auth_password"

	KeyLogPath           = "log_path"
	KeyMetaPath          = "meta_path"
	KeyFileDone          = "file_done"
	KeyMode              = "mode"
	KeyBufSize           = "reader_buf_size"
	KeyWhence            = "read_from"
	KeyEncoding          = "encoding"
	KeyMysqlEncoding     = "encoding"
	KeyReadIOLimit       = "readio_limit"
	KeyDataSourceTag     = "datasource_tag"
	KeyEncodeTag         = "encode_tag"
	KeyTagFile           = "tag_file"
	KeyHeadPattern       = "head_pattern"
	KeyNewFileNewLine    = "newfile_newline"
	KeySkipFileFirstLine = "skip_first_line"
	KeyReadSameInode     = "read_same_inode"
	KeyInodeSensitive    = "inode_sensitive"

	// 忽略文件路径
	KeyIgnoreLogPath = "ignore_log_path"

	// 忽略隐藏文件
	KeyIgnoreHiddenFile = "ignore_hidden"
	KeyIgnoreFileSuffix = "ignore_file_suffix"
	KeyValidFilePattern = "valid_file_pattern"

	KeyExpire        = "expire"
	KeyExpireDelete  = "expire_delete"
	KeySubmetaExpire = "submeta_expire"
	KeyMaxOpenFiles  = "max_open_files"
	KeyStatInterval  = "stat_interval"
	KeyRunTime       = "run_time"

	KeyMysqlOffsetKey     = "mysql_offset_key"
	KeyMysqlTimestampKey  = "mysql_timestamp_key"
	KeyMysqlStartTime     = "mysql_start_time"
	KeyMysqlBatchDuration = "mysql_batch_intervel"
	KeyMysqlTimestampInt  = "mysql_timestamp_int"
	KeyMysqlReadBatch     = "mysql_limit_batch"
	KeyMysqlDataSource    = "mysql_datasource"
	KeyMysqlDataBase      = "mysql_database"
	KeyMysqlSQL           = "mysql_sql"
	KeyMysqlCron          = "mysql_cron"
	KeyMysqlExecOnStart   = "mysql_exec_onstart"
	KeyMysqlHistoryAll    = "mysql_history_all"
	KyeMysqlTable         = "mysql_table"

	KeySQLSchema        = "sql_schema"
	KeyMagicLagDuration = "magic_lag_duration"

	KeyMssqlOffsetKey   = "mssql_offset_key"
	KeyMssqlReadBatch   = "mssql_limit_batch"
	KeyMssqlDataSource  = "mssql_datasource"
	KeyMssqlDataBase    = "mssql_database"
	KeyMssqlSchema      = "mssql_schema"
	KeyMssqlSQL         = "mssql_sql"
	KeyMssqlCron        = "mssql_cron"
	KeyMssqlExecOnStart = "mssql_exec_onstart"

	KeyPGsqlOffsetKey   = "postgres_offset_key"
	KeyPGtimestampKey   = "postgres_timestamp_key"
	KeyPGStartTime      = "postgres_start_time"
	KeyPGBatchDuration  = "postgres_batch_intervel"
	KeyPGtimestampInt   = "postgres_timestamp_int"
	KeyPGsqlReadBatch   = "postgres_limit_batch"
	KeyPGsqlDataSource  = "postgres_datasource"
	KeyPGsqlDataBase    = "postgres_database"
	KeyPGsqlSchema      = "postgres_schema"
	KeyPGsqlSQL         = "postgres_sql"
	KeyPGsqlCron        = "postgres_cron"
	KeyPGsqlExecOnStart = "postgres_exec_onstart"

	KeyESReadBatch   = "es_limit_batch"
	KeyESIndex       = "es_index"
	KeyESType        = "es_type"
	KeyESHost        = "es_host"
	KeyESKeepAlive   = "es_keepalive"
	KeyESVersion     = "es_version"
	KeyESCron        = "es_cron"
	KeyESExecOnstart = "es_exec_onstart"
	KeyESCronOffset  = "es_cron_offset"
	KeyESDateShift   = "es_date_shift"
	KeyESDateOffset  = "es_date_offset"

	KeyMongoHost        = "mongo_host"
	KeyMongoDatabase    = "mongo_database"
	KeyMongoCollection  = "mongo_collection"
	KeyMongoOffsetKey   = "mongo_offset_key"
	KeyMongoReadBatch   = "mongo_limit_batch"
	KeyMongoCron        = "mongo_cron"
	KeyMongoExecOnstart = "mongo_exec_onstart"
	KeyMongoFilters     = "mongo_filters"
	KeyMongoCert        = "mongo_cacert"

	KeyKafkaGroupID          = "kafka_groupid"
	KeyKafkaTopic            = "kafka_topic"
	KeyKafkaZookeeper        = "kafka_zookeeper"
	KeyKafkaZookeeperChroot  = "kafka_zookeeper_chroot"
	KeyKafkaZookeeperTimeout = "kafka_zookeeper_timeout"
	KeyKafkaMaxProcessTime   = "kafka_maxprocessing_time"

	KeyScriptParams      = "script_params"
	KeyScriptContent     = "script_content"
	KeyExecInterpreter   = "script_exec_interprepter"
	KeyScriptCron        = "script_cron"
	KeyScriptExecOnStart = "script_exec_onstart"

	KeyErrDirectReturn = "errDirectReturn"

	KeyHttpExecOnStart     = "http_exec_onstart"
	KeyHttpCron            = "http_cron"
	KeyHttpMethod          = "http_method"
	KeyHttpHeaders         = "http_headers"
	KeyHttpPageSize        = "http_page_size"
	KeyHttpPageNo          = "http_page_no"
	KeyHttpBody            = "http_body"
	KeyHttpDialTimeout     = "http_dial_timeout"
	KeyHttpResponseTimeout = "http_response_timeout"
)

// Constants for cloudtrail
const (
	KeyS3Region    = "s3_region"
	KeyS3AccessKey = "s3_access_key"
	KeyS3SecretKey = "s3_secret_key"
	KeyS3Bucket    = "s3_bucket"
	KeyS3Prefix    = "s3_prefix"
	KeyS3DatePrefix    = "s3_date_prefix"
	KeyS3PrefixDelimiter    = "s3_prefix_delimiter"

	KeySyncDirectory  = "sync_directory"
	KeySyncMetastore  = "sync_metastore"
	KeySyncInterval   = "sync_interval"
	KeySyncConcurrent = "sync_concurrent"
	KeySyncRangeLeft = "sync_range_left"
)

// Constants for cloudwatch
const (
	KeyRegion = "region"

	/*
		认证顺序：
		1. role_arn
		2. ak,sk
		3. profile
		4. 环境变量
		5. shared_credential_file
		6. EC2 instance profile
	*/
	KeyRoleArn              = "role_arn"
	KeyAWSAccessKey         = "aws_access_key"
	KeyAWSSecretKey         = "aws_secret_key"
	KeyAWSToken             = "aws_token"
	KeyAWSProfile           = "aws_profile"
	KeySharedCredentialFile = "shared_credential_file"
	KeyCollectInterval      = "interval"
	KeyNamespace            = "namespace"
	KeyRateLimit            = "ratelimit"
	KeyMetrics              = "metrics"
	KeyDimension            = "dimensions"
	KeyCacheTTL             = "cache_ttl"
	KeyPeriod               = "period"
	KeyDelay                = "delay"
)

// Constants for Elastic
const (
	ElasticVersion3 = "3.x"
	ElasticVersion5 = "5.x"
	ElasticVersion6 = "6.x"
)

// Constants for HTTP
const (
	KeyHTTPServiceAddress = "http_service_address"
	KeyHTTPServicePath    = "http_service_path"

	DefaultHTTPServiceAddress = ":4000"
	DefaultHTTPServicePath    = "/logkit/data"
)

// Constants for Redis
const (
	DataTypeHash          = "hash"
	DataTypeSortedSet     = "zset"
	DataTypeSet           = "set"
	DataTypeString        = "string"
	DataTypeList          = "list"
	DataTypeChannel       = "channel"
	DataTypePatterChannel = "pattern_channel"

	KeyRedisDataType   = "redis_datatype" // 必填
	KeyRedisDB         = "redis_db"       //默认 是0
	KeyRedisKey        = "redis_key"      //必填
	KeyRedisHashArea   = "redisHash_area"
	KeyRedisAddress    = "redis_address" // 默认127.0.0.1:6379
	KeyRedisPassword   = "redis_password"
	KeyTimeoutDuration = "redis_timeout"
)

const (
	SocketRulePacket      = "按原始包读取"
	SocketRuleJson        = "按json格式读取"
	SocketRuleLine        = "按换行符读取"
	SocketRuleHeadPattern = "按行首正则读取"
)

// Constants for SNMP
const (
	KeySnmpReaderAgents    = "snmp_agents"
	KeySnmpTableInitHost   = "snmp_table_init_host"
	KeySnmpReaderTimeOut   = "snmp_time_out"
	KeySnmpReaderInterval  = "snmp_interval"
	KeySnmpReaderRetries   = "snmp_retries"
	KeySnmpReaderVersion   = "snmp_version"
	KeySnmpReaderCommunity = "snmp_community"

	KeySnmpReaderMaxRepetitions = "snmp_max_repetitions"

	KeySnmpReaderContextName           = "snmp_context_name"
	KeySnmpReaderSecLevel              = "snmp_sec_level"
	KeySnmpReaderSecName               = "snmp_sec_name"
	KeySnmpReaderAuthProtocol          = "snmp_auth_protocol"
	KeySnmpReaderAuthPassword          = "snmp_auth_password"
	KeySnmpReaderPrivProtocol          = "snmp_priv_protocol"
	KeySnmpReaderPrivPassword          = "snmp_priv_password"
	KeySnmpReaderEngineID              = "snmp_engine_id"
	KeySnmpReaderEngineBoots           = "snmp_engine_boots"
	KeySnmpReaderEngineTime            = "snmp_engine_time"
	KeySnmpReaderTables                = "snmp_tables"
	KeySnmpReaderName                  = "snmp_reader_name"
	KeySnmpReaderFields                = "snmp_fields"
	SnmpReaderAuthProtocolMd5          = "MD5"
	SnmpReaderAuthProtocolSha          = "SHA"
	SnmpReaderAuthProtocolNoAuth       = "NoAuth"
	SnmpReaderAuthProtocolDes          = "DES"
	SnmpReaderAuthProtocolAes          = "AES"
	SnmpReaderAuthProtocolNoPriv       = "NoPriv"
	SnmpReaderAuthProtocolNoAuthNoPriv = "noAuthNoPriv"
	SnmpReaderAuthProtocolAuthNoPriv   = "authNoPriv"
	SnmpReaderAuthProtocolAuthPriv     = "authPriv"

	KeySnmpTableName = "snmp_table"
	KeyTimestamp     = "timestamp"
)

// Constants for Socket
const (
	// 监听的url形式包括：
	// socket_service_address = "tcp://:3110"
	// socket_service_address = "tcp://127.0.0.1:http"
	// socket_service_address = "tcp4://:3110"
	// socket_service_address = "tcp6://:3110"
	// socket_service_address = "tcp6://[2001:db8::1]:3110"
	// socket_service_address = "udp://:3110"
	// socket_service_address = "udp4://:3110"
	// socket_service_address = "udp6://:3110"
	// socket_service_address = "unix:///tmp/sys.sock"
	// socket_service_address = "unixgram:///tmp/sys.sock"
	KeySocketServiceAddress  = "socket_service_address"
	KeySocketSplitByLine     = "socket_split_by_line"
	KeySocketRule            = "socket_rule"
	KeySocketRuleHeadPattern = "head_pattern"

	// 最大并发连接数
	// 仅用于 stream sockets (e.g. TCP).
	// 0 (default) 为无限制.
	// socket_max_connections = 1024
	KeySocketMaxConnections = "socket_max_connections"

	// 读的超时时间
	// 仅用于 stream sockets (e.g. TCP).
	// 0 (default) 为没有超时
	// socket_read_timeout = "30s"
	KeySocketReadTimeout = "socket_read_timeout"

	// Socket的Buffer大小，默认65535
	// socket_read_buffer_size = 65535
	KeySocketReadBufferSize = "socket_read_buffer_size"

	// TCP连接的keep_alive时长
	// 0 表示关闭keep_alive
	// 默认5分钟
	KeySocketKeepAlivePeriod = "socket_keep_alive_period"
)

// FileReader's modes
const (
	ModeExtract    = "extract"
	ModeDir        = "dir"  // ModeDir 按时间顺序顺次读取文件夹下所有文件的模式
	ModeFile       = "file" // FileMode 读取单个文件模式
	ModeTailx      = "tailx"
	ModeFileAuto   = "fileauto"
	ModeDirx       = "dirx"
	ModeMySQL      = "mysql"
	ModeMSSQL      = "mssql"
	ModePostgreSQL = "postgres"
	ModeElastic    = "elastic"
	ModeMongo      = "mongo"
	ModeKafka      = "kafka"
	ModeRedis      = "redis"
	ModeSocket     = "socket"
	ModeHTTP       = "http"
	ModeHTTPFETCH  = "httpfetch"
	ModeScript     = "script"
	ModeSnmp       = "snmp"
	ModeCloudWatch = "cloudwatch"
	ModeCloudTrail = "cloudtrail"
	ModeCloudTrailV2 = "cloudtrailv2"
)

const (
	ReadModeHeadPatternString = "mode_head_pattern_string"
	ReadModeHeadPatternRegexp = "mode_head_pattern_regexp"

	ReadModeRunTimeString = "mode_run_time_string"
	ReadModeRunTimeStruct = "mode_run_time_struct"
)

// KeyWhence 的可选项
const (
	WhenceOldest = "oldest"
	WhenceNewest = "newest"
)

const (
	Loop = "loop"
)

const (
	StatusInit int32 = iota
	StatusStopped
	StatusStopping
	StatusRunning
)
