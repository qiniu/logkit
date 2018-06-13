package reader

import (
	"strings"

	. "github.com/qiniu/logkit/utils/models"
)

// Constants for cloudtrail
const (
	KeyS3Region    = "s3_region"
	KeyS3AccessKey = "s3_access_key"
	KeyS3SecretKey = "s3_secret_key"
	KeyS3Bucket    = "s3_bucket"
	KeyS3Prefix    = "s3_prefix"

	KeySyncDirectory  = "sync_directory"
	KeySyncMetastore  = "sync_metastore"
	KeySyncInterval   = "sync_interval"
	KeySyncConcurrent = "sync_concurrent"
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
	DateTypeHash          = "hash"
	DateTypeSortedSet     = "sortedSet"
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

// Constants for SNMP
const (
	KeySnmpReaderAgents    = "snmp_agents"
	KeySnmpReaderTimeOut   = "snmp_time_out"
	KeySnmpReaderInterval  = "snmp_interval"
	KeySnmpReaderRetries   = "snmp_retries"
	KeySnmpReaderVersion   = "snmp_version"
	KeySnmpReaderCommunity = "snmp_community"

	KeySnmpReaderMaxRepetitions = "snmp_max_repetitions"

	KeySnmpReaderContextName  = "snmp_context_name"
	KeySnmpReaderSecLevel     = "snmp_sec_level"
	KeySnmpReaderSecName      = "snmp_sec_name"
	KeySnmpReaderAuthProtocol = "snmp_auth_protocol"
	KeySnmpReaderAuthPassword = "snmp_auth_password"
	KeySnmpReaderPrivProtocol = "snmp_priv_protocol"
	KeySnmpReaderPrivPassword = "snmp_priv_password"
	KeySnmpReaderEngineID     = "snmp_engine_id"
	KeySnmpReaderEngineBoots  = "snmp_engine_boots"
	KeySnmpReaderEngineTime   = "snmp_engine_time"
	KeySnmpReaderTables       = "snmp_tables"
	KeySnmpReaderName         = "snmp_reader_name"
	KeySnmpReaderFields       = "snmp_fields"

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
	KeySocketServiceAddress = "socket_service_address"

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

// ModeUsages 用途说明
var ModeUsages = []KeyValue{
	{ModeFileAuto, "从文件读取( fileauto 模式)"},
	{ModeDir, "从文件读取( dir 模式)"},
	{ModeFile, "从文件读取( file 模式)"},
	{ModeTailx, "从文件读取( tailx 模式)"},
	{ModeMySQL, "从 MySQL 读取"},
	{ModeMSSQL, "从 MSSQL 读取"},
	{ModePostgreSQL, "从 PostgreSQL 读取"},
	{ModeElastic, "从 Elasticsearch 读取"},
	{ModeMongo, "从 MongoDB 读取"},
	{ModeKafka, "从 Kafka 读取"},
	{ModeRedis, "从 Redis 读取"},
	{ModeSocket, "从 Socket 读取"},
	{ModeHTTP, "从 http 请求中读取"},
	{ModeScript, "从脚本的执行结果中读取"},
	{ModeSnmp, "从 SNMP 服务中读取"},
	{ModeCloudWatch, "从 AWS Cloudwatch 中读取"},
	{ModeCloudTrail, "从 AWS CloudTrail 中读取"},
}

var (
	OptionMetaPath = Option{
		KeyName:      KeyMetaPath,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "数据保存路径(meta_path)",
		Advance:      true,
		ToolTip:      "一个文件夹，记录本次reader的读取位置，默认会自动生成",
	}
	OptionDataSourceTag = Option{
		KeyName:      KeyDataSourceTag,
		ChooseOnly:   false,
		Default:      "datasource",
		DefaultNoUse: false,
		Description:  "来源标签(datasource_tag)",
		Advance:      true,
		ToolTip:      "把读取日志的路径名称也作为标签，记录到解析出来的数据结果中，此处填写标签名称",
	}
	OptionBuffSize = Option{
		KeyName:      KeyBufSize,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "数据缓存大小(reader_buf_size)",
		CheckRegex:   "\\d+",
		Advance:      true,
		ToolTip:      "读取数据的缓存大小，默认4096，单位字节",
	}
	OptionEncoding = Option{
		KeyName:    KeyEncoding,
		ChooseOnly: true,
		ChooseOptions: []interface{}{"UTF-8", "UTF-16", "US-ASCII", "ISO-8859-1",
			"GBK", "latin1", "GB18030", "EUC-JP", "UTF-16BE", "UTF-16LE", "Big5", "Shift_JIS",
			"ISO-8859-2", "ISO-8859-3", "ISO-8859-4", "ISO-8859-5", "ISO-8859-6", "ISO-8859-7",
			"ISO-8859-8", "ISO-8859-9", "ISO-8859-10", "ISO-8859-11", "ISO-8859-12", "ISO-8859-13",
			"ISO-8859-14", "ISO-8859-15", "ISO-8859-16", "macos-0_2-10.2", "macos-6_2-10.4",
			"macos-7_3-10.2", "macos-29-10.2", "macos-35-10.2", "windows-1250", "windows-1251",
			"windows-1252", "windows-1253", "windows-1254", "windows-1255", "windows-1256",
			"windows-1257", "windows-1258", "windows-874", "IBM037", "ibm-273_P100-1995",
			"ibm-277_P100-1995", "ibm-278_P100-1995", "ibm-280_P100-1995", "ibm-284_P100-1995",
			"ibm-285_P100-1995", "ibm-290_P100-1995", "ibm-297_P100-1995", "ibm-420_X120-1999",
			//此处省略大量IBM的字符集，太多，等用户需要再加
			"KOI8-R", "KOI8-U", "ebcdic-xml-us"},
		Default:      "UTF-8",
		DefaultNoUse: false,
		Description:  "编码方式(encoding)",
		Advance:      true,
		ToolTip:      "读取日志文件的编码方式，默认为utf-8，即按照utf-8的编码方式读取文件",
	}
	OptionWhence = Option{
		KeyName:       KeyWhence,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{WhenceOldest, WhenceNewest},
		Default:       WhenceOldest,
		Description:   "读取起始位置(read_from)",
		ToolTip:       "在创建新文件或meta信息损坏的时候(即历史读取记录不存在)，将从数据源的哪个位置开始读取，最新或最老",
	}
	OptionReadIoLimit = Option{
		KeyName:      KeyReadIOLimit,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "读取速度限制(readio_limit)",
		CheckRegex:   "\\d+",
		Advance:      true,
		ToolTip:      "读取文件的磁盘限速，填写正整数，单位为MB/s, 默认限速20MB/s",
	}
	OptionHeadPattern = Option{
		KeyName:      KeyHeadPattern,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "按正则表达式规则换行(head_pattern)",
		Advance:      true,
		ToolTip:      "reader每次读取一行，若要读取多行，请填写head_pattern，表示匹配多行时新的一行的开始符合该正则表达式",
	}
	OptionSQLSchema = Option{
		KeyName:      KeySQLSchema,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "手动定义SQL字段类型(sql_schema)",
		Advance:      true,
		ToolTip:      `默认情况下会自动识别数据字段的类型，当不能识别时，可以sql_schema指定 mysql 数据字段的类型，目前支持string、long、float三种类型，单个字段左边为字段名称，右边为类型，空格分隔 abc float；不同的字段用逗号分隔。支持简写为float=>f，long=>l，string=>s. 如："sql_schema":"abc string,bcd float,xyz long"`,
	}
	OptionMagicLagDuration = Option{
		KeyName:      KeyMagicLagDuration,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "魔法变量时间延迟(magic_lag_duration)",
		Advance:      true,
		ToolTip:      `针对魔法变量进行时间延迟，单位支持h(时)、m(分)、s(秒)，如写24h，则渲染出来的时间魔法变量往前1天`,
	}
	OptionKeyNewFileNewLine = Option{
		KeyName:       KeyNewFileNewLine,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		DefaultNoUse:  false,
		Description:   "文件末尾添加换行符(newfile_newline)",
		Advance:       true,
		ToolTip:       "开启后，不同文件结尾自动添加换行符",
	}
	OptionKeySkipFileFirstLine = Option{
		KeyName:       KeySkipFileFirstLine,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		DefaultNoUse:  false,
		Description:   "跳过新文件的第一行(skip_first_line)",
		Advance:       true,
		Required:      false,
		ToolTip:       "常用于带抬头的csv文件，抬头与实际数据类型不一致",
	}
	OptionKeyValidFilePattern = Option{
		KeyName:      KeyValidFilePattern,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "以linux通配符匹配文件(valid_file_pattern)",
		Advance:      true,
		ToolTip:      `针对dir读取模式需要解析的日志文件，可以设置匹配文件名的模式串，匹配方式为linux通配符展开方式，默认为*，即匹配文件夹下全部文件`,
	}
)

var ModeKeyOptions = map[string][]Option{
	ModeDir: {
		{
			KeyName:      KeyLogPath,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "/home/users/john/log/",
			Required:     true,
			DefaultNoUse: true,
			Description:  "日志文件夹路径(log_path)",
			ToolTip:      "需要收集的日志的文件夹路径",
		},
		OptionMetaPath,
		OptionBuffSize,
		OptionWhence,
		OptionEncoding,
		OptionDataSourceTag,
		OptionReadIoLimit,
		OptionHeadPattern,
		OptionKeyNewFileNewLine,
		OptionKeySkipFileFirstLine,
		{
			KeyName:       KeyIgnoreHiddenFile,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "是否忽略隐藏文件(ignore_hidden)",
			Advance:       true,
			ToolTip:       "读取的过程中是否忽略隐藏文件",
		},
		{
			KeyName:      KeyIgnoreFileSuffix,
			ChooseOnly:   false,
			Default:      strings.Join(defaultIgnoreFileSuffix, ","),
			DefaultNoUse: false,
			Description:  "忽略此类后缀文件(ignore_file_suffix)",
			Advance:      true,
			ToolTip:      `针对dir读取模式需要解析的日志文件，可以设置读取的过程中忽略哪些文件后缀名，默认忽略的后缀包括".pid", ".swap", ".go", ".conf", ".tar.gz", ".tar", ".zip",".a", ".o", ".so"`,
		},
		OptionKeyValidFilePattern,
	},
	ModeFile: {
		{
			KeyName:      KeyLogPath,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "/home/users/john/log/my.log",
			DefaultNoUse: true,
			Description:  "日志文件路径(log_path)",
			ToolTip:      "需要收集的日志的文件路径",
		},
		OptionMetaPath,
		OptionBuffSize,
		OptionWhence,
		OptionDataSourceTag,
		OptionEncoding,
		OptionReadIoLimit,
		OptionHeadPattern,
	},
	ModeTailx: {
		{
			KeyName:      KeyLogPath,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "/home/users/*/mylog/*.log",
			DefaultNoUse: true,
			Description:  "日志文件路径模式串(log_path)",
			ToolTip:      "需要收集的日志的文件（夹）模式串路径，写 * 代表通配",
		},
		OptionMetaPath,
		OptionBuffSize,
		OptionWhence,
		OptionEncoding,
		OptionReadIoLimit,
		OptionDataSourceTag,
		OptionHeadPattern,
		{
			KeyName:      KeyExpire,
			ChooseOnly:   false,
			Default:      "24h",
			DefaultNoUse: false,
			Required:     true,
			Description:  "忽略文件的最大过期时间(expire)",
			CheckRegex:   "\\d+[hms]",
			ToolTip:      `当日志达到expire时间，则放弃追踪。写法为：数字加单位符号，组成字符串duration写法，支持时h、分m、秒s为单位，类似3h(3小时)，10m(10分钟)，5s(5秒)，默认的expire时间是24h`,
		},
		{
			KeyName:      KeyMaxOpenFiles,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "最大打开文件数(max_open_files)",
			CheckRegex:   "\\d+",
			Advance:      true,
			ToolTip:      "最大同时追踪的文件数，默认为256",
		},
		{
			KeyName:      KeyStatInterval,
			ChooseOnly:   false,
			Default:      "3m",
			DefaultNoUse: false,
			Description:  "扫描间隔(stat_interval)",
			CheckRegex:   "\\d+[hms]",
			Advance:      true,
			ToolTip:      `感知新增日志的定时检查时间`,
		},
	},
	ModeFileAuto: {
		{
			KeyName:      KeyLogPath,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "/your/log/dir/or/path*.log",
			DefaultNoUse: true,
			Description:  "日志路径(log_path)",
			ToolTip:      "需要收集的日志文件(夹)路径",
		},
		OptionMetaPath,
		OptionWhence,
		OptionEncoding,
		OptionDataSourceTag,
		OptionKeyNewFileNewLine,
		OptionHeadPattern,
	},
	ModeMySQL: {
		{
			KeyName:       KeyMysqlDataSource,
			Element:       Text,
			ChooseOnly:    false,
			Default:       "",
			Required:      true,
			Placeholder:   "<username>:<password>@tcp(<hostname>:<port>)",
			DefaultNoUse:  true,
			Description:   "数据库地址(mysql_datasource)",
			ToolTip:       `mysql数据源所需信息: username: 用户名, password: 用户密码, hostname: mysql地址, port: mysql端口, 示例：一个填写完整的字段类似于:"admin:123456@tcp(10.101.111.1:3306)"`,
			ToolTipActive: true,
		},
		{
			KeyName:      KeyMysqlDataBase,
			ChooseOnly:   false,
			Placeholder:  "<database>，默认读取所有用户数据库",
			DefaultNoUse: false,
			Default:      "",
			Description:  "数据库名称(mysql_database)",
			ToolTip:      `mysql数据库名称，不填默认读取所有用户数据库`,
		},
		{
			KeyName:      KeyMysqlSQL,
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "select * from <table>;",
			DefaultNoUse: true,
			Description:  "数据查询语句(mysql_sql)",
			ToolTip:      "填写要执行的sql语句",
		},
		OptionEncoding,
		{
			KeyName:      KeyMysqlOffsetKey,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "递增的列名称(mysql_offset_key)",
			Advance:      true,
			ToolTip:      "指定一个 mysql 的列名，作为 offset 的记录，类型必须是整型，建议使用插入(或修改)数据的时间戳(unixnano)作为该字段",
		},
		{
			KeyName:      KeyMysqlReadBatch,
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小(mysql_limit_batch)",
			CheckRegex:   "\\d+",
			Advance:      true,
			ToolTip:      "若数据量大，可以填写该字段，分批次查询",
		},
		OptionMetaPath,
		OptionDataSourceTag,
		{
			KeyName:       KeyMysqlCron,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "00 00 04 * * *",
			DefaultNoUse:  false,
			Description:   "定时任务",
			Advance:       true,
			ToolTip:       `定时任务触发周期，直接写"loop"循环执行，crontab的写法， 类似于* * * * * *，对应的是秒(0~59)，分(0~59)，时(0~23)，日(1~31)，月(1-12)，星期(0~6)，填*号表示所有遍历都执行`,
			ToolTipActive: true,
		},
		{
			KeyName:       KeyMysqlExecOnStart,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "启动时立即执行(mysql_exec_onstart)",
			ToolTip:       "启动时立即执行一次",
		},
		{
			KeyName:       KeyMysqlHistoryAll,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "false",
			DefaultNoUse:  false,
			Description:   "导入历史数据(history_all)",
			ToolTip:       "帮助导入符合要求的所有历史数据",
		},
		{
			KeyName:      KyeMysqlTable,
			ChooseOnly:   false,
			Placeholder:  "<table>",
			DefaultNoUse: true,
			Default:      "",
			Description:  "数据库表名(mysql_table)",
		},
		OptionSQLSchema,
		OptionMagicLagDuration,
	},
	ModeMSSQL: {
		{
			KeyName:       KeyMssqlDataSource,
			Element:       Text,
			ChooseOnly:    false,
			Placeholder:   "server=<hostname or instance>;user id=<username>;password=<password>;port=<port>",
			DefaultNoUse:  true,
			Default:       "",
			Required:      true,
			Description:   "数据库地址(mssql_datasource)",
			ToolTip:       `mssql数据源所需信息, username: 用户名, password: 用户密码, hostname: mssql地址,实例,port: mssql端口,默认1433, 示例：一个填写完整的mssql_datasource字段类似于:"server=localhost\SQLExpress;user id=sa;password=PassWord;port=1433"`,
			ToolTipActive: true,
		},
		{
			KeyName:      KeyMssqlDataBase,
			Default:      "",
			Required:     true,
			ChooseOnly:   false,
			Placeholder:  "<database>",
			DefaultNoUse: true,
			Description:  "数据库名称(mssql_database)",
			ToolTip:      "数据库名称",
		},
		{
			KeyName:      KeyMssqlSQL,
			Default:      "",
			Required:     false,
			ChooseOnly:   false,
			Placeholder:  "select * from <table>;",
			DefaultNoUse: true,
			Description:  "数据查询语句(mssql_sql)",
			ToolTip:      "要执行的sql语句",
		},
		{
			KeyName:      KeyMssqlOffsetKey,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
			Description:  "递增的列名称(mssql_offset_key)",
			Advance:      true,
			ToolTip:      `指定一个mssql的列名，作为offset的记录，类型必须是整型，建议使用插入(或修改)数据的时间戳(unixnano)作为该字段`,
		},
		OptionMetaPath,
		OptionDataSourceTag,
		{
			KeyName:      KeyMssqlReadBatch,
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小(mssql_limit_batch)",
			CheckRegex:   "\\d+",
			Advance:      true,
			ToolTip:      "若数据量大，可以填写该字段，分批次查询",
		},
		{
			KeyName:       KeyMssqlCron,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "00 00 04 * * *",
			DefaultNoUse:  false,
			Description:   "定时任务(mssql_cron)",
			Advance:       true,
			ToolTip:       `定时任务触发周期，直接写"loop"循环执行，crontab的写法，类似于* * * * * *，对应的是秒(0~59)，分(0~59)，时(0~23)，日(1~31)，月(1-12)，星期(0~6)，填*号表示所有遍历都执行`,
			ToolTipActive: true,
		},
		{
			KeyName:       KeyMssqlExecOnStart,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "启动时立即执行(mssql_exec_onstart)",
			ToolTip:       "启动时立即执行一次",
		},
		OptionSQLSchema,
		OptionMagicLagDuration,
	},
	ModePostgreSQL: {
		{
			KeyName:       KeyPGsqlDataSource,
			Element:       Text,
			ChooseOnly:    false,
			Default:       "",
			Required:      true,
			Placeholder:   "host=localhost port=5432 connect_timeout=10 user=pqgotest password=123456 sslmode=disable",
			DefaultNoUse:  true,
			Description:   "数据库地址(postgres_datasource)",
			ToolTip:       `PostgreSQL数据源所需信息，填写的形式如 host=localhost port=5432, 属性和实际的值用=(等于)符号连接，中间不要有空格，不同的属性用(空格)隔开，一个填写完整的 postgres_datasource 字段类似于:"host=localhost port=5432 connect_timeout=10 user=pqgotest password=123456 sslmode=disable"`,
			ToolTipActive: true,
		},
		{
			KeyName:      KeyPGsqlDataBase,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "<database>",
			DefaultNoUse: true,
			Description:  "数据库名称(postgres_database)",
			ToolTip:      "数据库名称",
		},
		{
			KeyName:      KeyPGsqlSQL,
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "select * from <table>;",
			DefaultNoUse: true,
			Description:  "数据查询语句(postgres_sql)",
			ToolTip:      "要执行的查询语句",
		},
		{
			KeyName:      KeyPGsqlOffsetKey,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
			Description:  "递增的列名称(postgres_offset_key)",
			Advance:      true,
			ToolTip:      `指定一个 PostgreSQL 的列名，作为 offset 的记录，类型必须是整型，建议使用插入(或修改)数据的时间戳(unixnano)作为该字段`,
		},
		OptionMetaPath,
		OptionDataSourceTag,
		{
			KeyName:      KeyPGsqlReadBatch,
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小(postgres_limit_batch)",
			CheckRegex:   "\\d+",
			Advance:      true,
			ToolTip:      "若数据量大，可以填写该字段，分批次查询",
		},
		{
			KeyName:       KeyPGsqlCron,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "00 00 04 * * *",
			DefaultNoUse:  false,
			Description:   "定时任务(postgres_cron)",
			Advance:       true,
			ToolTip:       `定时任务触发周期，直接写"loop"循环执行，crontab的写法，类似于* * * * * *，对应的是秒(0~59)，分(0~59)，时(0~23)，日(1~31)，月(1-12)，星期(0~6)，填*号表示所有遍历都执行`,
			ToolTipActive: true,
		},
		{
			KeyName:       KeyPGsqlExecOnStart,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "启动时立即执行(postgres_exec_onstart)",
			ToolTip:       "启动时立即执行一次",
		},
		OptionSQLSchema,
		OptionMagicLagDuration,
	},
	ModeElastic: {
		{
			KeyName:       KeyESHost,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  true,
			Required:      true,
			Placeholder:   "http://localhost:9200",
			Description:   "数据库地址(es_host)",
			ToolTip:       "es的host地址以及端口，常用端口是9200",
			ToolTipActive: true,
		},
		{
			KeyName:       KeyESVersion,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{ElasticVersion3, ElasticVersion5, ElasticVersion6},
			Description:   "版本(es_version)",
			ToolTip:       "版本，3.x包含了2.x",
		},
		{
			KeyName:      KeyESIndex,
			ChooseOnly:   false,
			Placeholder:  "app-repo-123",
			DefaultNoUse: true,
			Default:      "",
			Required:     true,
			Description:  "索引名称(es_index)",
		},
		{
			KeyName:      KeyESType,
			ChooseOnly:   false,
			Placeholder:  "type_app",
			Default:      "",
			Required:     true,
			DefaultNoUse: true,
			Description:  "app名称(es_type)",
		},
		OptionMetaPath,
		OptionDataSourceTag,
		{
			KeyName:      KeyESReadBatch,
			ChooseOnly:   false,
			Default:      "100",
			Required:     true,
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小(es_limit_batch)",
			Advance:      true,
			ToolTip:      "单批次查询数据大小，默认100",
		},
		{
			KeyName:      KeyESKeepAlive,
			ChooseOnly:   false,
			Default:      "1d",
			Required:     true,
			DefaultNoUse: false,
			Description:  "offset保存时间(es_keepalive)",
			CheckRegex:   "\\d+[dms]",
			Advance:      true,
			ToolTip:      "logkit重启后可以继续读取ES数据的Offset记录在es服务端保存的时长，默认1d",
		},
	},
	ModeMongo: {
		{
			KeyName:       KeyMongoHost,
			ChooseOnly:    false,
			Default:       "",
			Required:      true,
			Placeholder:   "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]",
			DefaultNoUse:  true,
			Description:   "数据库地址(mongo_host)",
			ToolTip:       `mongodb的url地址，基础的是mongo的host地址以及端口，默认是localhost:9200，扩展形式可以写为： mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]`,
			ToolTipActive: true,
		},
		{
			KeyName:      KeyMongoDatabase,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "app123",
			DefaultNoUse: true,
			Description:  "数据库名称(mongo_database)",
			ToolTip:      "",
		},
		{
			KeyName:      KeyMongoCollection,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "collection1",
			DefaultNoUse: true,
			Description:  "数据表名称(mongo_collection)",
			ToolTip:      "",
		},
		{
			KeyName:      KeyMongoOffsetKey,
			ChooseOnly:   false,
			Default:      "_id",
			Required:     true,
			DefaultNoUse: false,
			Description:  "递增的主键(mongo_offset_key)",
			ToolTip:      `指定一个mongo的列名，作为offset的记录，类型必须是整型(比如unixnano的时间，或者自增的primary key)`,
		},
		OptionMetaPath,
		OptionDataSourceTag,
		{
			KeyName:      KeyMongoReadBatch,
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小(mongo_limit_batch)",
			CheckRegex:   "\\d+",
			Advance:      true,
			ToolTip:      "单次请求获取的数据量",
		},
		{
			KeyName:       KeyMongoCron,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "00 00 04 * * *",
			DefaultNoUse:  false,
			Description:   "定时任务(mongo_cron)",
			Advance:       true,
			ToolTip:       `定时任务触发周期，直接写"loop"循环执行，crontab的写法，类似于* * * * * *，对应的是秒(0~59)，分(0~59)，时(0~23)，日(1~31)，月(1-12)，星期(0~6)，填*号表示所有遍历都执行`,
			ToolTipActive: true,
		},
		{
			KeyName:       KeyMongoExecOnstart,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "启动时立即执行(mongo_exec_onstart)",
			ToolTip:       "启动时立即执行一次",
		},
		{
			KeyName:      KeyMongoFilters,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Placeholder:  "{\"foo\": {\"i\": {\"$gt\": 10}}}",
			Description:  "数据过滤方式(mongo_filters)",
			Advance:      true,
			ToolTip:      "表示collection的过滤规则，默认不过滤，全部获取",
		},
	},
	ModeKafka: {
		{
			KeyName:      KeyKafkaGroupID,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "logkit1",
			DefaultNoUse: true,
			Description:  "consumer组名称(kafka_groupid)",
			ToolTip:      "kafka组名，多个logkit同时消费时写同一个组名可以协同读取数据",
		},
		{
			KeyName:      KeyKafkaTopic,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "test_topic1",
			DefaultNoUse: true,
			Description:  "topic名称(kafka_topic)",
		},
		{
			KeyName:       KeyKafkaZookeeper,
			ChooseOnly:    false,
			Default:       "",
			Required:      true,
			Placeholder:   "localhost:2181",
			DefaultNoUse:  true,
			Description:   "zookeeper地址(kafka_zookeeper)",
			ToolTip:       "zookeeper地址列表，多个用逗号分隔，常用端口是2181",
			ToolTipActive: true,
		},
		{
			KeyName:      KeyKafkaZookeeperChroot,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "zookeeper中kafka根路径(kafka_zookeeper_chroot)",
			Advance:      true,
			ToolTip:      "kafka在zookeeper根路径中的地址",
		},
		OptionWhence,
		{
			KeyName:      KeyKafkaZookeeperTimeout,
			ChooseOnly:   false,
			Default:      "1",
			Required:     true,
			DefaultNoUse: false,
			Description:  "zookeeper超时时间(kafka_zookeeper_timeout)",
			Advance:      true,
			ToolTip:      "zookeeper连接超时时间，单位为秒",
		},
		OptionDataSourceTag,
	},
	ModeRedis: {
		{
			KeyName:       KeyRedisDataType,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{DataTypeList, DataTypeChannel, DataTypePatterChannel, DataTypeString, DataTypeSet, DateTypeSortedSet, DateTypeHash},
			Description:   "数据读取模式(redis_datatype)",
			ToolTip:       "",
		},
		{
			KeyName:       KeyRedisDB,
			ChooseOnly:    false,
			Default:       "",
			Required:      true,
			Placeholder:   "db",
			DefaultNoUse:  true,
			Description:   "数据库名称(redis_db)",
			ToolTip:       `Redis的数据库名，默认为"0"`,
			ToolTipActive: true,
		},
		{
			KeyName:      KeyRedisKey,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "key1",
			DefaultNoUse: true,
			Description:  "redis键(redis_key)",
			ToolTip:      `Redis监听的键值，在不同模式（redis_datatype）下表示不同含义`,
		},
		{
			KeyName:      KeyRedisHashArea,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "hash模式对应的数据结构域(redisHash_area)",
			Advance:      true,
			ToolTip:      "",
		},
		{
			KeyName:       KeyRedisAddress,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "127.0.0.1:6379",
			Required:      true,
			DefaultNoUse:  false,
			Description:   "数据库地址(redis_address)",
			ToolTip:       `Redis的地址（IP+端口），默认为"127.0.0.1:6379"`,
			ToolTipActive: true,
		},
		{
			KeyName:      KeyRedisPassword,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "密码(redis_password)",
			Advance:      true,
		},
		{
			KeyName:      KeyTimeoutDuration,
			ChooseOnly:   false,
			Default:      "5s",
			Required:     true,
			DefaultNoUse: false,
			Description:  "单次读取超时时间(redis_timeout)",
			CheckRegex:   "\\d+[ms]",
			Advance:      true,
			ToolTip:      "每次等待键值数据的超时时间[m(分)、s(秒)]",
		},
		OptionDataSourceTag,
	},
	ModeSocket: {
		{
			KeyName:       KeySocketServiceAddress,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "tcp://127.0.0.1:3110",
			Required:      true,
			DefaultNoUse:  true,
			Description:   "socket监听的地址(socket_service_address)",
			ToolTip:       "socket监听的地址[协议://端口]，如udp://127.0.0.1:3110",
			ToolTipActive: true,
		},
		{
			KeyName:      KeySocketMaxConnections,
			ChooseOnly:   false,
			Default:      "0",
			DefaultNoUse: false,
			Description:  "最大并发连接数(socket_max_connections)",
			Advance:      true,
			ToolTip:      "仅tcp协议下生效",
		},
		{
			KeyName:       KeySocketReadTimeout,
			ChooseOnly:    false,
			Default:       "1m",
			DefaultNoUse:  false,
			Description:   "连接超时时间(socket_read_timeout)",
			Advance:       true,
			ToolTip:       "填0为不设置超时",
			ToolTipActive: true,
		},
		{
			KeyName:      KeySocketReadBufferSize,
			ChooseOnly:   false,
			Default:      "65535",
			DefaultNoUse: false,
			Description:  "连接缓存大小(socket_read_buffer_size)",
			Advance:      true,
			ToolTip:      "仅udp协议下生效",
		},
		{
			KeyName:       KeySocketKeepAlivePeriod,
			ChooseOnly:    false,
			Default:       "5m",
			DefaultNoUse:  false,
			Description:   "连接保持时长[0为关闭](socket_keep_alive_period)",
			Advance:       true,
			ToolTip:       "填0为关闭keep_alive",
			ToolTipActive: true,
		},
		OptionDataSourceTag,
	},
	ModeHTTP: {
		{
			KeyName:      KeyHTTPServiceAddress,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  DefaultHTTPServiceAddress,
			Required:     true,
			DefaultNoUse: true,
			Description:  "监听的地址和端口(http_service_address)",
			ToolTip:      "监听的地址和端口，格式为：[<ip/host/不填>:port]，如 :3000 , 监听3000端口的http请求",
		},
		{
			KeyName:      KeyHTTPServiceAddress,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  DefaultHTTPServicePath,
			Required:     true,
			DefaultNoUse: true,
			Description:  "监听地址前缀(http_service_path)",
			ToolTip:      "监听的请求地址，如 /data ",
		},
	},
	ModeScript: {
		{
			KeyName:      KeyExecInterpreter,
			ChooseOnly:   false,
			Default:      "/bin/bash",
			Placeholder:  "/bin/bash",
			Required:     true,
			DefaultNoUse: false,
			Description:  "脚本执行解释器(script_exec_interpreter)",
			ToolTip:      "脚本的解释器",
		},
		{
			KeyName:      KeyLogPath,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "/home/users/john/log/my.sh",
			DefaultNoUse: true,
			Description:  "脚本路径(log_path)",
			ToolTip:      "脚本的路径",
		},
		{
			KeyName:       KeyScriptCron,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "00 00 04 * * *",
			DefaultNoUse:  false,
			Description:   "定时任务(script_cron)",
			Advance:       true,
			ToolTip:       `定时任务触发周期，直接写"loop"循环执行，crontab的写法，类似于* * * * * *，对应的是秒(0~59)，分(0~59)，时(0~23)，日(1~31)，月(1-12)，星期(0~6)，填*号表示所有遍历都执行`,
			ToolTipActive: true,
		},
		{
			KeyName:       KeyScriptExecOnStart,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "启动时立即执行(script_exec_onstart)",
			ToolTip:       "",
		},
	},
	ModeCloudWatch: {
		{
			KeyName:      KeyRegion,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "us-east-1",
			DefaultNoUse: true,
			Required:     true,
			Description:  "区域(region)",
			ToolTip:      "服务所在区域",
		},
		{
			KeyName:      KeyNamespace,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "AWS/ELB",
			DefaultNoUse: true,
			Required:     true,
			Description:  "命名空间(namespace)",
			ToolTip:      "Cloudwatch数据的命名空间",
		},
		{
			KeyName:      KeyRoleArn,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "选填一种方式鉴权",
			DefaultNoUse: true,
			Description:  "授权角色(role_arn)",
			Advance:      true,
			ToolTip:      "AWS的授权角色(鉴权第一优先)",
		},
		{
			KeyName:      KeyAWSAccessKey,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "选填一种方式鉴权",
			DefaultNoUse: true,
			Description:  "AK(aws_access_key)",
			ToolTip:      "AWS的access_key(鉴权第二优先)",
		},
		{
			KeyName:      KeyAWSSecretKey,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "选填一种方式鉴权",
			DefaultNoUse: true,
			Description:  "SK(aws_secret_key)",
			ToolTip:      "AWS的secret_key(鉴权第二优先)",
		},
		{
			KeyName:      KeyAWSToken,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "选填一种方式鉴权",
			DefaultNoUse: true,
			Description:  "鉴权token(aws_token)",
			ToolTip:      "AWS的鉴权token",
		},
		{
			KeyName:      KeyAWSProfile,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "选填一种方式鉴权",
			DefaultNoUse: true,
			Description:  "共享profile(aws_profile)",
			ToolTip:      "鉴权第三优先",
		},
		{
			KeyName:      KeySharedCredentialFile,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "选填一种方式鉴权",
			DefaultNoUse: true,
			Description:  "鉴权文件(shared_credential_file)",
			ToolTip:      "鉴权文件路径(鉴权第四优先)",
		},
		{
			KeyName:      KeyCollectInterval,
			ChooseOnly:   false,
			Default:      "5m",
			Placeholder:  "",
			DefaultNoUse: false,
			Description:  "收集间隔(interval)",
			ToolTip:      "最小设置1分钟(1m)",
		},
		{
			KeyName:      KeyRateLimit,
			ChooseOnly:   false,
			Default:      "200",
			Placeholder:  "",
			DefaultNoUse: false,
			Required:     false,
			Description:  "每秒最大请求数(ratelimit)",
			Advance:      true,
			ToolTip:      "请求限速",
		},
		{
			KeyName:      KeyMetrics,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "Latency, RequestCount",
			DefaultNoUse: false,
			Required:     false,
			Description:  "metric名称(metrics)",
			ToolTip:      "可填写多个,逗号连接,为空全部收集",
		},
		{
			KeyName:      KeyDimension,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "LoadBalancerName p-example,LatencyName y-example",
			DefaultNoUse: false,
			Required:     false,
			Description:  "收集维度(dimensions)",
			ToolTip:      "可填写多个,逗号连接,为空全部收集",
		},
		{
			KeyName:      KeyDelay,
			ChooseOnly:   false,
			Default:      "5m",
			Placeholder:  "",
			DefaultNoUse: false,
			Required:     false,
			Description:  "收集延迟(delay)",
			Advance:      true,
			ToolTip:      "根据CloudWatch中数据产生的实际时间确定是否需要增加收集延迟",
		},
		{
			KeyName:      KeyCacheTTL,
			ChooseOnly:   false,
			Default:      "60m",
			Placeholder:  "60m",
			DefaultNoUse: false,
			Required:     false,
			Description:  "刷新metrics时间(cache_ttl)",
			Advance:      true,
			ToolTip:      "从namespace中刷新metrics的周期",
		},
		{
			KeyName:      KeyPeriod,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "1m",
			DefaultNoUse: false,
			Required:     false,
			Description:  "聚合间隔(period)",
			Advance:      true,
			ToolTip:      "从cloudwatch收集数据聚合的间隔",
		},
		OptionDataSourceTag,
	},
	ModeSnmp: {
		{
			KeyName:      KeySnmpReaderName,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "logkit_default_name",
			DefaultNoUse: true,
			Description:  "名称(snmp_reader_name)",
			Advance:      true,
			ToolTip:      "reader的读取名称",
		},
		{
			KeyName:       KeySnmpReaderAgents,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "127.0.0.1:161,10.10.0.1:161",
			Required:      true,
			DefaultNoUse:  true,
			Description:   "agents列表(snmp_agents)",
			ToolTip:       "多个可用逗号','分隔",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderTables,
			ChooseOnly:    false,
			Default:       "",
			Placeholder:   "[{\"table_name\":\"udpLocalAddress\",\"table_oid\":\"1.3.6.1.2.1.31.1.1\"}]",
			DefaultNoUse:  true,
			Description:   "tables配置(snmp_tables)",
			ToolTip:       "请填入json数组字符串",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderFields,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  true,
			Description:   "fields配置(snmp_fields)",
			ToolTip:       "请填入json数组字符串",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderVersion,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"1", "2", "3"},
			Default:       "2",
			DefaultNoUse:  true,
			Description:   "snmp协议版本(snmp_version)",
		},
		{
			KeyName:       KeySnmpReaderTimeOut,
			ChooseOnly:    false,
			Default:       "5s",
			Required:      true,
			DefaultNoUse:  false,
			Description:   "连接超时时间(snmp_time_out)",
			Advance:       true,
			ToolTip:       "超时时间，单位支持m(分)、s(秒)",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderInterval,
			ChooseOnly:    false,
			Default:       "30s",
			Required:      true,
			DefaultNoUse:  false,
			Description:   "收集频率(snmp_interval)",
			Advance:       true,
			ToolTip:       "收集频率，单位支持m(分)、s(秒)",
			ToolTipActive: true,
		},
		{
			KeyName:      KeySnmpReaderRetries,
			ChooseOnly:   false,
			Default:      "3",
			Required:     true,
			DefaultNoUse: false,
			Description:  "重试次数(snmp_retries)",
			Advance:      true,
		},
		{
			KeyName:       KeySnmpReaderCommunity,
			ChooseOnly:    false,
			Default:       "public",
			Placeholder:   "public",
			DefaultNoUse:  true,
			Description:   "community/秘钥(snmp_version)",
			ToolTip:       "community秘钥，没有特殊设置为public[版本1/2有效]",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderMaxRepetitions,
			ChooseOnly:    false,
			Default:       "50",
			DefaultNoUse:  false,
			Description:   "最大迭代次数(snmp_max_repetitions)",
			Advance:       true,
			ToolTip:       "版本2/3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderContextName,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "context名称(snmp_version)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderSecLevel,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"noAuthNoPriv", "authNoPriv", "authPriv"},
			DefaultNoUse:  true,
			Description:   "安全等级(snmp_sec_level)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderSecName,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "认证名称(snmp_sec_name)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderAuthProtocol,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"md5", "sha", ""},
			DefaultNoUse:  false,
			Description:   "认证协议(snmp_auth_protocol)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderAuthPassword,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "认证密码(snmp_auth_password)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderPrivProtocol,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"des", "aes", ""},
			DefaultNoUse:  false,
			Description:   "隐私协议(snmp_priv_protocol)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderPrivPassword,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "隐私密码(snmp_priv_password)",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderEngineID,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "snmp_priv_engine_id",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderEngineBoots,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "snmp_engine_boots",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
		{
			KeyName:       KeySnmpReaderEngineTime,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "snmp_engine_time",
			Advance:       true,
			ToolTip:       "版本3有效",
			ToolTipActive: true,
		},
	},
	ModeCloudTrail: {
		{
			KeyName:      KeyS3Region,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "us-east-1",
			DefaultNoUse: false,
			Required:     true,
			Description:  "区域(s3_region)",
			ToolTip:      "S3服务区域",
		},
		{
			KeyName:      KeyS3AccessKey,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "访问密钥",
			DefaultNoUse: false,
			Required:     true,
			Description:  "AK(s3_access_key)",
			ToolTip:      "访问密钥ID(AK)",
		},
		{
			KeyName:      KeyS3SecretKey,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "访问密钥",
			DefaultNoUse: false,
			Required:     true,
			Description:  "SK(s3_secret_key)",
			ToolTip:      "与访问密钥ID结合使用的密钥(SK)",
		},
		{
			KeyName:      KeyS3Bucket,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "",
			DefaultNoUse: false,
			Required:     true,
			Description:  "存储桶名称(s3_bucket)",
			ToolTip:      "存储桶名称",
		},
		{
			KeyName:      KeyS3Prefix,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "",
			DefaultNoUse: false,
			Description:  "文件前缀(s3_prefix)",
			Advance:      true,
			ToolTip:      "文件前缀",
		},
		{
			KeyName:      KeySyncDirectory,
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "",
			DefaultNoUse: false,
			Advance:      true,
			Description:  "本地同步目录，不填自动生成(sync_directory)",
			ToolTip:      "本地同步目录，不填自动生成",
		},
		{
			KeyName:      KeySyncInterval,
			ChooseOnly:   false,
			Default:      "5m",
			Placeholder:  "",
			DefaultNoUse: false,
			Advance:      true,
			Description:  "文件同步间隔(sync_interval)",
			ToolTip:      "文件同步的最小间隔(1m)",
		},
		{
			KeyName:      KeySyncConcurrent,
			ChooseOnly:   false,
			Default:      "5",
			Placeholder:  "",
			DefaultNoUse: false,
			Advance:      true,
			Description:  "文件同步的并发个数(sync_concurrent)",
			ToolTip:      "文件同步的最小并发个数(1)",
		},
		OptionKeyValidFilePattern,
		OptionKeySkipFileFirstLine,
		OptionDataSourceTag,
	},
}
