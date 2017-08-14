package reader

import (
	"strings"

	"github.com/qiniu/logkit/utils"
)

// ModeUsages 用途说明
var ModeUsages = []utils.KeyValue{
	{ModeDir, "从文件读取( dir 模式)"},
	{ModeFile, "从文件读取( file 模式)"},
	{ModeTailx, "从文件读取( tailx 模式)"},
	{ModeMysql, "从 MySQL 读取"},
	{ModeMssql, "从 MSSQL 读取"},
	{ModeElastic, "从 Elasticsearch 读取"},
	{ModeMongo, "从 MongoDB 读取"},
	{ModeKafka, "从 kafka 读取"},
	{ModeRedis, "从 redis 读取"},
}

var ModeKeyOptions = map[string]map[string]utils.Option{
	ModeDir: {
		KeyLogPath: utils.Option{
			ChooseOnly:   false,
			Default:      "/home/users/john/log/",
			DefaultNoUse: true,
			Description:  "日志文件夹路径",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyFileDone: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "读取过的文件信息保存路径",
		},
		KeyBufSize: utils.Option{
			ChooseOnly:   false,
			Default:      "4096",
			DefaultNoUse: false,
			Description:  "文件缓存数据大小",
		},
		KeyWhence: utils.Option{
			ChooseOnly:    true,
			ChooseOptions: []string{WhenceOldest, WhenceNewest},
			Description:   "读取的起始位置",
		},
		KeyEncoding: utils.Option{
			ChooseOnly:   false,
			Default:      "UTF-8",
			DefaultNoUse: false,
			Description:  "编码方式",
		},
		KeyReadIOLimit: utils.Option{
			ChooseOnly:   false,
			Default:      "20",
			DefaultNoUse: false,
			Description:  "读取速度限制(MB/s)",
		},
		KeyHeadPattern: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "多行读取的起始行正则表达式",
		},
		KeyIgnoreHiddenFile: utils.Option{
			ChooseOnly:   false,
			Default:      "true",
			DefaultNoUse: false,
			Description:  "是否忽略隐藏文件",
		},
		KeyIgnoreFileSuffix: utils.Option{
			ChooseOnly:   false,
			Default:      strings.Join(defaultIgnoreFileSuffix, ","),
			DefaultNoUse: false,
			Description:  "根据后缀忽略文件",
		},
		KeyValidFilePattern: utils.Option{
			ChooseOnly:   false,
			Default:      "*",
			DefaultNoUse: false,
			Description:  "根据正则表达式匹配文件",
		},
	},
	ModeFile: {
		KeyLogPath: utils.Option{
			ChooseOnly:   false,
			Default:      "/home/users/john/log/my.log",
			DefaultNoUse: true,
			Description:  "日志文件路径",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyBufSize: utils.Option{
			ChooseOnly:   false,
			Default:      "4096",
			DefaultNoUse: false,
			Description:  "文件缓存数据大小",
		},
		KeyWhence: utils.Option{
			ChooseOnly:    true,
			ChooseOptions: []string{WhenceOldest, WhenceNewest},
			Description:   "读取的起始位置",
		},
		KeyEncoding: utils.Option{
			ChooseOnly:   false,
			Default:      "UTF-8",
			DefaultNoUse: false,
			Description:  "编码方式",
		},
		KeyReadIOLimit: utils.Option{
			ChooseOnly:   false,
			Default:      "20",
			DefaultNoUse: false,
			Description:  "读取速度限制(MB/s)",
		},
		KeyHeadPattern: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "多行读取的起始行正则表达式",
		},
	},
	ModeTailx: {
		KeyLogPath: utils.Option{
			ChooseOnly:   false,
			Default:      "/home/users/*/mylog/*.log",
			DefaultNoUse: true,
			Description:  "日志文件路径模式串",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyBufSize: utils.Option{
			ChooseOnly:   false,
			Default:      "4096",
			DefaultNoUse: false,
			Description:  "文件缓存数据大小",
		},
		KeyWhence: utils.Option{
			ChooseOnly:    true,
			ChooseOptions: []string{WhenceOldest, WhenceNewest},
			Description:   "读取的起始位置",
		},
		KeyEncoding: utils.Option{
			ChooseOnly:   false,
			Default:      "UTF-8",
			DefaultNoUse: false,
			Description:  "编码方式",
		},
		KeyReadIOLimit: utils.Option{
			ChooseOnly:   false,
			Default:      "20",
			DefaultNoUse: false,
			Description:  "读取速度限制(MB/s)",
		},
		KeyDataSourceTag: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "具体的数据文件路径来源标签",
		},
		KeyHeadPattern: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "多行读取的起始行正则表达式",
		},
		KeyExpire: utils.Option{
			ChooseOnly:   false,
			Default:      "24h",
			DefaultNoUse: false,
			Description:  "文件过期时间(时h，分m，秒s)",
		},
		KeyMaxOpenFiles: utils.Option{
			ChooseOnly:   false,
			Default:      "256",
			DefaultNoUse: false,
			Description:  "最大的打开文件数",
		},
		KeyStatInterval: utils.Option{
			ChooseOnly:   false,
			Default:      "3m",
			DefaultNoUse: false,
			Description:  "文件扫描间隔",
		},
	},
	ModeMysql: {
		KeyMysqlDataSource: utils.Option{
			ChooseOnly:   false,
			Default:      "<username>:<password>@tcp(<hostname>:<port>)",
			DefaultNoUse: true,
			Description:  "数据库地址",
		},
		KeyMysqlDataBase: utils.Option{
			ChooseOnly:   false,
			Default:      "<database>",
			DefaultNoUse: true,
			Description:  "数据库名称",
		},
		KeyMysqlSQL: utils.Option{
			ChooseOnly:   false,
			Default:      "select * from <table>;",
			DefaultNoUse: true,
			Description:  "数据查询语句",
		},
		KeyMysqlOffsetKey: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "递增的列名称",
		},
		KeyMysqlReadBatch: utils.Option{
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyMysqlCron: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "定时任务调度Cron",
		},
		KeyMysqlExecOnStart: utils.Option{
			ChooseOnly:   false,
			Default:      "true",
			DefaultNoUse: false,
			Description:  "启动时立即执行",
		},
		KeySQLSchema: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "SQL字段类型定义",
		},
	},
	ModeMssql: {
		KeyMssqlDataSource: utils.Option{
			ChooseOnly:   false,
			Default:      "<username>:<password>@tcp(<hostname>:<port>)",
			DefaultNoUse: true,
			Description:  "数据库地址",
		},
		KeyMssqlDataBase: utils.Option{
			ChooseOnly:   false,
			Default:      "<database>",
			DefaultNoUse: true,
			Description:  "数据库名称",
		},
		KeyMssqlSQL: utils.Option{
			ChooseOnly:   false,
			Default:      "select * from <table>;",
			DefaultNoUse: true,
			Description:  "数据查询语句",
		},
		KeyMssqlOffsetKey: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: true,
			Description:  "递增的列名称",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyMssqlReadBatch: utils.Option{
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小",
		},
		KeyMssqlCron: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "定时任务调度Crontab",
		},
		KeyMssqlExecOnStart: utils.Option{
			ChooseOnly:   false,
			Default:      "true",
			DefaultNoUse: false,
			Description:  "启动时立即执行",
		},
		KeySQLSchema: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "SQL字段类型定义",
		},
	},
	ModeElastic: {
		KeyESHost: utils.Option{
			ChooseOnly:   false,
			Default:      "http://localhost:9200",
			DefaultNoUse: false,
			Description:  "数据库地址",
		},
		KeyESVersion: utils.Option{
			ChooseOnly:    true,
			ChooseOptions: []string{ElasticVersion2, ElasticVersion5},
			Description:   "ES版本号",
		},
		KeyESIndex: utils.Option{
			ChooseOnly:   false,
			Default:      "app-repo-123",
			DefaultNoUse: true,
			Description:  "ES索引名称",
		},
		KeyESType: utils.Option{
			ChooseOnly:   false,
			Default:      "type_app",
			DefaultNoUse: true,
			Description:  "ES的app名称",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyESReadBatch: utils.Option{
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小",
		},
		KeyESKeepAlive: utils.Option{
			ChooseOnly:   false,
			Default:      "1d",
			DefaultNoUse: false,
			Description:  "ES的Offset保存时间",
		},
	},
	ModeMongo: {
		KeyMongoHost: utils.Option{
			ChooseOnly:   false,
			Default:      "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]",
			DefaultNoUse: true,
			Description:  "数据库地址",
		},
		KeyMongoDatabase: utils.Option{
			ChooseOnly:   false,
			Default:      "app123",
			DefaultNoUse: true,
			Description:  "数据库名称",
		},
		KeyMongoCollection: utils.Option{
			ChooseOnly:   false,
			Default:      "collection1",
			DefaultNoUse: true,
			Description:  "数据表名称",
		},
		KeyMongoOffsetKey: utils.Option{
			ChooseOnly:   false,
			Default:      "_id",
			DefaultNoUse: false,
			Description:  "递增的主键",
		},
		KeyMetaPath: utils.Option{
			ChooseOnly:   false,
			Default:      "meta_path",
			DefaultNoUse: false,
			Description:  "断点续传元数据路径",
		},
		KeyMongoReadBatch: utils.Option{
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "分批查询的单批次大小",
		},
		KeyMongoCron: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "定时任务调度Cron",
		},
		KeyMongoExecOnstart: utils.Option{
			ChooseOnly:   false,
			Default:      "true",
			DefaultNoUse: false,
			Description:  "启动时立即执行",
		},
		KeyMongoFilters: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "数据过滤方式",
		},
	},
	ModeKafka: {
		KeyKafkaGroupID: utils.Option{
			ChooseOnly:   false,
			Default:      "logkit1",
			DefaultNoUse: false,
			Description:  "Kafka的consumer组名称",
		},
		KeyKafkaTopic: utils.Option{
			ChooseOnly:   false,
			Default:      "test_topic1",
			DefaultNoUse: true,
			Description:  "Kafka的topic名称",
		},
		KeyKafkaZookeeper: utils.Option{
			ChooseOnly:   false,
			Default:      "localhost:2181",
			DefaultNoUse: true,
			Description:  "Zookeeper地址",
		},
		KeyWhence: utils.Option{
			ChooseOnly:    true,
			ChooseOptions: []string{WhenceOldest, WhenceNewest},
			Description:   "读取的起始位置",
		},
	},
	ModeRedis: {
		KeyRedisDataType: utils.Option{
			ChooseOnly:    true,
			ChooseOptions: []string{DataTypeList, DataTypeChannel, DataTypePatterChannel},
			Description:   "Redis的数据读取模式",
		},
		KeyRedisDB: utils.Option{
			ChooseOnly:   false,
			Default:      "0",
			DefaultNoUse: true,
			Description:  "数据库名称",
		},
		KeyRedisKey: utils.Option{
			ChooseOnly:   false,
			Default:      "key1",
			DefaultNoUse: true,
			Description:  "redis键(key)",
		},
		KeyRedisAddress: utils.Option{
			ChooseOnly:   false,
			Default:      "127.0.0.1:6379",
			DefaultNoUse: false,
			Description:  "数据库地址",
		},
		KeyRedisPassword: utils.Option{
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "密码",
		},
		KeyTimeoutDuration: utils.Option{
			ChooseOnly:   false,
			Default:      "5s",
			DefaultNoUse: false,
			Description:  "单次读取超时时间(m(分)、s(秒))",
		},
	},
}
