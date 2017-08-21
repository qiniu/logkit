package reader

import (
	"strings"

	"fmt"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

// Reader 是一个通用的行读取reader接口
type Reader interface {
	//Name reader名称
	Name() string
	//Source 读取的数据源
	Source() string
	ReadLine() (string, error)
	SetMode(mode string, v interface{}) error
	Close() error
	SyncMeta()
}

// FileReader reader 接口方法
type FileReader interface {
	Name() string
	Source() string
	Read(p []byte) (n int, err error)
	Close() error
	SyncMeta() error
}

// TODO 构建统一的 Server reader框架， 减少重复的编码
type ServerReader interface {
	//Name reader名称
	Name() string
	//Source 读取的数据源
	Source() string
	Start()
	ReadLine() (string, error)
	Close() error
	SyncMeta()
}

// FileReader's conf keys
const (
	KeyLogPath       = "log_path"
	KeyMetaPath      = "meta_path"
	KeyFileDone      = "file_done"
	KeyMode          = "mode"
	KeyBufSize       = "reader_buf_size"
	KeyWhence        = "read_from"
	KeyEncoding      = "encoding"
	KeyReadIOLimit   = "readio_limit"
	KeyDataSourceTag = "datasource_tag"
	KeyHeadPattern   = "head_pattern"
	KeyRunnerName    = "runner_name"

	// 忽略隐藏文件
	KeyIgnoreHiddenFile = "ignore_hidden"
	KeyIgnoreFileSuffix = "ignore_file_suffix"
	KeyValidFilePattern = "valid_file_pattern"

	KeyExpire       = "expire"
	KeyMaxOpenFiles = "max_open_files"
	KeyStatInterval = "stat_interval"

	KeyMysqlOffsetKey   = "mysql_offset_key"
	KeyMysqlReadBatch   = "mysql_limit_batch"
	KeyMysqlDataSource  = "mysql_datasource"
	KeyMysqlDataBase    = "mysql_database"
	KeyMysqlSQL         = "mysql_sql"
	KeyMysqlCron        = "mysql_cron"
	KeyMysqlExecOnStart = "mysql_exec_onstart"

	KeySQLSchema = "sql_schema"

	KeyMssqlOffsetKey   = "mssql_offset_key"
	KeyMssqlReadBatch   = "mssql_limit_batch"
	KeyMssqlDataSource  = "mssql_datasource"
	KeyMssqlDataBase    = "mssql_database"
	KeyMssqlSQL         = "mssql_sql"
	KeyMssqlCron        = "mssql_cron"
	KeyMssqlExecOnStart = "mssql_exec_onstart"

	KeyESReadBatch = "es_limit_batch"
	KeyESIndex     = "es_index"
	KeyESType      = "es_type"
	KeyESHost      = "es_host"
	KeyESKeepAlive = "es_keepalive"
	KeyESVersion   = "es_version"

	KeyMongoHost        = "mongo_host"
	KeyMongoDatabase    = "mongo_database"
	KeyMongoCollection  = "mongo_collection"
	KeyMongoOffsetKey   = "mongo_offset_key"
	KeyMongoReadBatch   = "mongo_limit_batch"
	KeyMongoCron        = "mongo_cron"
	KeyMongoExecOnstart = "mongo_exec_onstart"
	KeyMongoFilters     = "mongo_filters"
	KeyMongoCert        = "mongo_cacert"

	KeyKafkaGroupID   = "kafka_groupid"
	KeyKafkaTopic     = "kafka_topic"
	KeyKafkaZookeeper = "kafka_zookeeper"
)

var defaultIgnoreFileSuffix = []string{
	".pid", ".swap", ".go", ".conf", ".tar.gz", ".tar", ".zip",
	".a", ".o", ".so"}

// FileReader's modes
const (
	ModeDir     = "dir"
	ModeFile    = "file"
	ModeTailx   = "tailx"
	ModeMysql   = "mysql"
	ModeMssql   = "mssql"
	ModeElastic = "elastic"
	ModeMongo   = "mongo"
	ModeKafka   = "kafka"
	ModeRedis   = "redis"
)

const (
	ReadModeHeadPatternString = "mode_head_pattern_string"
	ReadModeHeadPatternRegexp = "mode_head_pattern_regexp"
)

// KeyWhence 的可选项
const (
	WhenceOldest = "oldest"
	WhenceNewest = "newest"
)

// NewFileReader 创建FileReader
func NewFileBufReader(conf conf.MapConf) (reader Reader, err error) {
	meta, err := NewMetaWithConf(conf)
	if err != nil {
		log.Warn(err)
		return
	}
	return NewFileBufReaderWithMeta(conf, meta)
}

func NewFileBufReaderWithMeta(conf conf.MapConf, meta *Meta) (reader Reader, err error) {
	mode, _ := conf.GetStringOr(KeyMode, ModeDir)
	logpath, err := conf.GetString(KeyLogPath)
	if err != nil && (mode == ModeFile || mode == ModeDir || mode == ModeTailx) {
		return
	}
	err = nil
	bufSize, _ := conf.GetIntOr(KeyBufSize, defaultBufSize)
	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	decoder, _ := conf.GetStringOr(KeyEncoding, "")
	if decoder != "" {
		meta.SetEncodingWay(strings.ToLower(decoder))
	}
	headPattern, _ := conf.GetStringOr(KeyHeadPattern, "")
	var fr FileReader
	switch mode {
	case ModeDir:
		// 默认不读取隐藏文件
		ignoreHidden, _ := conf.GetBoolOr(KeyIgnoreHiddenFile, true)
		ignoreFileSuffix, _ := conf.GetStringListOr(KeyIgnoreFileSuffix, defaultIgnoreFileSuffix)
		validFilesRegex, _ := conf.GetStringOr(KeyValidFilePattern, "*")
		fr, err = NewSeqFile(meta, logpath, ignoreHidden, ignoreFileSuffix, validFilesRegex, whence)
		if err != nil {
			return
		}
		reader, err = NewReaderSize(fr, meta, bufSize)

	case ModeFile:
		fr, err = NewSingleFile(meta, logpath, whence)
		if err != nil {
			return
		}
		reader, err = NewReaderSize(fr, meta, bufSize)
	case ModeTailx:
		expireDur, _ := conf.GetStringOr(KeyExpire, "24h")
		stateIntervalDur, _ := conf.GetStringOr(KeyStatInterval, "3m")
		maxOpenFiles, _ := conf.GetIntOr(KeyMaxOpenFiles, 256)
		reader, err = NewMultiReader(meta, logpath, whence, expireDur, stateIntervalDur, maxOpenFiles)
	case ModeMysql: // Mysql 模式是启动mysql reader,读取mysql数据表
		reader, err = NewSQLReader(meta, conf)
	case ModeMssql: // Mssql 模式是启动mssql reader，读取mssql数据表
		reader, err = NewSQLReader(meta, conf)
	case ModeElastic:
		readBatch, _ := conf.GetIntOr(KeyESReadBatch, 100)
		estype, err := conf.GetString(KeyESType)
		if err != nil {
			return nil, err
		}
		esindex, err := conf.GetString(KeyESIndex)
		if err != nil {
			return nil, err
		}
		eshost, _ := conf.GetStringOr(KeyESHost, "http://localhost:9200")
		if !strings.HasPrefix(eshost, "http://") && !strings.HasPrefix(eshost, "https://") {
			eshost = "http://" + eshost
		}
		esVersion, _ := conf.GetStringOr(KeyESVersion, ElasticVersion2)
		keepAlive, _ := conf.GetStringOr(KeyESKeepAlive, "6h")
		reader, err = NewESReader(meta, readBatch, estype, esindex, eshost, esVersion, keepAlive)
	case ModeMongo:
		readBatch, _ := conf.GetIntOr(KeyMongoReadBatch, 100)
		database, err := conf.GetString(KeyMongoDatabase)
		if err != nil {
			return nil, err
		}
		coll, err := conf.GetString(KeyMongoCollection)
		if err != nil {
			return nil, err
		}
		mongohost, _ := conf.GetStringOr(KeyMongoHost, "localhost:9200")
		offsetKey, _ := conf.GetStringOr(KeyMongoOffsetKey, MongoDefaultOffsetKey)
		cronSchedule, _ := conf.GetStringOr(KeyMongoCron, "")
		execOnStart, _ := conf.GetBoolOr(KeyMongoExecOnstart, true)
		filters, _ := conf.GetStringOr(KeyMongoFilters, "")
		certfile, _ := conf.GetStringOr(KeyMongoCert, "")
		reader, err = NewMongoReader(meta, readBatch, mongohost, database, coll, offsetKey, cronSchedule, filters, certfile, execOnStart)
	case ModeKafka:
		consumerGroup, err := conf.GetString(KeyKafkaGroupID)
		if err != nil {
			return nil, err
		}
		topics, err := conf.GetStringList(KeyKafkaTopic)
		if err != nil {
			return nil, err
		}
		zookeepers, err := conf.GetStringList(KeyKafkaZookeeper)
		reader, err = NewKafkaReader(meta, consumerGroup, topics, zookeepers, whence)
	case ModeRedis:
		reader, err = NewRedisReader(meta, conf)
	default:
		err = fmt.Errorf("mode %v not supported now", mode)
	}
	if err != nil {
		return
	}
	if headPattern != "" {
		err = reader.SetMode(ReadModeHeadPatternString, headPattern)
	}
	return
}
