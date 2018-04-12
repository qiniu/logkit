package reader

import (
	"fmt"

	"errors"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
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

// StatsReader 是一个通用的带有统计接口的reader
type StatsReader interface {
	//Name reader名称
	Name() string
	Status() StatsInfo
}

//获取数据lag的接口
type LagReader interface {
	Lag() (*LagInfo, error)
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
	KeyLogPath        = "log_path"
	KeyMetaPath       = "meta_path"
	KeyFileDone       = "file_done"
	KeyMode           = "mode"
	KeyBufSize        = "reader_buf_size"
	KeyWhence         = "read_from"
	KeyEncoding       = "encoding"
	KeyReadIOLimit    = "readio_limit"
	KeyDataSourceTag  = "datasource_tag"
	KeyTagFile        = "tag_file"
	KeyHeadPattern    = "head_pattern"
	KeyNewFileNewLine = "newfile_newline"

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

	KeyPGsqlOffsetKey   = "postgres_offset_key"
	KeyPGsqlReadBatch   = "postgres_limit_batch"
	KeyPGsqlDataSource  = "postgres_datasource"
	KeyPGsqlDataBase    = "postgres_database"
	KeyPGsqlSQL         = "postgres_sql"
	KeyPGsqlCron        = "postgres_cron"
	KeyPGsqlExecOnStart = "postgres_exec_onstart"

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

	KeyKafkaGroupID          = "kafka_groupid"
	KeyKafkaTopic            = "kafka_topic"
	KeyKafkaZookeeper        = "kafka_zookeeper"
	KeyKafkaZookeeperChroot  = "kafka_zookeeper_chroot"
	KeyKafkaZookeeperTimeout = "kafka_zookeeper_timeout"

	KeyExecInterpreter   = "script_exec_interprepter"
	KeyScriptCron        = "script_cron"
	KeyScriptExecOnStart = "script_exec_onstart"

	KeyErrDirectReturn = "errDirectReturn"
)

var defaultIgnoreFileSuffix = []string{
	".pid", ".swap", ".go", ".conf", ".tar.gz", ".tar", ".zip",
	".a", ".o", ".so"}

// FileReader's modes
const (
	ModeDir        = "dir"
	ModeFile       = "file"
	ModeTailx      = "tailx"
	ModeFileAuto   = "fileauto"
	ModeMysql      = "mysql"
	ModeMssql      = "mssql"
	ModePG         = "postgres"
	ModeElastic    = "elastic"
	ModeMongo      = "mongo"
	ModeKafka      = "kafka"
	ModeRedis      = "redis"
	ModeSocket     = "socket"
	ModeHttp       = "http"
	ModeScript     = "script"
	ModeSnmp       = "snmp"
	ModeCloudWatch = "cloudwatch"
	ModeClockTrail = "clocktrail"
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

const (
	Loop = "loop"
)

func NewFileBufReader(conf conf.MapConf, errDirectReturn bool) (reader Reader, err error) {
	rs := NewReaderRegistry()
	return rs.NewReader(conf, errDirectReturn)
}

// ReaderRegistry reader 的工厂类。可以注册自定义reader
type ReaderRegistry struct {
	readerTypeMap map[string]func(*Meta, conf.MapConf) (Reader, error)
}

func NewReaderRegistry() *ReaderRegistry {
	ret := &ReaderRegistry{
		readerTypeMap: map[string]func(*Meta, conf.MapConf) (Reader, error){},
	}
	ret.RegisterReader(ModeDir, NewFileDirReader)
	ret.RegisterReader(ModeFileAuto, NewFileAutoReader)
	ret.RegisterReader(ModeFile, NewSingleFileReader)
	ret.RegisterReader(ModeTailx, NewMultiReader)
	ret.RegisterReader(ModeMysql, NewSQLReader)
	ret.RegisterReader(ModeMssql, NewSQLReader)
	ret.RegisterReader(ModePG, NewSQLReader)
	ret.RegisterReader(ModeElastic, NewESReader)
	ret.RegisterReader(ModeMongo, NewMongoReader)
	ret.RegisterReader(ModeKafka, NewKafkaReader)
	ret.RegisterReader(ModeRedis, NewRedisReader)
	ret.RegisterReader(ModeSocket, NewSocketReader)
	ret.RegisterReader(ModeHttp, NewHttpReader)
	ret.RegisterReader(ModeScript, NewScriptReader)
	ret.RegisterReader(ModeSnmp, NewSnmpReader)
	ret.RegisterReader(ModeCloudWatch, NewCloudWatchReader)
	ret.RegisterReader(ModeClockTrail, NewClockTrailReader)

	return ret
}

func (registry *ReaderRegistry) RegisterReader(readerType string, constructor func(*Meta, conf.MapConf) (Reader, error)) error {
	_, exist := registry.readerTypeMap[readerType]
	if exist {
		return errors.New("readerType " + readerType + " has been existed")
	}
	registry.readerTypeMap[readerType] = constructor
	return nil
}

func (r *ReaderRegistry) NewReader(conf conf.MapConf, errDirectReturn bool) (reader Reader, err error) {
	meta, err := NewMetaWithConf(conf)
	if err != nil {
		log.Warn(err)
		return
	}
	return r.NewReaderWithMeta(conf, meta, errDirectReturn)
}

func (r *ReaderRegistry) NewReaderWithMeta(conf conf.MapConf, meta *Meta, errDirectReturn bool) (reader Reader, err error) {
	if errDirectReturn {
		conf[KeyErrDirectReturn] = Bool2String(errDirectReturn)
	}
	mode, _ := conf.GetStringOr(KeyMode, ModeDir)
	headPattern, _ := conf.GetStringOr(KeyHeadPattern, "")

	constructor, exist := r.readerTypeMap[mode]
	if !exist {
		return nil, fmt.Errorf("reader type unsupperted : %v", mode)
	}

	reader, err = constructor(meta, conf)
	if err != nil {
		return
	}
	if headPattern != "" {
		err = reader.SetMode(ReadModeHeadPatternString, headPattern)
	}
	return
}

func NewFileDirReader(meta *Meta, conf conf.MapConf) (reader Reader, err error) {
	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	logpath, err := conf.GetString(KeyLogPath)
	if err != nil {
		return
	}
	bufSize, _ := conf.GetIntOr(KeyBufSize, defaultBufSize)

	// 默认不读取隐藏文件
	ignoreHidden, _ := conf.GetBoolOr(KeyIgnoreHiddenFile, true)
	ignoreFileSuffix, _ := conf.GetStringListOr(KeyIgnoreFileSuffix, defaultIgnoreFileSuffix)
	validFilesRegex, _ := conf.GetStringOr(KeyValidFilePattern, "*")
	newfileNewLine, _ := conf.GetBoolOr(KeyNewFileNewLine, false)
	fr, err := NewSeqFile(meta, logpath, ignoreHidden, newfileNewLine, ignoreFileSuffix, validFilesRegex, whence)
	if err != nil {
		return
	}
	return NewReaderSize(fr, meta, bufSize)
}

func NewSingleFileReader(meta *Meta, conf conf.MapConf) (reader Reader, err error) {

	logpath, err := conf.GetString(KeyLogPath)
	if err != nil {
		return
	}
	bufSize, _ := conf.GetIntOr(KeyBufSize, defaultBufSize)
	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	errDirectReturn, _ := conf.GetBoolOr(KeyErrDirectReturn, false)

	fr, err := NewSingleFile(meta, logpath, whence, errDirectReturn)
	if err != nil {
		return
	}
	return NewReaderSize(fr, meta, bufSize)
}
