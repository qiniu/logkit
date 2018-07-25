package reader

import (
	"errors"
	"fmt"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
)

// Reader 代表了一个通用的行读取器
type Reader interface {
	// Name 用于返回读取器的具体名称
	Name() string
	// SetMode 用于设置读取器的匹配模式
	SetMode(mode string, v interface{}) error
	// Source 用于返回当前读取的数据源
	Source() string
	// ReadLine 用于向读取器请求返回一行数据
	ReadLine() (string, error)
	// SyncMeta 用于通知读取器保存同步相关元数据
	SyncMeta()
	// Close 用于关闭读取器
	Close() error
}

// DaemonReader 代表了一个需要守护线程的读取器
type DaemonReader interface {
	// Start 用于非阻塞的启动读取器对应的守护线程，需要读取器自行负责其生命周期
	Start() error
}

// DataReader 代表了一个可直接读取内存数据结构的读取器
type DataReader interface {
	// ReadData 用于读取一条数据以及数据的实际读取字节
	ReadData() (Data, int64, error)
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

// FileReader's conf keys
const (
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
	KeyTagFile           = "tag_file"
	KeyHeadPattern       = "head_pattern"
	KeyNewFileNewLine    = "newfile_newline"
	KeySkipFileFirstLine = "skip_first_line"

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
	KeyMysqlHistoryAll  = "mysql_history_all"
	KyeMysqlTable       = "mysql_table"

	KeySQLSchema        = "sql_schema"
	KeyMagicLagDuration = "magic_lag_duration"

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

var DefaultIgnoreFileSuffixes = []string{
	".pid", ".swap", ".go", ".conf", ".tar.gz", ".tar", ".zip",
	".a", ".o", ".so"}

// FileReader's modes
const (
	ModeDir        = "dir"
	ModeFile       = "file"
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
	ModeScript     = "script"
	ModeSnmp       = "snmp"
	ModeCloudWatch = "cloudwatch"
	ModeCloudTrail = "cloudtrail"
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

const (
	StatusInit int32 = iota
	StatusStopped
	StatusStopping
	StatusRunning
)

func NewReader(conf conf.MapConf, errDirectReturn bool) (reader Reader, err error) {
	rs := NewRegistry()
	return rs.NewReader(conf, errDirectReturn)
}

//Deprecated: NewFileBufReader 名字上有歧义，实际上就是NewReader，包括任何类型，保证兼容性，保留
func NewFileBufReader(conf conf.MapConf, errDirectReturn bool) (reader Reader, err error) {
	rs := NewRegistry()
	return rs.NewReader(conf, errDirectReturn)
}

type Constructor func(*Meta, conf.MapConf) (Reader, error)

// registeredConstructors keeps a list of all available reader constructors can be registered by Registry.
var registeredConstructors = map[string]Constructor{}

// RegisterConstructor adds a new constructor for a given type of reader.
func RegisterConstructor(typ string, c Constructor) {
	registeredConstructors[typ] = c
}

// Registry reader 的工厂类。可以注册自定义reader
type Registry struct {
	readerTypeMap map[string]func(*Meta, conf.MapConf) (Reader, error)
}

func NewRegistry() *Registry {
	ret := &Registry{
		readerTypeMap: map[string]func(*Meta, conf.MapConf) (Reader, error){},
	}
	ret.RegisterReader(ModeDir, NewFileDirReader)
	ret.RegisterReader(ModeFile, NewSingleFileReader)

	for typ, c := range registeredConstructors {
		ret.RegisterReader(typ, c)
	}

	return ret
}

func (reg *Registry) RegisterReader(readerType string, constructor Constructor) error {
	_, exist := reg.readerTypeMap[readerType]
	if exist {
		return errors.New("readerType " + readerType + " has been existed")
	}
	reg.readerTypeMap[readerType] = constructor
	return nil
}

func (reg *Registry) NewReader(conf conf.MapConf, errDirectReturn bool) (reader Reader, err error) {
	meta, err := NewMetaWithConf(conf)
	if err != nil {
		log.Warn(err)
		return
	}
	return reg.NewReaderWithMeta(conf, meta, errDirectReturn)
}

func (reg *Registry) NewReaderWithMeta(conf conf.MapConf, meta *Meta, errDirectReturn bool) (Reader, error) {
	if errDirectReturn {
		conf[KeyErrDirectReturn] = Bool2String(errDirectReturn)
	}
	mode, _ := conf.GetStringOr(KeyMode, ModeDir)
	headPattern, _ := conf.GetStringOr(KeyHeadPattern, "")

	constructor, exist := reg.readerTypeMap[mode]
	if !exist {
		return nil, fmt.Errorf("reader type unsupperted : %v", mode)
	}

	reader, err := constructor(meta, conf)
	if err != nil {
		return nil, err
	}
	if headPattern != "" {
		err = reader.SetMode(ReadModeHeadPatternString, headPattern)
		if err != nil {
			return nil, err
		}
	}

	return reader, nil
}

func NewFileDirReader(meta *Meta, conf conf.MapConf) (reader Reader, err error) {
	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	logpath, err := conf.GetString(KeyLogPath)
	if err != nil {
		return
	}
	bufSize, _ := conf.GetIntOr(KeyBufSize, DefaultBufSize)

	// 默认不读取隐藏文件
	ignoreHidden, _ := conf.GetBoolOr(KeyIgnoreHiddenFile, true)
	ignoreFileSuffix, _ := conf.GetStringListOr(KeyIgnoreFileSuffix, DefaultIgnoreFileSuffixes)
	validFilesRegex, _ := conf.GetStringOr(KeyValidFilePattern, "*")
	newfileNewLine, _ := conf.GetBoolOr(KeyNewFileNewLine, false)
	skipFirstLine, _ := conf.GetBoolOr(KeySkipFileFirstLine, false)
	fr, err := NewSeqFile(meta, logpath, ignoreHidden, newfileNewLine, ignoreFileSuffix, validFilesRegex, whence)
	if err != nil {
		return
	}
	fr.SkipFileFirstLine = skipFirstLine
	return NewReaderSize(fr, meta, bufSize)
}

func NewSingleFileReader(meta *Meta, conf conf.MapConf) (reader Reader, err error) {
	logpath, err := conf.GetString(KeyLogPath)
	if err != nil {
		return
	}
	bufSize, _ := conf.GetIntOr(KeyBufSize, DefaultBufSize)
	whence, _ := conf.GetStringOr(KeyWhence, WhenceOldest)
	errDirectReturn, _ := conf.GetBoolOr(KeyErrDirectReturn, true)

	fr, err := NewSingleFile(meta, logpath, whence, errDirectReturn)
	if err != nil {
		return
	}
	return NewReaderSize(fr, meta, bufSize)
}
