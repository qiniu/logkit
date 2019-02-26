package reader

import (
	"errors"
	"fmt"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/reader/config"
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

type OnceReader interface {
	ReadDone() bool
}

// FileReader reader 接口方法
type FileReader interface {
	Name() string
	Source() string
	Read(p []byte) (n int, err error)
	Close() error
	SyncMeta() error
}

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
		return nil, fmt.Errorf("reader type unsupported : %v", mode)
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

type SourceIndex struct {
	Source string
	Index  int
}

type NewSourceRecorder interface {
	NewSourceIndex() []SourceIndex
}
