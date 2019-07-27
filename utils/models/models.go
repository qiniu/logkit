package models

import (
	"fmt"
	"regexp"
	"runtime"
	"sync/atomic"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils/equeue"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const (
	B = 1 << (iota * 10)
	KB
	MB
	GB
)

const (
	CheckPattern       = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	CheckPatternKey    = "^[a-zA-Z_.][a-zA-Z0-9_.]{0,127}$"
	DefaultEncodingWay = "UTF-8"

	KeyType   = "type"
	ProcessAt = "process_at"
	Local     = "local"
	Server    = "server"

	GlobalKeyName = "name"
	ExtraInfo     = "extra_info"
	/* 该选项兼容如下配置 KeyPandoraExtraInfo */

	KeyCore     = "core"
	KeyHostName = "hostname"
	KeyOsInfo   = "osinfo"
	KeyLocalIp  = "localip"

	ContentTypeHeader     = "Content-Type"
	ContentEncodingHeader = "Content-Encoding"

	ApplicationJson = "application/json"
	TextPlain       = "text/plain"
	ApplicationGzip = "application/gzip"

	KeyPandoraStash      = "pandora_stash"       // 当只有一条数据且 sendError 时候，将其转化为 raw 发送到 pandora_stash 这个字段
	KeyPandoraSeparateId = "pandora_separate_id" // 当一条数据大于2M且 sendError 时候，将其切片，切片记录到 pandora_separate_id 这个字段
	TypeIP               = "ip"                  // schema ip

	SchemaFreeTokensPrefix = "schema_free_tokens_"
	LogDBTokensPrefix      = "logdb_tokens_"
	TsDBTokensPrefix       = "tsdb_tokens_"
	KodoTokensPrefix       = "kodo_tokens_"

	KeyRunnerName = "runner_name"

	DefaultDirPerm  = 0755
	DefaultFilePerm = 0600

	DefaultMaxBatchSize = 2 * MB

	DefaultSendIntervalSeconds = 60

	DefaultTruncateMaxSize = 1024

	DefaultErrorsListCap = 100

	PipeLineError = "ErrorMessage="

	Text        = "text"
	Checkbox    = "checkbox"
	Radio       = "radio"
	InputNumber = "inputNumber"

	LONG   = "long"
	FLOAT  = "float"
	STRING = "string"
	DATE   = "date"
	DROP   = "drop"

	DefaultSelfRunnerName = DefaultInternalPrefix + "CollectLogRunner"
	DefaultInternalPrefix = "LogkitInternal"
)

var (
	MaxProcs                     = 1
	NumCPU                       = runtime.NumCPU()
	LogkitAutoCreateDescription  = "由logkit日志收集自动创建"
	MetricAutoCreateDescription  = "由logkit监控收集自动创建"
	SelfLogAutoCreateDescription = "由logkit收集自身日志创建"

	// matches named captures that contain a modifier.
	//   ie,
	//     %{NUMBER:bytes:long}
	//     %{IPORHOST:clientip:date}
	//     %{HTTPDATE:ts1:float}
	ModifierRe = regexp.MustCompile(`%{\w+:(\w+):(long|string|date|float|drop)}`)
	// matches a plain pattern name. ie, %{NUMBER}
	PatternOnlyRe = regexp.MustCompile(`%{(\w+)}`)

	Encoding = []interface{}{"UTF-8", "UTF-16", "US-ASCII", "ISO-8859-1",
		"GBK", "latin1", "GB18030", "EUC-JP", "UTF-16BE", "UTF-16LE", "Big5", "Shift_JIS",
		"ISO-8859-2", "ISO-8859-3", "ISO-8859-4", "ISO-8859-5", "ISO-8859-6", "ISO-8859-7",
		"ISO-8859-8", "ISO-8859-9", "ISO-8859-10", "ISO-8859-11", "ISO-8859-13",
		"ISO-8859-14", "ISO-8859-15", "ISO-8859-16", "macos-0_2-10.2", "macos-6_2-10.4",
		"macos-7_3-10.2", "macos-29-10.2", "macos-35-10.2", "windows-1250", "windows-1251",
		"windows-1252", "windows-1253", "windows-1254", "windows-1255", "windows-1256",
		"windows-1257", "windows-1258", "windows-874", "IBM037", "ibm-273_P100-1995",
		"ibm-277_P100-1995", "ibm-278_P100-1995", "ibm-280_P100-1995", "ibm-284_P100-1995",
		"ibm-285_P100-1995", "ibm-290_P100-1995", "ibm-297_P100-1995", "ibm-420_X120-1999",
		//此处省略大量IBM的字符集，太多，等用户需要再加
		"KOI8-R", "KOI8-U", "ebcdic-xml-us"}
)

type Option struct {
	KeyName            string
	ChooseOnly         bool
	Element            string // 前端显示类型
	ChooseOptions      []interface{}
	Default            interface{}
	DefaultNoUse       bool // 是否使用默认值，true为不使用默认值，false为使用默认值
	Description        string
	CheckRegex         string
	Style              string `json:"style"`
	Required           bool   `json:"required"` // 是否必填
	Placeholder        string `json:"placeholder"`
	Type               string `json:"Type,omitempty"`
	Secret             bool
	Advance            bool                   `json:"advance,omitempty"`
	AdvanceDepend      string                 `json:"advance_depend,omitempty"`
	AdvanceDependValue interface{}            `json:"advance_depend_value,omitempty"`
	ToolTip            string                 `json:"tooltip,omitempty"` // 该选项说明
	MutiDefaultSource  bool                   `json:"muti_default_source"`
	MultiDefault       map[string]interface{} `json:"multi_default,omitempty"`
	MultiDefaultDepend string                 `json:"multi_default_depend,omitempty"`
}

type GrokLabel struct {
	Name  string
	Value string
}

func NewGrokLabel(name, dataValue string) GrokLabel {
	return GrokLabel{
		Name:  name,
		Value: dataValue,
	}
}

type KeyValue struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	SortKey string `json:"sort_key"`
}

// Data store as use key/value map
type Data map[string]interface{}

type AuthTokens struct {
	RunnerName   string
	SenderIndex  int
	SenderTokens conf.MapConf
}

type Pandora struct {
	Name     string `json:"pandora_name"`
	Region   string `json:"pandora_region"`
	Pipeline string `json:"pandora_pipeline"`
	LogDB    string `json:"pandora_logdb"`
	AK       string `json:"pandora_ak"`
	SK       string `json:"pandora_sk"`
}

type LagInfo struct {
	Size     int64  `json:"size"`
	SizeUnit string `json:"sizeunit"`
	Ftlags   int64  `json:"ftlags"`
	Total    int64  `json:"total"`
}

type StatsError struct {
	StatsInfo
	SendError           *reqerr.SendError `json:"error"`
	Ft                  bool              `json:"-"`
	FtNotRetry          bool              `json:"-"`
	DatasourceSkipIndex []int
	RemainDatas         []Data
}

type StatsInfo struct {
	Errors     int64  `json:"errors"`
	Success    int64  `json:"success"`
	Speed      int64  `json:"speed"`
	Trend      string `json:"trend"`
	LastError  string `json:"last_error"`
	FtQueueLag int64  `json:"-"`
}

type ErrorStatistic struct {
	ErrorSlice []equeue.ErrorInfo `json:"error_slice"`

	//对于runnerstatus的CompatibleErrorResult结构体来说，下面的三个都没用，只是为了保证兼容性
	//服务端用这个结构体也没有用到下面这三个成员
	MaxSize int `json:"max_size"`

	//以下为v1.0.4及以前版本的结构，为了兼容保留
	Front int `json:"front"`
	Rear  int `json:"rear"`
}

func (e ErrorStatistic) IsNewVersion() bool {
	if e.Front == 0 && e.Rear == 0 && len(e.ErrorSlice) > 0 {
		return true
	}
	return false
}

func (e ErrorStatistic) GetMaxSize() int {
	if e.MaxSize <= 0 {
		return DefaultErrorsListCap
	}
	return e.MaxSize
}

func (se *StatsError) AddSuccess() {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Success, 1)
}

func (se *StatsError) AddSuccessNum(n int) {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Success, int64(n))
}

func (se *StatsError) AddErrors() {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Errors, 1)
}

func (se *StatsError) AddErrorsNum(n int) {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Errors, int64(n))
}

func (se *StatsError) Error() string {
	if se == nil {
		return ""
	}
	return fmt.Sprintf("success %d errors %d last error %s, send error detail %v", se.Success, se.Errors, se.LastError, se.SendError)
}

func (se *StatsError) ErrorIndexIn(idx int) bool {
	for _, v := range se.DatasourceSkipIndex {
		if v == idx {
			return true
		}
	}
	return false
}

type KeyValueSlice []KeyValue

func (slice KeyValueSlice) Len() int {
	return len(slice)
}

func (slice KeyValueSlice) Less(i, j int) bool {
	return slice[i].SortKey < slice[j].SortKey
}

func (slice KeyValueSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
