package models

import (
	"fmt"
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

	DefaultTruncateMaxSize = 1024

	DefaultErrorsListCap = 100

	PipeLineError = "ErrorMessage="

	Text        = "text"
	Checkbox    = "checkbox"
	Radio       = "radio"
	InputNumber = "inputNumber"
)

var (
	MaxProcs                    = 1
	NumCPU                      = runtime.NumCPU()
	LogkitAutoCreateDescription = "由logkit日志收集自动创建"
	MetricAutoCreateDescription = "由logkit监控收集自动创建"
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
	Errors     int64   `json:"errors"`
	Success    int64   `json:"success"`
	Speed      float64 `json:"speed"`
	Trend      string  `json:"trend"`
	LastError  string  `json:"last_error"`
	FtQueueLag int64   `json:"-"`
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
