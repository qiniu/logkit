package models

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/qiniu/logkit/conf"
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
	TestPlain       = "text/plain"
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
	Advance            bool        `json:"advance,omitempty"`
	AdvanceDepend      string      `json:"advance_depend,omitempty"`
	AdvanceDependValue interface{} `json:"advance_depend_value,omitempty"`
	ToolTip            string      `json:"tooltip,omitempty"` // 该选项说明
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
	ErrorDetail         error `json:"error"`
	Ft                  bool  `json:"-"`
	FtNotRetry          bool  `json:"-"`
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

type ErrorQueue struct {
	lock       sync.RWMutex
	ErrorSlice []ErrorInfo `json:"error_slice"`
	Front      int         `json:"front"`
	Rear       int         `json:"rear"`
	MaxSize    int         `json:"max_size"`
}

type ErrorInfo struct {
	Error     string `json:"error"`
	Timestamp int64  `json:"timestamp"`
	Count     int64  `json:"count"`
}

func NewErrorQueue(maxSize int) *ErrorQueue {
	if maxSize <= 0 {
		maxSize = DefaultErrorsListCap
	}
	return &ErrorQueue{
		ErrorSlice: make([]ErrorInfo, maxSize+1), // 多余的1个空间用来判断队列是否满了
		MaxSize:    maxSize + 1,
	}
}

// 向队列中添加单个元素
func (queue *ErrorQueue) Put(e ErrorInfo) {
	if queue.EqualLast(e) {
		queue.lock.Lock()
		last := (queue.Rear + queue.MaxSize - 1) % queue.MaxSize
		queue.ErrorSlice[last].Count++
		queue.ErrorSlice[last].Timestamp = e.Timestamp
		queue.lock.Unlock()
		return
	}

	queue.lock.Lock()
	if (queue.Rear+1)%queue.MaxSize == queue.Front {
		queue.Front = (queue.Front + 1) % queue.MaxSize
	}
	queue.ErrorSlice[queue.Rear] = e
	queue.ErrorSlice[queue.Rear].Count = 1 // 个数增加 1
	queue.Rear = (queue.Rear + 1) % queue.MaxSize
	queue.lock.Unlock()
}

// 向队列中添加元素
func (queue *ErrorQueue) Append(errors []ErrorInfo) {
	queue.lock.Lock()
	for _, e := range errors {
		if (queue.Rear+1)%queue.MaxSize == queue.Front {
			queue.Front = (queue.Front + 1) % queue.MaxSize
		}
		queue.ErrorSlice[queue.Rear] = e
		queue.Rear = (queue.Rear + 1) % queue.MaxSize
	}
	queue.lock.Unlock()
}

// 获取队列中最后一个元素
func (queue *ErrorQueue) Get() ErrorInfo {
	if queue.IsEmpty() {
		return ErrorInfo{}
	}

	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.ErrorSlice[(queue.Rear-1+queue.MaxSize)%queue.MaxSize]
}

func (queue *ErrorQueue) Size() int {
	if queue.IsEmpty() {
		return 0
	}

	queue.lock.RLock()
	defer queue.lock.RUnlock()
	return (queue.Rear - queue.Front + queue.MaxSize) % queue.MaxSize
}

func (queue *ErrorQueue) IsEmpty() bool {
	if queue == nil {
		return true
	}

	queue.lock.RLock()
	defer queue.lock.RUnlock()
	return queue.Rear == queue.Front
}

// 按进出顺序复制到数组中
func (queue *ErrorQueue) Sort() []ErrorInfo {
	if queue.IsEmpty() {
		return nil
	}

	var errorInfoList []ErrorInfo
	queue.lock.RLock()
	for i := queue.Front; i != queue.Rear; i = (i + 1) % queue.MaxSize {
		errorInfoList = append(errorInfoList, queue.ErrorSlice[i])
	}
	queue.lock.RUnlock()
	return errorInfoList
}

// 返回队列实际容量
func (queue *ErrorQueue) GetMaxSize() int {
	return queue.MaxSize - 1
}

// 将另一个queue复制到当前queue中
func (queue *ErrorQueue) CopyQueue(src *ErrorQueue) {
	if src.IsEmpty() {
		return
	}

	src.lock.Lock()
	for i := src.Front; i != src.Rear; i = (i + 1) % src.MaxSize {
		queue.Copy(src.ErrorSlice[i])
	}
	queue.Front = src.Front
	queue.Rear = src.Rear
	src.lock.Unlock()
}

// 将另一个queue复制到当前queue中
func (queue *ErrorQueue) Set(index int, e ErrorInfo) {
	queue.lock.Lock()
	if index < queue.Front || index > queue.Rear {
		return
	}
	queue.ErrorSlice[index] = e
	queue.ErrorSlice[index].Count = e.Count
	queue.ErrorSlice[index].Timestamp = e.Timestamp
	if index == queue.Rear {
		queue.Rear = (queue.Rear + 1) % queue.MaxSize
	}
	queue.lock.Unlock()
}

// 将另一个queue复制到当前queue中
func (queue *ErrorQueue) Copy(e ErrorInfo) {
	queue.lock.Lock()
	if (queue.Rear+1)%queue.MaxSize == queue.Front {
		queue.Front = (queue.Front + 1) % queue.MaxSize
	}
	queue.ErrorSlice[queue.Rear] = e
	queue.Rear = (queue.Rear + 1) % queue.MaxSize
	queue.lock.Unlock()
}

// 获取 queue 中 front rear之间的数据
func (queue *ErrorQueue) GetErrorSlice(front, rear int) []ErrorInfo {
	if queue.IsEmpty() {
		return nil
	}

	var errorInfoArr []ErrorInfo
	queue.lock.Lock()
	if front%queue.MaxSize < queue.Front {
		front = queue.Front
	}
	if rear%queue.MaxSize > queue.Rear {
		rear = queue.Rear
	}
	for i := front % queue.MaxSize; i != rear; i = (i + 1) % queue.MaxSize {
		if queue.ErrorSlice[i].Count != 0 {
			errorInfoArr = append(errorInfoArr, queue.ErrorSlice[i])
		}
	}
	queue.lock.Unlock()
	return errorInfoArr
}

// 向队列中添加元素
func (queue *ErrorQueue) EqualLast(e ErrorInfo) bool {
	if queue.IsEmpty() {
		return false
	}
	queue.lock.RLock()
	defer queue.lock.RUnlock()
	last := (queue.Rear + queue.MaxSize - 1) % queue.MaxSize
	lastError := queue.ErrorSlice[last].Error
	current := e.Error
	if strings.EqualFold(lastError, current) {
		return true
	}

	lastErrorIdx := strings.Index(lastError, PipeLineError)
	currentIdx := strings.Index(current, PipeLineError)
	if lastErrorIdx != -1 && currentIdx != -1 {
		currentErrArr := strings.SplitN(current[currentIdx:], ":", 2)
		lastErrorArr := strings.SplitN(lastError[lastErrorIdx:], ":", 2)
		if strings.EqualFold(currentErrArr[0], lastErrorArr[0]) {
			return true
		}
	}
	return false
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
	return fmt.Sprintf("success %v errors %v errordetail %v", se.Success, se.Errors, se.ErrorDetail)
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
