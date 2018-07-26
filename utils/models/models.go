package models

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/qiniu/logkit/conf"
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

	SchemaFreeTokensPrefix = "schema_free_tokens_"
	LogDBTokensPrefix      = "logdb_tokens_"
	TsDBTokensPrefix       = "tsdb_tokens_"
	KodoTokensPrefix       = "kodo_tokens_"

	KeyRunnerName = "runner_name"

	DefaultDirPerm  = 0755
	DefaultFilePerm = 0600

	DefaultMaxBatchSize = 2 * 1024 * 1024

	DefaultErrorsListCap = 100

	PipeLineError = "ErrorMessage="

	Text        = "text"
	Checkbox    = "checkbox"
	Radio       = "radio"
	InputNumber = "inputNumber"
)

type Option struct {
	KeyName       string
	ChooseOnly    bool
	Element       string
	ChooseOptions []interface{}
	Default       interface{}
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
	Style         string `json:"style"`
	Required      bool   `json:"required"`
	Placeholder   string `json:"placeholder"`
	Type          string `json:"Type,omitempty"`
	Secret        bool
	Advance       bool   `json:"advance,omitempty"`
	AdvanceDepend string `json:"advance_depend,omitempty"`
	ToolTip       string `json:"tooltip,omitempty"`
	ToolTipActive bool   `json:"tooltip_active,omitempty"`
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
	ErrorSlice []ErrorInfo `json:"error_slice"`
	Front      int         `json:"front"`
	Rear       int         `json:"rear"`
	maxSize    int         `json:"max_size"`
	mutex      sync.RWMutex
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
		make([]ErrorInfo, maxSize+1), // 多余的1个空间用来判断队列是否满了
		0,
		0,
		maxSize + 1,
		sync.RWMutex{},
	}
}

// 向队列中添加单个元素
func (entry *ErrorQueue) Put(e ErrorInfo) {
	if entry.EqualLast(e) {
		entry.mutex.Lock()
		last := (entry.Rear + entry.maxSize - 1) % entry.maxSize
		entry.ErrorSlice[last].Count++
		entry.ErrorSlice[last].Timestamp = e.Timestamp
		entry.mutex.Unlock()
		return
	}

	entry.mutex.Lock()
	if (entry.Rear+1)%entry.maxSize == entry.Front {
		entry.Front = (entry.Front + 1) % entry.maxSize
	}
	entry.ErrorSlice[entry.Rear] = e
	entry.ErrorSlice[entry.Rear].Count = 1 // 个数增加 1
	entry.Rear = (entry.Rear + 1) % entry.maxSize
	entry.mutex.Unlock()
}

// 向队列中添加元素
func (entry *ErrorQueue) Append(errors []ErrorInfo) {
	entry.mutex.Lock()
	for _, e := range errors {
		if (entry.Rear+1)%entry.maxSize == entry.Front {
			entry.Front = (entry.Front + 1) % entry.maxSize
		}
		entry.ErrorSlice[entry.Rear] = e
		entry.Rear = (entry.Rear + 1) % entry.maxSize
	}
	entry.mutex.Unlock()
}

// 获取队列中最后一个元素
func (entry *ErrorQueue) Get() ErrorInfo {
	if entry.IsEmpty() {
		return ErrorInfo{}
	}

	entry.mutex.Lock()
	defer entry.mutex.Unlock()
	return entry.ErrorSlice[(entry.Rear-1+entry.maxSize)%entry.maxSize]
}

func (entry *ErrorQueue) Size() int {
	if entry.IsEmpty() {
		return 0
	}

	entry.mutex.RLock()
	defer entry.mutex.RUnlock()
	return (entry.Rear - entry.Front + entry.maxSize) % entry.maxSize
}

func (entry *ErrorQueue) IsEmpty() bool {
	entry.mutex.RLock()
	defer entry.mutex.RUnlock()
	return entry.Rear == entry.Front
}

// 按进出顺序复制到数组中
func (entry *ErrorQueue) Sort() []ErrorInfo {
	if entry.IsEmpty() {
		return nil
	}

	var errorInfoList []ErrorInfo
	entry.mutex.RLock()
	for i := entry.Front; i != entry.Rear; i = (i + 1) % entry.maxSize {
		errorInfoList = append(errorInfoList, entry.ErrorSlice[i])
	}
	entry.mutex.RUnlock()
	return errorInfoList
}

// 返回队列实际容量
func (entry *ErrorQueue) GetMaxSize() int {
	return entry.maxSize - 1
}

// 将另一个queue复制到当前queue中
func (entry *ErrorQueue) CopyQueue(src *ErrorQueue) {
	if src.IsEmpty() {
		return
	}

	src.mutex.Lock()
	for i := src.Front; i != src.Rear; i = (i + 1) % src.maxSize {
		entry.Copy(src.ErrorSlice[i])
	}
	entry.Front = src.Front
	entry.Rear = src.Rear
	src.mutex.Unlock()
}

// 将另一个queue复制到当前queue中
func (entry *ErrorQueue) Set(index int, e ErrorInfo) {
	entry.mutex.Lock()
	if index < entry.Front || index > entry.Rear {
		return
	}
	entry.ErrorSlice[index] = e
	entry.ErrorSlice[index].Count = e.Count
	entry.ErrorSlice[index].Timestamp = e.Timestamp
	if index == entry.Rear {
		entry.Rear = (entry.Rear + 1) % entry.maxSize
	}
	entry.mutex.Unlock()
}

// 将另一个queue复制到当前queue中
func (entry *ErrorQueue) Copy(e ErrorInfo) {
	entry.mutex.Lock()
	if (entry.Rear+1)%entry.maxSize == entry.Front {
		entry.Front = (entry.Front + 1) % entry.maxSize
	}
	entry.ErrorSlice[entry.Rear] = e
	entry.Rear = (entry.Rear + 1) % entry.maxSize
	entry.mutex.Unlock()
}

// 获取 queue 中 front rear之间的数据
func (entry *ErrorQueue) GetErrorSlice(front, rear int) []ErrorInfo {
	if entry.IsEmpty() {
		return nil
	}

	var errorInfoArr []ErrorInfo
	entry.mutex.Lock()
	if front%entry.maxSize < entry.Front {
		front = entry.Front
	}
	if rear%entry.maxSize > entry.Rear {
		rear = entry.Rear
	}
	for i := front % entry.maxSize; i != rear; i = (i + 1) % entry.maxSize {
		if entry.ErrorSlice[i].Count != 0 {
			errorInfoArr = append(errorInfoArr, entry.ErrorSlice[i])
		}
	}
	entry.mutex.Unlock()
	return errorInfoArr
}

// 向队列中添加元素
func (entry *ErrorQueue) EqualLast(e ErrorInfo) bool {
	if entry.IsEmpty() {
		return false
	}
	entry.mutex.RLock()
	defer entry.mutex.RUnlock()
	last := (entry.Rear + entry.maxSize - 1) % entry.maxSize
	lastError := entry.ErrorSlice[last].Error
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
	slice[i].Key, slice[j].Key = slice[j].Key, slice[i].Key
	slice[i].Value, slice[j].Value = slice[j].Value, slice[i].Value
	slice[i].SortKey, slice[j].SortKey = slice[j].SortKey, slice[i].SortKey
}
