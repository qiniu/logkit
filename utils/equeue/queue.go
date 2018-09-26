package equeue

import (
	"container/ring"
	"strings"
	"time"
)

const (
	DefaultErrorsListCap = 100
	PipeLineError        = "ErrorMessage="
)

//保证内部成员的数值只能由方法调用而改变，不能直接修改
type ErrorQueue struct {
	r        *ring.Ring //永远指向最新(最后)元素
	l        *ring.Ring //永远指向第早(最前)元素
	capacity int
	size     int
}

type ErrorInfo struct {
	Error     string `json:"error"`
	Timestamp int64  `json:"timestamp"`
	Count     int64  `json:"count"`
}

func NewError(msg string) ErrorInfo {
	return ErrorInfo{
		Error:     msg,
		Timestamp: time.Now().UnixNano(),
		Count:     0,
	}
}

func New(capacity int) *ErrorQueue {
	if capacity <= 0 {
		capacity = DefaultErrorsListCap
	}
	return &ErrorQueue{
		capacity: capacity,
	}
}

// 向队列中添加单个元素
func (q *ErrorQueue) Put(e ErrorInfo) {
	if q.EqualLast(e) {
		lerr := q.r.Value.(*ErrorInfo)
		lerr.Count++
		lerr.Timestamp = e.Timestamp
		return
	}
	if e.Count <= 0 {
		e.Count = 1
	}
	if q.size < q.capacity {
		n := ring.New(1)
		n.Value = &e
		if q.r == nil {
			q.r = n
			q.l = n
		} else {
			q.r.Link(n)
			q.r = q.r.Next()
		}
		q.size++
	} else {
		q.r = q.r.Next()
		q.r.Value = &e
		q.l = q.r.Next()
	}
}

// 向队列中添加元素
func (q *ErrorQueue) Append(errors []ErrorInfo) {
	for _, e := range errors {
		q.Put(e)
	}
}

// 获取队列中最后一个元素
func (q *ErrorQueue) End() ErrorInfo {
	if q.Empty() {
		return ErrorInfo{}
	}
	e := q.r.Value.(*ErrorInfo)
	return *e
}

// 获取队列中第一个元素
func (q *ErrorQueue) Front() ErrorInfo {
	if q.Empty() {
		return ErrorInfo{}
	}
	e := q.l.Value.(*ErrorInfo)
	return *e
}

// 获取队列中第N个元素
func (q *ErrorQueue) GetN(n int) ErrorInfo {
	if q.Empty() {
		return ErrorInfo{}
	}
	n = n % q.size
	tmp := q.l
	for i := 1; i < n; i++ {
		tmp = tmp.Next()
	}
	e := tmp.Value.(*ErrorInfo)
	return *e
}

func (q *ErrorQueue) Size() int {
	return q.size
}

func (q *ErrorQueue) Empty() bool {
	if q == nil {
		return true
	}
	if q.r == nil {
		return true
	}
	if q.size <= 0 {
		return true
	}
	return false
}

// 按进出顺序复制到数组中
func (q *ErrorQueue) List() []ErrorInfo {
	if q.Empty() {
		return nil
	}

	errorInfoList := make([]ErrorInfo, 0, q.size)
	tmp := q.l
	for i := 0; i < q.size; i++ {
		errorInfoList = append(errorInfoList, *tmp.Value.(*ErrorInfo))
		tmp = tmp.Next()
	}
	return errorInfoList
}

// 返回队列实际容量
func (q *ErrorQueue) GetMaxSize() int {
	return q.capacity
}

// 将另一个queue复制到当前queue中
func (q *ErrorQueue) Clone() *ErrorQueue {
	if q.Empty() {
		if q != nil {
			return New(q.capacity)
		}
		return nil
	}
	nq := New(q.capacity)
	nq.l = ring.New(q.Size())
	nq.r = nq.l
	nq.size = q.size
	tmp := q.l
	for i := 0; i < q.size; i++ {
		e := tmp.Value.(*ErrorInfo)
		nq.r.Value = &ErrorInfo{
			Error:     e.Error,
			Timestamp: e.Timestamp,
			Count:     e.Count,
		}
		tmp = tmp.Next()
		nq.r = nq.r.Next()
	}
	return nq
}

// 比较是否和最后的error类型相等，方便增加计数而不是直接插入元素
func (q *ErrorQueue) EqualLast(e ErrorInfo) bool {
	if q.Empty() {
		return false
	}
	//永远不会断言失败，因为Put进去的时候就控制了类型
	lerr := q.r.Value.(*ErrorInfo)
	return EqualErrors(lerr.Error, e.Error)
}

func EqualErrors(old, new string) bool {
	if strings.EqualFold(old, new) {
		return true
	}
	lastErrorIdx := strings.Index(old, PipeLineError)
	currentIdx := strings.Index(new, PipeLineError)
	if lastErrorIdx != -1 && currentIdx != -1 {
		currentErrArr := strings.SplitN(new[currentIdx:], ":", 2)
		lastErrorArr := strings.SplitN(old[lastErrorIdx:], ":", 2)
		if strings.EqualFold(currentErrArr[0], lastErrorArr[0]) {
			return true
		}
	}
	return false
}
