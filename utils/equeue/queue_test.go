package equeue

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorQueue(t *testing.T) {
	var testErrorQueue *ErrorQueue
	assert.True(t, testErrorQueue.Empty())

	testErrorQueue = New(DefaultErrorsListCap)
	assert.Equal(t, DefaultErrorsListCap, testErrorQueue.capacity)
	assert.True(t, testErrorQueue.Empty())
	assert.Equal(t, 0, testErrorQueue.Size())

	testErrorQueue.Put(ErrorInfo{"test error", 123456, 0})
	assert.False(t, testErrorQueue.Empty())
	assert.Equal(t, 1, testErrorQueue.Size())
	errorsList := testErrorQueue.List()
	assert.Equal(t, 1, len(errorsList))
	assert.Equal(t, int64(1), testErrorQueue.Front().Count)

	for i := 0; i < 80; i++ {
		testErrorQueue.Put(ErrorInfo{"test error", 123456, 0})
	}
	assert.False(t, testErrorQueue.Empty())
	assert.Equal(t, 1, testErrorQueue.Size())
	assert.Equal(t, int64(81), testErrorQueue.Front().Count)
	errorsList = testErrorQueue.List()
	assert.Equal(t, 1, len(errorsList))
	assert.Equal(t, ErrorInfo{"test error", 123456, 81}, testErrorQueue.End())

	for i := 0; i < 180; i++ {
		testErrorQueue.Put(ErrorInfo{"test error" + strconv.Itoa(i), 123456, 0})
	}
	assert.False(t, testErrorQueue.Empty())
	assert.Equal(t, DefaultErrorsListCap, testErrorQueue.Size())
	errorsList = testErrorQueue.List()
	assert.Equal(t, DefaultErrorsListCap, len(errorsList))
	assert.Equal(t, int64(1), testErrorQueue.GetN(1).Count)
	assert.Equal(t, ErrorInfo{"test error179", 123456, 1}, testErrorQueue.End())

	expectError := ErrorInfo{
		Error:     "my test",
		Count:     10,
		Timestamp: 10000,
	}
	testErrorQueue.Append([]ErrorInfo{expectError})
	assert.False(t, testErrorQueue.Empty())
	assert.Equal(t, DefaultErrorsListCap, testErrorQueue.Size())
	errorsList = testErrorQueue.List()
	assert.Equal(t, DefaultErrorsListCap, len(errorsList))
	assert.Equal(t, int64(10), errorsList[DefaultErrorsListCap-1].Count)
	assert.Equal(t, expectError, testErrorQueue.End())
}

func TestErrRandRead(t *testing.T) {
	q := New(DefaultErrorsListCap)
	var elist []ErrorInfo
	for i := 0; i < 50; i++ {
		e := ErrorInfo{
			Error:     fmt.Sprintf("err: num%v", i),
			Timestamp: int64(i),
			Count:     int64(i + 1),
		}
		q.Put(e)
		elist = append(elist, e)
	}
	for i := 0; i < 10; i++ {
		tr := rand.Intn(50)
		assert.Equal(t, elist[tr], q.GetN(tr+1))
	}
	assert.Equal(t, elist, q.List())

	for i := 50; i < 102; i++ {
		e := ErrorInfo{
			Error:     fmt.Sprintf("err: num%v", i),
			Timestamp: int64(i),
			Count:     int64(i + 1),
		}
		q.Put(e)
	}
	elist = make([]ErrorInfo, 0)
	for i := 2; i < 102; i++ {
		e := ErrorInfo{
			Error:     fmt.Sprintf("err: num%v", i),
			Timestamp: int64(i),
			Count:     int64(i + 1),
		}
		elist = append(elist, e)
	}
	for i := 0; i < 10; i++ {
		tr := rand.Intn(100)
		assert.Equal(t, elist[tr], q.GetN(tr+1))
	}
	assert.Equal(t, elist, q.List())
}

func TestSize(t *testing.T) {
	q := New(51)
	assert.Equal(t, true, q.Empty())
	var elist []ErrorInfo
	for i := 0; i < 50; i++ {
		e := ErrorInfo{
			Error:     fmt.Sprintf("err: num%v", i),
			Timestamp: int64(i),
			Count:     int64(i + 1),
		}
		q.Put(e)
		elist = append(elist, e)
	}
	assert.Equal(t, elist, q.List())
	assert.Equal(t, 50, q.Size())

	assert.Equal(t, 51, q.GetMaxSize())

	for i := 50; i < 60; i++ {
		e := ErrorInfo{
			Error:     fmt.Sprintf("err: num%v", i),
			Timestamp: int64(i),
			Count:     int64(i + 1),
		}
		assert.Equal(t, false, q.EqualLast(e))
		q.Put(e)
		assert.Equal(t, true, q.EqualLast(e))
	}
	elist = make([]ErrorInfo, 0)
	for i := 9; i < 60; i++ {
		e := ErrorInfo{
			Error:     fmt.Sprintf("err: num%v", i),
			Timestamp: int64(i),
			Count:     int64(i + 1),
		}
		elist = append(elist, e)
	}
	assert.Equal(t, elist, q.List())
	assert.Equal(t, 51, q.GetMaxSize())
	assert.Equal(t, len(elist), q.Size())
}

func TestRandAppend(t *testing.T) {
	q := New(100)
	assert.Equal(t, true, q.Empty())
	var elist []ErrorInfo
	var total int
	for i := 0; i < 100; i++ {
		var te []ErrorInfo
		ni := rand.Intn(10) + 1
		for j := 0; j < ni; j++ {
			ri := rand.Intn(1000)
			te = append(te, ErrorInfo{
				Error:     fmt.Sprintf("err: num%v", ri),
				Timestamp: int64(ri),
				Count:     int64(ri + 1),
			})
		}
		total += ni
		q.Append(te)
		assert.Equal(t, true, q.EqualLast(te[ni-1]))
		elist = append(elist, te...)
	}
	elist = elist[total-100:]
	assert.Equal(t, elist, q.List())
}

func TestGetFirstLast(t *testing.T) {
	var maxsize = 50
	q := New(maxsize)
	assert.Equal(t, true, q.Empty())
	var elist []ErrorInfo
	var total int
	for i := 0; i < 100; i++ {
		var te []ErrorInfo
		ni := rand.Intn(10) + 1
		for j := 0; j < ni; j++ {
			ri := rand.Intn(1000)
			te = append(te, ErrorInfo{
				Error:     fmt.Sprintf("err: num%v", ri),
				Timestamp: int64(ri),
				Count:     int64(ri + 1),
			})
		}
		total += ni
		q.Append(te)
		assert.Equal(t, te[ni-1], q.End())
		elist = append(elist, te...)
		var firstidx int
		if total >= 50 {
			firstidx = total - 50
		}
		assert.Equal(t, elist[firstidx], q.Front())
	}
	elist = elist[total-maxsize:]
	assert.Equal(t, elist, q.List())
}

func TestClone(t *testing.T) {

	var fq *ErrorQueue
	kq := fq.Clone()
	assert.Nil(t, kq)
	fq = New(10)
	kq = fq.Clone()
	assert.Equal(t, New(10), kq)

	var maxsize = 50
	q := New(maxsize)
	assert.Equal(t, true, q.Empty())
	for i := 0; i < 100; i++ {
		var te []ErrorInfo
		ni := rand.Intn(10) + 1
		for j := 0; j < ni; j++ {
			ri := rand.Intn(1000)
			te = append(te, ErrorInfo{
				Error:     fmt.Sprintf("err: num%v", ri),
				Timestamp: int64(ri),
				Count:     int64(ri + 1),
			})
		}
		q.Append(te)
		q.Put(te[0])
	}
	nq := q.Clone()
	assert.Equal(t, nq.List(), q.List())
}
