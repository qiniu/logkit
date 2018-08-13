package queue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestDirectQueue(t *testing.T) {
	t.Parallel()
	dq := NewDirectQueue("TestDirectQueue")
	assert.Equal(t, "TestDirectQueue", dq.Name())
	ddq := dq.(DataQueue)
	ch := ddq.ReadDatasChan()
	var wg sync.WaitGroup
	puts := [][]Data{{Data{"a": 1}}}
	recv := [][]Data{}
	wg.Add(1)
	go func() {
		for exp := range ch {
			recv = append(recv, exp)
		}
		wg.Done()
	}()
	for _, v := range puts {
		err := ddq.PutDatas(v)
		if err != nil {
			t.Error(err)
		}
	}
	err := dq.Close()
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
	assert.Equal(t, puts, recv)
}

func TestDirectQueueEmpty(t *testing.T) {
	t.Parallel()
	dq := NewDirectQueue("TestDirectQueueEmpty")
	assert.Equal(t, "TestDirectQueueEmpty", dq.Name())
	ddq := dq.(DataQueue)
	ch := ddq.ReadDatasChan()
	var wg sync.WaitGroup
	puts := [][]Data{
		{Data{"a": 1}},
		{Data{"b": 2}},
		{Data{"c": 3}},
	}
	recv := [][]Data{}
	wg.Add(1)
	go func() {
		for exp := range ch {
			recv = append(recv, exp)
		}
		wg.Done()
	}()
	for i := 0; i < 3; i++ {
		err := ddq.PutDatas(puts[i])
		if err != nil {
			t.Error(err)
		}
	}
	if err := dq.Empty(); err != nil {
		t.Error(err)
	}
	for i := 3; i < len(puts); i++ {
		err := ddq.PutDatas(puts[i])
		if err != nil {
			t.Error(err)
		}
	}
	err := dq.Close()
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
	assert.Equal(t, puts, recv)
}
