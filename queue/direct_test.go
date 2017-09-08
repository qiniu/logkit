package queue

import (
	"testing"

	"sync"

	"github.com/stretchr/testify/assert"
)

func TestDirectQueue(t *testing.T) {
	dq := NewDirectQueue("TestDirectQueue")
	assert.Equal(t, "TestDirectQueue", dq.Name())
	ch := dq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}
	wg.Add(1)
	go func() {
		for exp := range ch {
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	for _, v := range puts {
		err := dq.Put([]byte(v))
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
	dq := NewDirectQueue("TestDirectQueueEmpty")
	assert.Equal(t, "TestDirectQueueEmpty", dq.Name())
	ch := dq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}
	wg.Add(1)
	go func() {
		for exp := range ch {
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	for i := 0; i < 3; i++ {
		err := dq.Put([]byte(puts[i]))
		if err != nil {
			t.Error(err)
		}
	}
	if err := dq.Empty(); err != nil {
		t.Error(err)
	}
	for i := 3; i < len(puts); i++ {
		err := dq.Put([]byte(puts[i]))
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
