package queue

import (
	"testing"

	"sync"

	"github.com/stretchr/testify/assert"
)

func TestMemoryQueue(t *testing.T) {
	mq := NewMemoryQueue("TestMemoryQueue", 10)
	assert.Equal(t, "TestMemoryQueue", mq.Name())
	ch := mq.ReadChan()
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
		err := mq.Put([]byte(v))
		if err != nil {
			t.Error(err)
		}
	}
	err := mq.Close()
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
	assert.Equal(t, puts, recv)
}

func TestMemoryQueueEmpty(t *testing.T) {
	mq := NewMemoryQueue("TestMemoryQueueEmpty", 10)
	assert.Equal(t, "TestMemoryQueueEmpty", mq.Name())
	ch := mq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	for i := 0; i < 3; i++ {
		err := mq.Put([]byte(puts[i]))
		if err != nil {
			t.Error(err)
		}
	}
	if err := mq.Empty(); err != nil {
		t.Error(err)
	}
	for i := 3; i < len(puts); i++ {
		err := mq.Put([]byte(puts[i]))
		if err != nil {
			t.Error(err)
		}
	}
	recv := []string{}
	wg.Add(1)
	go func() {
		for exp := range ch {
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	err := mq.Close()
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
	assert.Equal(t, puts[3:], recv)
}

func TestMemoryQueueFull(t *testing.T) {
	mq := NewMemoryQueue("TestMemoryQueueFull", 5)
	assert.Equal(t, "TestMemoryQueueFull", mq.Name())
	ch := mq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	for i := 0; i < len(puts); {
		err := mq.Put([]byte(puts[i]))
		if err != nil {
			if err := mq.Empty(); err != nil {
				t.Error(err)
			}
		} else {
			i++
		}
	}
	recv := []string{}
	wg.Add(1)
	go func() {
		for exp := range ch {
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	err := mq.Close()
	if err != nil {
		t.Error(err)
	}
	wg.Wait()
	assert.Equal(t, puts[5:], recv)

}
