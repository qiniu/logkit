package queue

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDiskQueue(t *testing.T) {
	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := NewDiskQueue(dqName, tmpDir, 1024, 4, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("test")
	err = dq.Put(msg)
	assert.Equal(t, err, nil)
	assert.Equal(t, dq.Depth(), int64(1))

	msgOut := <-dq.ReadChan()
	assert.Equal(t, msgOut, msg)
	dq.Close()
}

func TestDiskQueueWithMemory(t *testing.T) {
	dqName := "test_disk_queue_with_memory" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}
	dq1 := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, true, 7)
	assert.NotEqual(t, dq1, nil)
	assert.Equal(t, dq1.Depth(), int64(0))
	for _, v := range puts {
		err := dq1.Put([]byte(v))
		assert.NoError(t, err)
	}
	dq1.Close()
	dq2 := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, true, 10)
	assert.NotEqual(t, dq2, nil)
	assert.Equal(t, dq2.Depth(), int64(7))
	ch := dq2.ReadChan()
	for range puts {
		exp := <-ch
		recv = append(recv, string(exp))
	}
	assert.Equal(t, puts, recv)
	dq2.Close()
}

func TestDiskQueueMemoryLength(t *testing.T) {
	dqName := "test_disk_queue_memory_length" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq1 := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, -1).(*diskQueue)
	assert.NotEqual(t, dq1, nil)
	assert.Equal(t, 0, cap(dq1.memoryChan))
	dq1.Close()
	dq2 := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, true, 0).(*diskQueue)
	assert.NotEqual(t, dq2, nil)
	assert.Equal(t, 100, cap(dq2.memoryChan))
	dq2.Close()
	dq3 := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, true, 1).(*diskQueue)
	assert.NotEqual(t, dq3, nil)
	assert.Equal(t, 1, cap(dq3.memoryChan))
	dq3.Close()
}

func TestDiskQueueRoll(t *testing.T) {
	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	ml := int64(len(msg))
	dq := NewDiskQueue(dqName, tmpDir, 9*(ml+4), int32(ml), 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	for i := 0; i < 10; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	assert.Equal(t, dq.(*diskQueue).writeFileNum, int64(1))
	assert.Equal(t, dq.(*diskQueue).writePos, int64(0))
	dq.Close()
}

func assertFileNotExist(t *testing.T, fn string) {
	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	assert.Equal(t, f, (*os.File)(nil))
	assert.Equal(t, os.IsNotExist(err), true)
}

func TestDiskQueueEmpty(t *testing.T) {
	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	dq := NewDiskQueue(dqName, tmpDir, 100, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	for i := 0; i < 3; i++ {
		<-dq.ReadChan()
	}

	for {
		if dq.Depth() == 97 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.Equal(t, dq.Depth(), int64(97))

	numFiles := dq.(*diskQueue).writeFileNum
	dq.Empty()

	assertFileNotExist(t, dq.(*diskQueue).metaDataFileName())
	for i := int64(0); i <= numFiles; i++ {
		assertFileNotExist(t, dq.(*diskQueue).fileName(i))
	}
	assert.Equal(t, dq.Depth(), int64(0))
	assert.Equal(t, dq.(*diskQueue).readFileNum, dq.(*diskQueue).writeFileNum)
	assert.Equal(t, dq.(*diskQueue).readPos, dq.(*diskQueue).writePos)
	assert.Equal(t, dq.(*diskQueue).nextReadPos, dq.(*diskQueue).readPos)
	assert.Equal(t, dq.(*diskQueue).nextReadFileNum, dq.(*diskQueue).readFileNum)

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		assert.Equal(t, err, nil)
		assert.Equal(t, dq.Depth(), int64(i+1))
	}

	for i := 0; i < 100; i++ {
		<-dq.ReadChan()
	}

	for {
		if dq.Depth() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	assert.Equal(t, dq.Depth(), int64(0))
	assert.Equal(t, dq.(*diskQueue).readFileNum, dq.(*diskQueue).writeFileNum)
	assert.Equal(t, dq.(*diskQueue).readPos, dq.(*diskQueue).writePos)
	assert.Equal(t, dq.(*diskQueue).nextReadPos, dq.(*diskQueue).readPos)
	dq.Close()
}

func TestDiskQueueEmptyWithMemory(t *testing.T) {
	dqName := "test_disk_queue_empty_with_memory" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, true, 7)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}
	for i := 0; i < 3; i++ {
		err := dq.Put([]byte(puts[i]))
		assert.NoError(t, err)
	}
	err = dq.Empty()
	assert.NoError(t, err)
	for i := 3; i < len(puts); i++ {
		err := dq.Put([]byte(puts[i]))
		if err != nil {
			t.Error(err)
		}
	}
	ch := dq.ReadChan()
	for range puts[3:] {
		exp := <-ch
		recv = append(recv, string(exp))
	}
	assert.Equal(t, puts[3:], recv)
	dq.Close()
}

func TestDiskQueueFullWithMemory(t *testing.T) {
	dqName := "test_disk_queue_full_with_memory" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := NewDiskQueue(dqName, tmpDir, 1024, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, true, 5)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}
	for i := 0; i < len(puts); {
		err := dq.Put([]byte(puts[i]))
		if err != nil {
			err := dq.Empty()
			assert.NoError(t, err)
		} else {
			i++
		}
	}
	ch := dq.ReadChan()
	for range puts[5:] {
		exp := <-ch
		recv = append(recv, string(exp))
	}
	assert.Equal(t, puts[5:], recv)
	dq.Close()
}
func TestDiskQueueCorruption(t *testing.T) {
	dqName := "test_disk_queue_corruption" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	// require a non-zero message length for the corrupt (len 0) test below
	dq := NewDiskQueue(dqName, tmpDir, 1000, 10, 1<<10, 5, 5, 2*time.Second, 10*1024*1024, false, 0)

	msg := make([]byte, 123) // 127 bytes per message, 8 (1016 bytes) messages per file
	for i := 0; i < 25; i++ {
		dq.Put(msg)
	}

	assert.Equal(t, dq.Depth(), int64(25))

	// corrupt the 2nd file
	dqFn := dq.(*diskQueue).fileName(1)
	os.Truncate(dqFn, 500) // 3 valid messages, 5 corrupted

	for i := 0; i < 19; i++ { // 1 message leftover in 4th file
		assert.Equal(t, <-dq.ReadChan(), msg)
	}

	// corrupt the 4th (current) file
	dqFn = dq.(*diskQueue).fileName(3)
	os.Truncate(dqFn, 100)

	dq.Put(msg) // in 5th file

	assert.Equal(t, <-dq.ReadChan(), msg)

	// write a corrupt (len 0) message at the 5th (current) file
	dq.(*diskQueue).writeFile.Write([]byte{0, 0, 0, 0})

	// force a new 6th file - put into 5th, then readOne errors, then put into 6th
	dq.Put(msg)
	dq.Put(msg)

	assert.Equal(t, <-dq.ReadChan(), msg)
	dq.Close()
}

func TestDiskQueueTorture(t *testing.T) {
	var wg sync.WaitGroup

	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := NewDiskQueue(dqName, tmpDir, 262144, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(0))

	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")

	numWriters := 4
	numReaders := 4
	readExitChan := make(chan int)
	writeExitChan := make(chan int)

	var depth int64
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case <-writeExitChan:
					return
				default:
					err := dq.Put(msg)
					if err == nil {
						atomic.AddInt64(&depth, 1)
					}
				}
			}
		}()
	}

	time.Sleep(1 * time.Second)

	dq.Close()

	t.Logf("closing writeExitChan")
	close(writeExitChan)
	wg.Wait()

	t.Logf("restarting diskqueue")

	dq = NewDiskQueue(dqName, tmpDir, 262144, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), depth)

	var read int64
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case m := <-dq.ReadChan():
					assert.Equal(t, msg, m)
					atomic.AddInt64(&read, 1)
				case <-readExitChan:
					return
				}
			}
		}()
	}

	t.Logf("waiting for depth 0")
	for {
		if dq.Depth() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("closing readExitChan")
	close(readExitChan)
	wg.Wait()

	assert.Equal(t, read, depth)

	dq.Close()
}

func BenchmarkDiskQueuePut(b *testing.B) {
	b.StopTimer()
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := NewDiskQueue(dqName, tmpDir, 1024768*100, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	size := 1024
	b.SetBytes(int64(size))
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dq.Put(data)
	}
}

func BenchmarkDiskWrite(b *testing.B) {
	b.StopTimer()
	fileName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	f, _ := os.OpenFile(path.Join(tmpDir, fileName), os.O_RDWR|os.O_CREATE, 0600)
	size := 256
	b.SetBytes(int64(size))
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		f.Write(data)
	}
	f.Sync()
}

func BenchmarkDiskWriteBuffered(b *testing.B) {
	b.StopTimer()
	fileName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	f, _ := os.OpenFile(path.Join(tmpDir, fileName), os.O_RDWR|os.O_CREATE, 0600)
	size := 256
	b.SetBytes(int64(size))
	data := make([]byte, size)
	w := bufio.NewWriterSize(f, 1024*4)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		w.Write(data)
		if i%1024 == 0 {
			w.Flush()
		}
	}
	w.Flush()
	f.Sync()
}

// this benchmark should be run via:
//    $ go test -test.bench 'DiskQueueGet' -test.benchtime 0.1
// (so that it does not perform too many iterations)
func BenchmarkDiskQueueGet(b *testing.B) {
	b.StopTimer()
	dqName := "bench_disk_queue_get" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := NewDiskQueue(dqName, tmpDir, 1024768, 0, 1<<10, 2500, 2500, 2*time.Second, 10*1024*1024, false, 0)
	for i := 0; i < b.N; i++ {
		dq.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		<-dq.ReadChan()
	}
}
