package queue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/rateio"
)

// diskQueue implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64
	depthMemory  int64

	sync.RWMutex

	// instantiation time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // NOTE: currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	syncEveryWrite  int64         // number of writes per fsync
	syncEveryRead   int64         // number of reads per fsync
	syncTimeout     time.Duration // duration of time per fsync
	exitFlag        int32
	needSync        bool
	writeLimit      int // 限速 单位byte
	enableMemory    bool
	maxMemoryLength int64

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte

	// memory channel
	memoryChan chan []byte

	// internal channels
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
}

// newDiskQueue instantiates a new instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func NewDiskQueue(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEveryWrite, syncEveryRead int64, syncTimeout time.Duration, writeLimit int,
	enableMemory bool, maxMemoryLength int) BackendQueue {
	if !enableMemory {
		maxMemoryLength = 0
	} else if enableMemory && maxMemoryLength <= 0 {
		maxMemoryLength = 100
	}
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		enableMemory:      enableMemory,
		maxMemoryLength:   int64(maxMemoryLength),
		readChan:          make(chan []byte),
		memoryChan:        make(chan []byte, maxMemoryLength),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEveryWrite:    syncEveryWrite,
		syncEveryRead:     syncEveryRead,
		syncTimeout:       syncTimeout,
		writeLimit:        writeLimit,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		log.Warnf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()

	return &d
}

// Name returns the name of the queue
func (d *diskQueue) Name() string {
	return d.name
}

func (d *diskQueue) SyncMeta() {
	return
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	d.RLock()
	defer d.RUnlock()

	return atomic.LoadInt64(&d.depth) + atomic.LoadInt64(&d.depthMemory)
}

// ReadChan returns the []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		log.Warnf("DISKQUEUE(%s): deleting", d.name)
	} else {
		log.Warnf("DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	log.Warnf("DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		log.Warnf("ERROR: diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) deleteAllMemory() {
	for {
		select {
		case <-d.memoryChan:
		default:
			atomic.StoreInt64(&d.depthMemory, 0)
			return
		}
	}
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			log.Warnf("ERROR: diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		log.Warnf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	// msgSize 大小不合法，意味着可能磁盘数据损坏，应该立刻报错退出
	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	//
	// NOTE: 注意这里的逻辑，意味着使用者不能随意更改 maxBytesPerFile 这个值
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		log.Warnf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d)", dataLen)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	mr := io.MultiReader(&d.writeBuf, bytes.NewReader(data))
	writer := rateio.NewRateWriter(d.writeFile, d.writeLimit)
	_, err = io.Copy(writer, mr)
	if err != nil {
		log.Warn("io.Copy error -", err)
		writer.Close()
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	writer.Close()

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	// 注意这里是写完这一个消息之后才滚动
	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			log.Warnf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err)
		}

		// 关闭被滚动文件
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

func (d *diskQueue) writeMemory(msg []byte) error {
	if atomic.LoadInt64(&d.depthMemory) >= d.maxMemoryLength {
		return errors.New("memory channel is full")
	}
	select {
	case d.memoryChan <- msg:
		atomic.AddInt64(&d.depthMemory, 1)
		return nil
	default:
		return errors.New("memory channel is full")
	}
}

func (d *diskQueue) saveToDisk() {
	for {
		select {
		case msg := <-d.memoryChan:
			d.writeOne(msg)
		default:
			return
		}
	}
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return AtomicRename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// 处理各种指针错乱的场景
//
func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			log.Warnf(
				"ERROR: diskqueue(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			log.Warnf(
				"ERROR: diskqueue(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			log.Warnf(
				"ERROR: diskqueue(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			log.Warnf(
				"ERROR: diskqueue(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

// 真实改变读指针的位置（意味着上次读出的消息已确保被消费）
//
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// see if we need to clean up the old file
	// 尝试清除已经读过的文件
	//
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			log.Warnf("ERROR: failed to Remove(%s) - %s", fn, err)
		}
	}

	d.checkTailCorruption(depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	log.Warnf(
		"NOTICE: diskqueue(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := AtomicRename(badFn, badRenameFn)
	if err != nil {
		log.Warnf(
			"ERROR: diskqueue(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var origin int = FROM_NONE
	var dataRead []byte
	var err error
	var count int64
	var readCount int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		if count == d.syncEveryWrite {
			count = 0
			d.needSync = true
		}

		if readCount == d.syncEveryRead {
			readCount = 0
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				log.Warnf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err)
			}
		}

		if origin == FROM_NONE {
			if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
				if d.nextReadPos == d.readPos {
					dataRead, err = d.readOne()
					if err != nil {
						log.Warnf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
							d.name, d.readPos, d.fileName(d.readFileNum), err)
						// NOTE: 根据 handleReadError() 的逻辑，只要读发生错误，就会调过当前这个文件，直接开始读下一个文件
						d.handleReadError()
						continue
					}
				}
				origin = FROM_DISK
				r = d.readChan
			} else if d.enableMemory {
				select {
				case dataRead = <-d.memoryChan:
					origin = FROM_MEMORY
					r = d.readChan
				default:
					r = nil
				}
			} else {
				r = nil
			}
		} else {
			r = d.readChan
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			switch origin {
			case FROM_DISK:
				// moveForward sets needSync flag if a file is removed
				readCount++
				d.moveForward()
			case FROM_MEMORY:
				atomic.AddInt64(&d.depthMemory, -1)
			}
			origin = FROM_NONE
		case <-d.emptyChan:
			if d.enableMemory {
				d.deleteAllMemory()
			}
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
			origin = FROM_NONE
		case dataWrite := <-d.writeChan:
			if d.enableMemory {
				d.writeResponseChan <- d.writeMemory(dataWrite)
			} else {
				count++
				d.writeResponseChan <- d.writeOne(dataWrite)
			}
		case <-syncTicker.C:
			if count > 0 || readCount > 0 {
				count = 0
				readCount = 0
				d.needSync = true
			}
		case <-d.exitChan:
			if origin == FROM_MEMORY {
				err = d.writeOne(dataRead)
				if err != nil {
					log.Errorf("DISKQUEUE(%s): drop one msg - %v", d.name, err)
				}
			}
			goto exit
		}
	}

exit:
	log.Warnf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	if d.enableMemory {
		d.saveToDisk()
	}
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
