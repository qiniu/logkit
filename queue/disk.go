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
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/rateio"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var _ BackendQueue = &diskQueue{}

// diskQueue implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// 运行状态相关变量（部分会通过元数据文件存取）
	readFileNum          int64
	readPos              int64
	writeFileNum         int64
	writePos             int64
	depthMemory          int64
	depth                int64
	currentDiskUsedBytes int64

	sync.RWMutex

	// 实例化时获取的配置
	name                string
	dataPath            string // 存放文件的目录
	maxBytesPerFile     int64  // NOTE: currently this cannot change once created
	minMsgSize          int32
	maxMsgSize          int32
	syncEveryWrite      int64         // number of writes per fsync
	syncEveryRead       int64         // number of reads per fsync
	syncTimeout         time.Duration // duration of time per fsync
	exitFlag            int32
	needSync            bool
	writeRateLimit      int // 限速 单位byte
	enableMemoryQueue   bool
	memoryQueueSize     int64
	enableDiskUsedLimit bool
	maxDiskUsedBytes    int64

	// 尚未通过 readChan 更新的实际最新读取位置
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
	emptyChan         chan struct{}
	emptyResponseChan chan error
	exitChan          chan struct{}
	stopped           int32
	exitSyncChan      chan struct{}
}

type NewDiskQueueOptions struct {
	Name              string
	DataPath          string
	MaxBytesPerFile   int64
	MinMsgSize        int32
	MaxMsgSize        int32
	SyncEveryWrite    int64
	SyncEveryRead     int64
	SyncTimeout       time.Duration
	WriteRateLimit    int
	EnableMemoryQueue bool
	MemoryQueueSize   int64

	// DisableDiskUsedLimit 指示是否禁用磁盘占用限制，超出限制后数据将不会再写入到文件而被直接丢弃
	DisableDiskUsedLimit bool
	MaxDiskUsedBytes     int64
}

// newDiskQueue instantiates a new instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func NewDiskQueue(opts NewDiskQueueOptions) BackendQueue {
	if !opts.EnableMemoryQueue {
		opts.MemoryQueueSize = 0
	} else if opts.EnableMemoryQueue && opts.MemoryQueueSize <= 0 {
		opts.MemoryQueueSize = 100
	}

	if !opts.DisableDiskUsedLimit && opts.MaxDiskUsedBytes <= 0 {
		opts.MaxDiskUsedBytes = 32 * GB
	}

	d := diskQueue{
		name:                opts.Name,
		dataPath:            opts.DataPath,
		maxBytesPerFile:     opts.MaxBytesPerFile,
		minMsgSize:          opts.MinMsgSize,
		maxMsgSize:          opts.MaxMsgSize,
		enableMemoryQueue:   opts.EnableMemoryQueue,
		memoryQueueSize:     opts.MemoryQueueSize,
		enableDiskUsedLimit: !opts.DisableDiskUsedLimit,
		maxDiskUsedBytes:    opts.MaxDiskUsedBytes,
		readChan:            make(chan []byte),
		memoryChan:          make(chan []byte, opts.MemoryQueueSize),
		writeChan:           make(chan []byte),
		writeResponseChan:   make(chan error),
		emptyChan:           make(chan struct{}),
		emptyResponseChan:   make(chan error),
		exitChan:            make(chan struct{}),
		exitSyncChan:        make(chan struct{}),
		syncEveryWrite:      opts.SyncEveryWrite,
		syncEveryRead:       opts.SyncEveryRead,
		syncTimeout:         opts.SyncTimeout,
		writeRateLimit:      opts.WriteRateLimit,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil {
		log.Warnf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err.Error())
	}

	go d.ioLoop()

	return &d
}

// Name returns the name of the queue
func (d *diskQueue) Name() string {
	return d.name
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

	if d.exitFlag == 1 {
		return nil
	}
	d.exitFlag = 1

	if deleted {
		log.Warnf("DISKQUEUE(%s): deleting", d.name)
	} else {
		log.Debugf("DISKQUEUE(%s): closing", d.name)
	}

	atomic.AddInt32(&d.stopped, 1)

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

	d.emptyChan <- struct{}{}
	return <-d.emptyResponseChan
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		log.Warnf("ERROR: diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr.Error())
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
			log.Warnf("ERROR: diskqueue(%s) failed to remove data file - %s", d.name, innerErr.Error())
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

	if d.enableDiskUsedLimit {
		atomic.StoreInt64(&d.currentDiskUsedBytes, 0)
	}

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
	// NOTE: 注意这里的逻辑，意味着使用者不能随意更改 maxBytesPerFile 这个值，可以考虑在切文件的时候拿到当前读文件的 size
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
	dataLen := int32(len(data))
	totalBytes := int64(4 + dataLen)

	if d.enableDiskUsedLimit && atomic.LoadInt64(&d.currentDiskUsedBytes)+totalBytes > d.maxDiskUsedBytes {
		return fmt.Errorf("current disk used bytes has exceeded max disk used bytes: %d", d.maxDiskUsedBytes)
	}

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

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d)", dataLen)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	mr := io.MultiReader(&d.writeBuf, bytes.NewReader(data))
	writer := rateio.NewRateWriter(d.writeFile, d.writeRateLimit)
	_, err = io.Copy(writer, mr)
	if err != nil {
		log.Warnf("io.Copy error - %s", err.Error())
		writer.Close()
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	writer.Close()

	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	if d.enableDiskUsedLimit {
		atomic.AddInt64(&d.currentDiskUsedBytes, totalBytes)
	}

	// 注意这里是写完这一个消息之后才滚动
	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			log.Warnf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
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
	if atomic.LoadInt64(&d.depthMemory) >= d.memoryQueueSize {
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
			err := d.writeOne(msg)
			if err != nil {
				// FIXME: 需要一个合适的方案防止数据丢失
				log.Errorf("DISKQUEUE(%s): drop one msg from memory chan - %s", d.name, err.Error())
			}
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

// retrieveMetaData 从 meta 文件中获取上次保存的状态数据，用于初始化 diskQueue
func (d *diskQueue) retrieveMetaData() error {
	metaPath := d.metaDataFileName()
	if !utils.IsExist(metaPath) {
		return nil
	}

	f, err := os.OpenFile(metaPath, os.O_RDONLY, 0600)
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

	// 如开启磁盘用量限制，则需计算出当前已占用磁盘字节数
	if !d.enableDiskUsedLimit {
		return nil
	}
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fpath := d.fileName(i)
		if !utils.IsExist(fpath) {
			continue
		}

		fi, err := os.Stat(fpath)
		if err != nil {
			return err
		}
		atomic.AddInt64(&d.currentDiskUsedBytes, fi.Size())
	}
	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
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
	return fmt.Sprintf(filepath.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(filepath.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
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

		fpath := d.fileName(oldReadFileNum)
		if utils.IsExist(fpath) {
			fi, err := os.Stat(fpath)
			if err != nil {
				log.Warnf("ERROR: failed to stat file %q: %s", fpath, err.Error())
			} else {
				atomic.AddInt64(&d.currentDiskUsedBytes, -fi.Size())
			}

			err = os.Remove(fpath)
			if err != nil {
				log.Warnf("ERROR: failed to remove file %q: %s", fpath, err.Error())
			}
		}
	}

	d.checkTailCorruption(depth)
}

func (d *diskQueue) handleReadError(failRead int) {
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

	err := AtomicRename(badFn, badRenameFn)
	if err != nil {
		log.Warnf(
			"ERROR: diskqueue(%s) failed to rename bad diskqueue file %s to %s, error: %s",
			d.name, badFn, badRenameFn, err.Error())
	} else {
		log.Warnf(
			"NOTICE: diskqueue(%s) jump to next file and saving bad file as %s",
			d.name, badRenameFn)
	}

	if failRead >= 10 {
		log.Errorf("ERROR: diskqueue(%s) continue fail read, set readFileNum with writeFileNum: %d", d.name, d.writeFileNum)
		d.readFileNum = d.writeFileNum
	} else {
		d.readFileNum++
	}
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
	var origin int = FromNone
	var dataRead []byte
	var err error
	var count int64
	var readCount int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)
	failRead := 1

DONE:
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
				log.Warnf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
			}
		}

		if origin == FromNone {
			// 限制为10，否则kill之后接收不到 exitChan 卡住
			if (d.readFileNum < d.writeFileNum) && (d.readPos < d.writePos) && failRead <= 10 {
				if d.nextReadPos == d.readPos {
					dataRead, err = d.readOne()
					if err != nil && atomic.LoadInt32(&d.stopped) == 0 {
						log.Errorf("ERROR: reading from diskqueue(%s) at %d of %s failRead %d - %s",
							d.name, d.readPos, d.fileName(d.readFileNum), failRead, err.Error())
						// NOTE: 根据 handleReadError() 的逻辑，只要读发生错误，就会调过当前这个文件，直接开始读下一个文件
						d.handleReadError(failRead)
						time.Sleep(time.Duration(failRead) * time.Second)
						failRead++
						continue
					}
				}
				origin = FromDisk
				r = d.readChan
			} else if d.enableMemoryQueue {
				select {
				case dataRead = <-d.memoryChan:
					origin = FromMemory
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
		failRead = 1

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			switch origin {
			case FromDisk:
				// moveForward sets needSync flag if a file is removed
				readCount++
				d.moveForward()
			case FromMemory:
				atomic.AddInt64(&d.depthMemory, -1)
			}
			origin = FromNone
		case <-d.emptyChan:
			if d.enableMemoryQueue {
				d.deleteAllMemory()
			}
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
			origin = FromNone
		case dataWrite := <-d.writeChan:
			if d.enableMemoryQueue {
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
			if origin == FromMemory {
				err = d.writeOne(dataRead)
				if err != nil {
					// FIXME: 需要一个合适的方案防止数据丢失
					log.Errorf("DISKQUEUE(%s): drop one msg from memory chan - %s", d.name, err.Error())
				}
			}
			break DONE
		}
	}

	syncTicker.Stop()
	log.Warnf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	if d.enableMemoryQueue {
		d.saveToDisk()
	}
	d.exitSyncChan <- struct{}{}
}
