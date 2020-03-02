package audit

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils"
)

const defaultRotateSize = 100 * 1024 * 1024

type Message struct {
	Runnername string
	Timestamp  int64
	ReadBytes  int64
	ReadLines  int64
	SendLines  int64
	RunnerNote string
	Lag        int64
}

type Audit struct {
	Dir        string
	RotateSize int64

	mu      sync.Mutex
	buf     bytes.Buffer // for accumulating text to write
	f       *os.File
	backoff *utils.Backoff
}

func NewAuditLogger(dir string, rotatesize int64) (*Audit, error) {
	//小于1kb，设为默认值
	if rotatesize <= 1024 {
		rotatesize = defaultRotateSize
	}
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	} else if err != nil {
		return nil, fmt.Errorf("stat dir error %v", err)
	} else {
		if !info.IsDir() {
			return nil, fmt.Errorf("%s is not a dir", dir)
		}
	}
	file, err := openFile(dir)
	if err != nil {
		return nil, err
	}
	return &Audit{
		Dir:        dir,
		RotateSize: rotatesize,
		f:          file,
		backoff:    utils.NewBackoff(2, 1, 1*time.Second, 5*time.Minute),
	}, nil
}

func openFile(dir string) (*os.File, error) {
	name := time.Now().Format("20060102150405.000")
	newfile := filepath.Join(dir, name)
	file, err := os.OpenFile(newfile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		err = fmt.Errorf("rotateLog open newfile %v err %v", newfile, err)
		return nil, err
	}
	return file, nil
}

//返回true就rotate
func (a *Audit) checksize() bool {
	if a.f == nil {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	st, err := a.f.Stat()
	if err != nil {
		log.Errorf("state file err %v", err)
		return true
	}
	if st.Size() < a.RotateSize {
		return false
	}
	return true
}

func (a *Audit) rotate() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f != nil {
		a.f.Close()
		a.f = nil
	}
	file, err := openFile(a.Dir)
	if err != nil {
		return err
	}
	a.f = file
	return nil
}

func (a *Audit) Log(msg Message) {
	if a.checksize() {
		if err := a.rotate(); err != nil {
			log.Errorf("rotate log err %v", err)
			time.Sleep(a.backoff.Duration())
			return
		}
		a.backoff.Reset()
	}
	if err := a.output(&msg); err != nil {
		log.Errorf("audit output msg err %v", err)
		time.Sleep(a.backoff.Duration())
	} else {
		a.backoff.Reset()
	}
}

func itoa(buf *bytes.Buffer, i int64) {
	if i == 0 {
		buf.WriteByte('0')
		return
	}
	var u uint64 = uint64(i)

	// Assemble decimal in reverse order.
	var b [64]byte
	bp := len(b)
	for ; u > 0; u /= 10 {
		bp--
		b[bp] = byte(u%10) + '0'
		if u == 0 {
			break
		}
	}

	// avoid slicing b to avoid an allocation.
	for bp < len(b) {
		buf.WriteByte(b[bp])
		bp++
	}
}

func (a *Audit) output(msg *Message) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.buf.Reset()
	a.buf.WriteString(msg.Runnername)
	a.buf.WriteByte('\t')
	itoa(&a.buf, msg.Timestamp)
	a.buf.WriteByte('\t')
	itoa(&a.buf, msg.ReadBytes)
	a.buf.WriteByte('\t')
	itoa(&a.buf, msg.ReadLines)
	a.buf.WriteByte('\t')
	itoa(&a.buf, msg.SendLines)
	a.buf.WriteByte('\t')
	a.buf.WriteString(msg.RunnerNote)
	a.buf.WriteByte('\t')
	itoa(&a.buf, msg.Lag)
	a.buf.WriteByte('\n')

	_, err := a.f.Write(a.buf.Bytes())
	return err
}
