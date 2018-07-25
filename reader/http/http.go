package http

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/labstack/echo"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/queue"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.Reader       = &Reader{}
)

const (
	DefaultSyncEvery       = 10
	DefaultMaxBodySize     = 100 * 1024 * 1024
	DefaultMaxBytesPerFile = 500 * 1024 * 1024
	DefaultWriteSpeedLimit = 10 * 1024 * 1024 // 默认写速限制为10MB
)

func init() {
	reader.RegisterConstructor(reader.ModeHTTP, NewReader)
}

type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32

	bufQueue queue.BackendQueue
	readChan <-chan []byte

	address string
	path    string

	server *http.Server
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	address, _ := conf.GetStringOr(reader.KeyHTTPServiceAddress, reader.DefaultHTTPServiceAddress)
	path, _ := conf.GetStringOr(reader.KeyHTTPServicePath, reader.DefaultHTTPServicePath)
	address, _ = RemoveHttpProtocal(address)

	bq := queue.NewDiskQueue(Hash("Reader<"+address+">_buffer"), meta.BufFile(), DefaultMaxBytesPerFile, 0,
		DefaultMaxBytesPerFile, DefaultSyncEvery, DefaultSyncEvery, time.Second*2, DefaultWriteSpeedLimit, false, 0)
	err := CreateDirIfNotExist(meta.BufFile())
	if err != nil {
		return nil, err
	}
	return &Reader{
		meta:     meta,
		status:   reader.StatusInit,
		bufQueue: bq,
		readChan: bq.ReadChan(),
		address:  address,
		path:     path,
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopped
}

func (r *Reader) Name() string {
	return "HTTPReader<" + r.address + ">"
}

func (r *Reader) SetMode(_ string, _ interface{}) error {
	return fmt.Errorf("reader %q does not support read mode", r.Name())
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	e := echo.New()
	e.POST(r.path, r.postData())

	r.server = &http.Server{
		Handler: e,
		Addr:    r.address,
	}
	go func() {
		if err := r.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("Runner[%v] %q daemon start HTTP server failed: %v", r.meta.RunnerName, r.Name(), err)
		}
	}()
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return r.address
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case data := <-r.readChan:
		return string(data), nil
	case <-timer.C:
	}

	return "", nil
}

func (_ *Reader) SyncMeta() {}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())

	r.server.Shutdown(context.Background())
	err := r.bufQueue.Close()
	atomic.StoreInt32(&r.status, reader.StatusStopped)
	return err
}

func (r *Reader) postData() echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := r.pickUpData(c.Request()); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, nil)
	}
}

func (r *Reader) pickUpData(req *http.Request) (err error) {
	if req.ContentLength > DefaultMaxBodySize {
		return errors.New("the request body is too large")
	}
	reqBody := req.Body
	defer reqBody.Close()
	contentEncoding := req.Header.Get(ContentEncodingHeader)
	contentType := req.Header.Get(ContentTypeHeader)
	if contentEncoding == "gzip" || contentType == "application/gzip" {
		reqBody, err = gzip.NewReader(req.Body)
		if err != nil {
			return fmt.Errorf("read gzip body error %v", err)
		}
	}
	br := bufio.NewReader(reqBody)
	return r.storageData(br)
}

func (r *Reader) storageData(br *bufio.Reader) (err error) {
	for {
		line, err := r.readLine(br)
		if err != nil {
			if err != io.EOF {
				log.Errorf("runner[%v] Reader[%v] read data from http request error, %v\n", r.meta.RunnerName, r.Name(), err)
			}
			break
		}
		if line == "" {
			continue
		}
		r.bufQueue.Put([]byte(line))
	}
	return
}

func (r *Reader) readLine(br *bufio.Reader) (str string, err error) {
	isPrefix := true
	var line, fragment []byte
	for isPrefix && err == nil {
		fragment, isPrefix, err = br.ReadLine()
		line = append(line, fragment...)
	}
	return string(line), err
}
