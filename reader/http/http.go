package http

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net"
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
	address string
	path    string

	meta   *reader.Meta
	status int32

	listener net.Listener
	bufQueue queue.BackendQueue
	readChan <-chan []byte
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
	readChan := bq.ReadChan()
	return &Reader{
		address:  address,
		path:     path,
		meta:     meta,
		bufQueue: bq,
		readChan: readChan,
		status:   reader.StatusInit,
	}, nil
}

func (h *Reader) Name() string {
	return "Reader<" + h.address + ">"
}

func (h *Reader) Source() string {
	return h.address
}

func (h *Reader) Start() error {
	if !atomic.CompareAndSwapInt32(&h.status, reader.StatusInit, reader.StatusRunning) {
		return fmt.Errorf("runner[%v] Reader[%v] already started", h.meta.RunnerName, h.Name())
	}
	var err error
	r := echo.New()
	r.POST(h.path, h.postData())

	if h.listener, err = net.Listen("tcp", h.address); err != nil {
		return err
	}

	server := &http.Server{
		Handler: r,
		Addr:    h.address,
	}
	go func() {
		server.Serve(h.listener)
	}()
	log.Infof("runner[%v] Reader[%v] has started and listener service on %v\n", h.meta.RunnerName, h.Name(), h.address)
	return nil
}

func (h *Reader) ReadLine() (data string, err error) {
	if atomic.LoadInt32(&h.status) == reader.StatusInit {
		err = h.Start()
		if err != nil {
			log.Error(err)
		}
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-h.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (h *Reader) SetMode(mode string, v interface{}) error {
	return fmt.Errorf("runner[%v] Reader[%v] not support read mode\n", h.meta.RunnerName, h.Name())
}

func (h *Reader) Close() error {
	if atomic.CompareAndSwapInt32(&h.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] Reader[%v] stopping", h.meta.RunnerName, h.Name())
	} else {
		h.bufQueue.Close()
	}
	if h.listener != nil {
		h.listener.Close()
	}
	return nil
}

func (h *Reader) SyncMeta() {}

func (h *Reader) postData() echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := h.pickUpData(c.Request()); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, nil)
	}
}

func (h *Reader) pickUpData(req *http.Request) (err error) {
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
	r := bufio.NewReader(reqBody)
	return h.storageData(r)
}

func (h *Reader) storageData(r *bufio.Reader) (err error) {
	for {
		line, err := h.readLine(r)
		if err != nil {
			if err != io.EOF {
				log.Errorf("runner[%v] Reader[%v] read data from http request error, %v\n", h.meta.RunnerName, h.Name(), err)
			}
			break
		}
		if line == "" {
			continue
		}
		h.bufQueue.Put([]byte(line))
	}
	return
}

func (h *Reader) readLine(r *bufio.Reader) (str string, err error) {
	isPrefix := true
	var line, fragment []byte
	for isPrefix && err == nil {
		fragment, isPrefix, err = r.ReadLine()
		line = append(line, fragment...)
	}
	return string(line), err
}
