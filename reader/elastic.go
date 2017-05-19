package reader

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"

	"gopkg.in/olivere/elastic.v3"
)

type ElasticReader struct {
	esindex   string //es索引
	estype    string //es type
	eshost    string //eshost+port
	readBatch int    // 每次读取的数据量
	keepAlive string //scrollID 保留时间

	readChan chan json.RawMessage

	meta   *Meta  // 记录offset的元数据
	offset string // 当前处理es的offset

	status  int32
	mux     sync.Mutex
	started bool
}

func NewESReader(meta *Meta, readBatch int, estype, esindex, eshost, keepAlive string) (er *ElasticReader, err error) {

	offset, _, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("%v -meta data is corrupted err:%v, omit meta data", meta.MetaFile(), err)
	}
	er = &ElasticReader{
		esindex:   esindex,
		estype:    estype,
		eshost:    eshost,
		readBatch: readBatch,
		keepAlive: keepAlive,
		meta:      meta,
		status:    StatusInit,
		offset:    offset,
		readChan:  make(chan json.RawMessage),
		mux:       sync.Mutex{},
		started:   false,
	}

	return er, nil
}

func (er *ElasticReader) Name() string {
	return "ESReader:" + er.Source()
}

func (er *ElasticReader) Source() string {
	return er.eshost + "_" + er.esindex + "_" + er.estype
}

func (er *ElasticReader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&er.status, StatusRunning, StatusStoping) {
		log.Infof("%v stopping", er.Name())
	} else {
		close(er.readChan)
	}
	return
}

//Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
func (er *ElasticReader) Start() {
	er.mux.Lock()
	defer er.mux.Unlock()
	if er.started {
		return
	}
	go er.run()
	er.started = true
	log.Printf("%v pull data deamon started", er.Name())
}

func (er *ElasticReader) ReadLine() (data string, err error) {
	if !er.started {
		er.Start()
	}
	timer := time.NewTicker(time.Millisecond)
	select {
	case dat := <-er.readChan:
		data = string(dat)
	case <-timer.C:
	}
	return
}

func (er *ElasticReader) run() {
	// 防止并发run
	for {
		if atomic.LoadInt32(&er.status) == StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&er.status, StatusInit, StatusRunning) {
			break
		}
	}
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&er.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&er.status, StatusStoping, StatusStopped) {
			close(er.readChan)
		}
		log.Infof("%v successfully finnished", er.Name())
	}()

	// 开始work逻辑
	for {
		if atomic.LoadInt32(&er.status) == StatusStoping {
			log.Warnf("%v stopped from running", er.Name())
			return
		}
		err := er.exec()
		if err == nil {
			log.Infof("%v successfully exec", er.Name())
			return
		}
		log.Error(err)
		time.Sleep(3 * time.Second)
	}
}

func (er *ElasticReader) exec() (err error) {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(er.eshost))
	if err != nil {
		return
	}
	scroll := client.Scroll(er.esindex).Type(er.estype).Size(er.readBatch).KeepAlive(er.keepAlive)
	for {
		results, err := scroll.ScrollId(er.offset).Do()
		if err == io.EOF {
			return nil // all results retrieved
		}
		if err != nil {
			return err // something went wrong
		}

		// Send the hits to the hits channel
		for _, hit := range results.Hits.Hits {
			er.readChan <- *hit.Source
		}
		er.offset = results.ScrollId
		if atomic.LoadInt32(&er.status) == StatusStoping {
			log.Warnf("%v stopped from running", er.Name())
			return nil
		}
	}
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (er *ElasticReader) SyncMeta() {
	if err := er.meta.WriteOffset(er.offset, 0); err != nil {
		log.Errorf("%v SyncMeta error %v", er.Name(), err)
	}
	return
}

func (er *ElasticReader) SetMode(mode string, v interface{}) error {
	return errors.New("ElasticReader not support readmode")
}
