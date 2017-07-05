package reader

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/qiniu/log"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ElasticVersion2 = "2.x"
	ElasticVersion5 = "5.x"
)

type ElasticReader struct {
	esindex   string //es索引
	estype    string //es type
	eshost    string //eshost+port
	readBatch int    // 每次读取的数据量
	keepAlive string //scrollID 保留时间
	esVersion string //ElasticSearch version
	readChan  chan json.RawMessage

	meta   *Meta  // 记录offset的元数据
	offset string // 当前处理es的offset

	status  int32
	mux     sync.Mutex
	started bool
}

func NewESReader(meta *Meta, readBatch int, estype, esindex, eshost, esVersion, keepAlive string) (er *ElasticReader, err error) {

	offset, _, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
	}
	er = &ElasticReader{
		esindex:   esindex,
		estype:    estype,
		eshost:    eshost,
		esVersion: esVersion,
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
		log.Infof("Runner[%v] %v stopping", er.meta.RunnerName, er.Name())
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
	log.Infof("Runner[%v] %v pull data deamon started", er.meta.RunnerName, er.Name())
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
		log.Infof("Runner[%v] %v successfully finished", er.meta.RunnerName, er.Name())
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
	switch er.esVersion {
	case ElasticVersion5:
		var client *elasticV5.Client
		client, err = elasticV5.NewClient(elasticV5.SetURL(er.eshost))
		if err != nil {
			return
		}
		scroll := client.Scroll(er.esindex).Type(er.estype).Size(er.readBatch).KeepAlive(er.keepAlive)
		for {
			ctx := context.Background()
			results, err := scroll.ScrollId(er.offset).Do(ctx)
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
				log.Warnf("Runner[%v] %v stopped from running", er.meta.RunnerName, er.Name())
				return nil
			}
		}
	default:
		var client *elasticV3.Client
		client, err = elasticV3.NewClient(elasticV3.SetURL(er.eshost))
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
				log.Warnf("Runner[%v] %v stopped from running", er.meta.RunnerName, er.Name())
				return nil
			}
		}

	}
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (er *ElasticReader) SyncMeta() {
	if err := er.meta.WriteOffset(er.offset, 0); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", er.meta.RunnerName, er.Name(), err)
	}
	return
}

func (er *ElasticReader) SetMode(mode string, v interface{}) error {
	return errors.New("ElasticReader not support read mode")
}
