package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	elasticV6 "github.com/olivere/elastic"
	"github.com/qiniu/log"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

type Reader struct {
	esindex   string //es索引
	estype    string //es type
	eshost    string //eshost+port
	readBatch int    // 每次读取的数据量
	keepAlive string //scrollID 保留时间
	esVersion string //ElasticSearch version
	readChan  chan json.RawMessage

	meta   *reader.Meta // 记录offset的元数据
	offset string       // 当前处理es的offset

	stats     StatsInfo
	statsLock sync.RWMutex

	status  int32
	mux     sync.Mutex
	started bool
}

func init() {
	reader.RegisterConstructor(reader.ModeElastic, NewReader)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (er reader.Reader, err error) {
	readBatch, _ := conf.GetIntOr(reader.KeyESReadBatch, 100)
	estype, err := conf.GetString(reader.KeyESType)
	if err != nil {
		return nil, err
	}
	esindex, err := conf.GetString(reader.KeyESIndex)
	if err != nil {
		return nil, err
	}
	eshost, _ := conf.GetStringOr(reader.KeyESHost, "http://localhost:9200")
	if !strings.HasPrefix(eshost, "http://") && !strings.HasPrefix(eshost, "https://") {
		eshost = "http://" + eshost
	}
	esVersion, _ := conf.GetStringOr(reader.KeyESVersion, reader.ElasticVersion3)
	keepAlive, _ := conf.GetStringOr(reader.KeyESKeepAlive, "6h")

	offset, _, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
	}
	er = &Reader{
		esindex:   esindex,
		estype:    estype,
		eshost:    eshost,
		esVersion: esVersion,
		readBatch: readBatch,
		keepAlive: keepAlive,
		meta:      meta,
		status:    reader.StatusInit,
		offset:    offset,
		readChan:  make(chan json.RawMessage),
		mux:       sync.Mutex{},
		statsLock: sync.RWMutex{},
		started:   false,
	}

	return er, nil
}

func (er *Reader) Name() string {
	return "ESReader:" + er.Source()
}

func (er *Reader) Source() string {
	return er.eshost + "_" + er.esindex + "_" + er.estype
}

func (er *Reader) setStatsError(err string) {
	er.statsLock.Lock()
	defer er.statsLock.Unlock()
	er.stats.LastError = err
}

func (er *Reader) Status() StatsInfo {
	er.statsLock.RLock()
	defer er.statsLock.RUnlock()
	return er.stats
}

func (er *Reader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&er.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] %v stopping", er.meta.RunnerName, er.Name())
	} else {
		atomic.CompareAndSwapInt32(&er.status, reader.StatusInit, reader.StatusStopped)
		close(er.readChan)
	}
	return
}

//Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
func (er *Reader) Start() {
	er.mux.Lock()
	defer er.mux.Unlock()
	if er.started {
		return
	}
	go er.run()
	er.started = true
	log.Infof("Runner[%v] %v pull data deamon started", er.meta.RunnerName, er.Name())
}

func (er *Reader) ReadLine() (data string, err error) {
	if !er.started {
		er.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-er.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (er *Reader) run() (err error) {
	// 防止并发run
	for {
		if atomic.LoadInt32(&er.status) == reader.StatusStopped || atomic.LoadInt32(&er.status) == reader.StatusStopping {
			return
		}
		if atomic.CompareAndSwapInt32(&er.status, reader.StatusInit, reader.StatusRunning) {
			break
		}
	}
	//double check
	if atomic.LoadInt32(&er.status) == reader.StatusStopped || atomic.LoadInt32(&er.status) == reader.StatusStopping {
		return
	}
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&er.status, reader.StatusRunning, reader.StatusInit)
		if atomic.CompareAndSwapInt32(&er.status, reader.StatusStopping, reader.StatusStopped) {
			close(er.readChan)
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", er.meta.RunnerName, er.Name())
		}
	}()

	// 开始work逻辑
	for {
		if atomic.LoadInt32(&er.status) == reader.StatusStopping {
			log.Warnf("%v stopped from running", er.Name())
			return
		}
		err = er.exec()
		if err == nil {
			log.Infof("%v successfully exec", er.Name())
			return
		}
		log.Error(err)
		er.setStatsError(err.Error())
		time.Sleep(3 * time.Second)
	}
}

func (er *Reader) exec() (err error) {
	// Create a client
	switch er.esVersion {
	case reader.ElasticVersion6:
		var client *elasticV6.Client
		client, err = elasticV6.NewClient(elasticV6.SetURL(er.eshost))
		if err != nil {
			return
		}
		scroll := client.Scroll(er.esindex).Type(er.estype).Size(er.readBatch).KeepAlive(er.keepAlive)
		for {
			ctx := context.Background()
			results, err := scroll.ScrollId(er.offset).Do(ctx)
			if err == io.EOF {
				return nil // builtin results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				er.readChan <- *hit.Source
			}
			er.offset = results.ScrollId
			if atomic.LoadInt32(&er.status) == reader.StatusStopping {
				log.Warnf("Runner[%v] %v stopped from running", er.meta.RunnerName, er.Name())
				return nil
			}
		}
	case reader.ElasticVersion5:
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
				return nil // builtin results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				er.readChan <- *hit.Source
			}
			er.offset = results.ScrollId
			if atomic.LoadInt32(&er.status) == reader.StatusStopping {
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
				return nil // builtin results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				er.readChan <- *hit.Source
			}
			er.offset = results.ScrollId
			if atomic.LoadInt32(&er.status) == reader.StatusStopping {
				log.Warnf("Runner[%v] %v stopped from running", er.meta.RunnerName, er.Name())
				return nil
			}
		}

	}
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (er *Reader) SyncMeta() {
	if err := er.meta.WriteOffset(er.offset, 0); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", er.meta.RunnerName, er.Name(), err)
	}
	return
}

func (er *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("ElasticReader not support read mode")
}
