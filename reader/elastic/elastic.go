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
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.Reader       = &Reader{}
)

type Reader struct {
	meta *reader.Meta // 记录offset的元数据
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32
	// Note: 原子操作，用于表示获取数据的线程运行状态，只可能是 StatusInit 和 StatusRunning
	routineStatus int32

	stopChan chan struct{}
	readChan chan json.RawMessage
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	esindex   string //es索引
	estype    string //es type
	eshost    string //eshost+port
	readBatch int    // 每次读取的数据量
	keepAlive string //scrollID 保留时间
	esVersion string //ElasticSearch version
	offset    string // 当前处理es的offset
}

func init() {
	reader.RegisterConstructor(reader.ModeElastic, NewReader)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
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
	return &Reader{
		meta:          meta,
		status:        reader.StatusInit,
		routineStatus: reader.StatusInit,
		stopChan:      make(chan struct{}),
		readChan:      make(chan json.RawMessage),
		errChan:       make(chan error),
		esindex:       esindex,
		estype:        estype,
		eshost:        eshost,
		esVersion:     esVersion,
		readBatch:     readBatch,
		keepAlive:     keepAlive,
		offset:        offset,
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopped
}

func (r *Reader) Name() string {
	return "ESReader:" + r.Source()
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("elastic reader not support read mode")
}

func (r *Reader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) exec() error {
	// 当上个任务还未执行完成的时候直接跳过
	if !atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusRunning) {
		log.Errorf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
		return nil
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			close(r.readChan)
			close(r.errChan)
		}
		atomic.StoreInt32(&r.routineStatus, reader.StatusInit)
	}()

	// Create a client
	switch r.esVersion {
	case reader.ElasticVersion6:
		client, err := elasticV6.NewClient(elasticV6.SetURL(r.eshost))
		if err != nil {
			return err
		}
		scroll := client.Scroll(r.esindex).Type(r.estype).Size(r.readBatch).KeepAlive(r.keepAlive)
		for {
			ctx := context.Background()
			results, err := scroll.ScrollId(r.offset).Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				r.readChan <- *hit.Source
			}
			r.offset = results.ScrollId
			if r.isStopping() || r.hasStopped() {
				return nil
			}
		}
	case reader.ElasticVersion5:
		client, err := elasticV5.NewClient(elasticV5.SetURL(r.eshost))
		if err != nil {
			return err
		}
		scroll := client.Scroll(r.esindex).Type(r.estype).Size(r.readBatch).KeepAlive(r.keepAlive)
		for {
			ctx := context.Background()
			results, err := scroll.ScrollId(r.offset).Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				r.readChan <- *hit.Source
			}
			r.offset = results.ScrollId
			if r.isStopping() || r.hasStopped() {
				return nil
			}
		}
	default:
		client, err := elasticV3.NewClient(elasticV3.SetURL(r.eshost))
		if err != nil {
			return err
		}
		scroll := client.Scroll(r.esindex).Type(r.estype).Size(r.readBatch).KeepAlive(r.keepAlive)
		for {
			results, err := scroll.ScrollId(r.offset).Do()
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				r.readChan <- *hit.Source
			}
			r.offset = results.ScrollId
			if r.isStopping() || r.hasStopped() {
				return nil
			}
		}

	}
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			if err := r.exec(); err != nil {
				log.Errorf("Runner[%v] %q exec failed: %v ", r.meta.RunnerName, r.Name(), err)
				r.setStatsError(err.Error())
				r.sendError(err)
			}

			select {
			case <-r.stopChan:
				atomic.StoreInt32(&r.status, reader.StatusStopped)
				log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
				return
			case <-ticker.C:
			}
		}
	}()
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return r.eshost + "_" + r.esindex + "_" + r.estype
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case data := <-r.readChan:
		return string(data), nil
	case err := <-r.errChan:
		return "", err
	case <-timer.C:
	}

	return "", nil
}

func (r *Reader) Status() StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

// SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复
func (r *Reader) SyncMeta() {
	if err := r.meta.WriteOffset(r.offset, 0); err != nil {
		log.Errorf("Runner[%v] reader %q sync meta failed: %v", r.meta.RunnerName, r.Name(), err)
	}
	return
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.LoadInt32(&r.routineStatus) != reader.StatusRunning {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
}
