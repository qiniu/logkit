package mongo

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"
	"github.com/robfig/cron"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.Reader       = &Reader{}
)

func init() {
	reader.RegisterConstructor(ModeMongo, NewReader)
}

// CollectionFilter is just a typed map of strings of map[string]interface{}
type CollectionFilter map[string]interface{}

const (
	DefaultOffsetKey = "_id"
)

type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32
	/*
		Note: 原子操作，用于表示获取数据的线程运行状态

		- StatusInit: 当前没有任务在执行
		- StatusRunning: 当前有任务正在执行
		- StatusStopping: 数据管道已经由上层关闭，执行中的任务完成时直接退出无需再处理
	*/
	routineStatus int32

	stopChan chan struct{}
	readChan chan []byte //bson
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	host              string
	database          string
	collection        string
	offsetkey         string
	readBatch         int // 每次读取的数据量
	collectionFilters map[string]CollectionFilter

	isLoop       bool
	loopDuration time.Duration
	execOnStart  bool
	Cron         *cron.Cron //定时任务
	session      *mgo.Session
	offset       interface{} //对于默认的offset_key: "_id", 是objectID作为offset，存储的表现形式是string，其他则是int64

}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	readBatch, _ := conf.GetIntOr(KeyMongoReadBatch, 100)
	database, err := conf.GetString(KeyMongoDatabase)
	if err != nil {
		return nil, err
	}
	collection, err := conf.GetString(KeyMongoCollection)
	if err != nil {
		return nil, err
	}
	host, err := conf.GetPasswordEnvString(KeyMongoHost)
	if err != nil {
		return nil, err
	}
	offsetkey, _ := conf.GetStringOr(KeyMongoOffsetKey, DefaultOffsetKey)
	cronSched, _ := conf.GetStringOr(KeyMongoCron, "")
	execOnStart, _ := conf.GetBoolOr(KeyMongoExecOnstart, true)
	filters, _ := conf.GetStringOr(KeyMongoFilters, "")
	certfile, _ := conf.GetStringOr(KeyMongoCert, "")

	keyOrObj, offset, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err: %v, omit meta data...", meta.RunnerName, meta.MetaFile(), err)
	}
	if keyOrObj != offsetkey {
		offset = 0
	}
	if certfile != "" {
		log.Warnf("Runner[%v] MongoDB reader does not support certfile Now", meta.RunnerName)
		//TODO mongo鉴权暂时不支持
	}
	r := &Reader{
		meta:              meta,
		status:            StatusInit,
		routineStatus:     StatusInit,
		stopChan:          make(chan struct{}),
		readChan:          make(chan []byte),
		errChan:           make(chan error),
		host:              host,
		database:          database,
		collection:        collection,
		offsetkey:         offsetkey,
		readBatch:         readBatch, //这个参数目前没有用
		collectionFilters: map[string]CollectionFilter{},
		execOnStart:       execOnStart,
		Cron:              cron.New(),
	}
	if offsetkey == DefaultOffsetKey {
		if bson.IsObjectIdHex(keyOrObj) {
			r.offset = bson.ObjectIdHex(keyOrObj)
		} else {
			r.offset = nil
		}
	} else {
		r.offset = offset
	}

	if filters != "" {
		if jerr := jsoniter.Unmarshal([]byte(filters), &r.collectionFilters); jerr != nil {
			return nil, errors.New("malformed collection_filters")
		}
	}
	if len(cronSched) > 0 {
		cronSched = strings.ToLower(cronSched)
		if strings.HasPrefix(cronSched, Loop) {
			r.isLoop = true
			r.loopDuration, err = reader.ParseLoopDuration(cronSched)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", r.meta.RunnerName, r.Name(), err)
			}
			if r.loopDuration.Nanoseconds() <= 0 {
				r.loopDuration = 1 * time.Second
			}
		} else {
			err = r.Cron.AddFunc(cronSched, r.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron added with schedule <%v>", r.meta.RunnerName, r.Name(), cronSched)
		}
	}
	return r, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *Reader) Name() string {
	return "MongoReader<" + r.Source() + ">"
}

func (_ *Reader) SetMode(_ string, _ interface{}) error {
	return errors.New("MongoDB Reader does not support read mode")
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

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	if r.isLoop {
		go func() {
			ticker := time.NewTicker(r.loopDuration)
			defer ticker.Stop()
			for {
				r.run()

				select {
				case <-r.stopChan:
					atomic.StoreInt32(&r.status, StatusStopped)
					log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
					return
				case <-ticker.C:
				}
			}
		}()

	} else {
		if r.execOnStart {
			go r.run()
		}
		r.Cron.Start()
	}
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return r.host + "_" + r.database + "_" + r.collection
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
	var key string
	var offset int64
	if r.offsetkey == DefaultOffsetKey {
		if id, ok := r.offset.(bson.ObjectId); ok {
			key = id.Hex()
		}
	} else {
		key = r.offsetkey
		if ofs, ok := r.offset.(int64); ok {
			offset = ofs
		} else if ofs, ok := r.offset.(int); ok {
			offset = int64(ofs)
		}
	}
	if err := r.meta.WriteOffset(key, offset); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
	}
}

func (r *Reader) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	r.Cron.Stop()
	if r.session != nil {
		r.session.Close()
	}

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return
}

func (r *Reader) run() {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			errMsg := fmt.Sprintf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
			log.Error(errMsg)
			if !r.isLoop {
				// 通知上层 Cron 执行间隔可能过短或任务执行时间过长
				r.sendError(errors.New(errMsg))
			}
		}
		return
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			if atomic.CompareAndSwapInt32(&r.routineStatus, StatusRunning, StatusStopping) {
				close(r.readChan)
				close(r.errChan)
			}
			return
		}
		atomic.StoreInt32(&r.routineStatus, StatusInit)
	}()

	// 如果执行失败，最多重试 10 次
	for i := 1; i <= 10; i++ {
		// 判断上层是否已经关闭，先判断 routineStatus 再判断 status 可以保证同时只有一个 r.run 会运行到此处
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, task is interrupted", r.meta.RunnerName, r.Name())
			return
		}

		err := r.exec()
		if err == nil {
			log.Infof("Runner[%v] %q task has been successfully executed", r.meta.RunnerName, r.Name())
			return
		}

		log.Errorf("Runner[%v] %q task execution failed: %v ", r.meta.RunnerName, r.Name(), err)
		r.setStatsError(err.Error())
		r.sendError(err)

		if r.isLoop {
			return // 循环执行的任务上层逻辑已经等同重试
		}
		time.Sleep(3 * time.Second)
	}
	log.Warnf("Runner[%v] %q task execution failed and gave up after 10 tries", r.meta.RunnerName, r.Name())
}

func (r *Reader) catQuery(c string, lastID interface{}, mgoSession *mgo.Session) *mgo.Query {
	query := bson.M{}
	if f, ok := r.collectionFilters[c]; ok {
		query = bson.M(f)
	}
	if lastID != nil {
		query[r.offsetkey] = bson.M{"$gt": lastID}
	}
	return mgoSession.DB(r.database).C(c).Find(query).Sort(r.offsetkey)
}

func (r *Reader) exec() (err error) {
	if r.session == nil {
		r.session, err = utils.MongoDail(r.host, "", 0)
		if err != nil {
			return
		}
		r.session.SetSocketTimeout(time.Second * 5)
		r.session.SetSyncTimeout(time.Second * 5)
	} else {
		err := r.session.Ping()
		if err != nil {
			r.session.Refresh()
			r.session.SetSocketTimeout(time.Second * 5)
			r.session.SetSyncTimeout(time.Second * 5)
		} else {
			time.Sleep(time.Second * 5)
		}
	}

	iter := r.catQuery(r.collection, r.offset, r.session).Iter()

	var result bson.M
	for iter.Next(&result) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, iteration is interrupted", r.meta.RunnerName, r.Name())
			return nil
		}
		if id, ok := result[r.offsetkey]; ok {
			r.offset = id
		}
		bytes, ierr := jsoniter.Marshal(result)
		if ierr != nil {
			log.Errorf("Runner[%v] %v json marshal inner error %v", r.meta.RunnerName, result, ierr)
		}
		r.readChan <- bytes
		result = bson.M{}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}
