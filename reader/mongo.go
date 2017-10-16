package reader

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/logkit/utils"

	"strings"

	"github.com/qiniu/log"
	"github.com/robfig/cron"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// CollectionFilter is just a typed map of strings of map[string]interface{}
type CollectionFilter map[string]interface{}

const (
	MongoDefaultOffsetKey = "_id"
)

type MongoReader struct {
	host              string
	database          string
	collection        string
	offsetkey         string
	readBatch         int // 每次读取的数据量
	collectionFilters map[string]CollectionFilter

	Cron         *cron.Cron //定时任务
	loop         bool
	loopDuration time.Duration
	readChan     chan []byte //bson
	meta         *Meta       // 记录offset的元数据
	session      *mgo.Session
	offset       interface{} //对于默认的offset_key: "_id", 是objectID作为offset，存储的表现形式是string，其他则是int64

	execOnStart bool
	status      int32
	started     bool
	mux         sync.Mutex
	stats       utils.StatsInfo
	statsLock   sync.RWMutex
}

func NewMongoReader(meta *Meta, readBatch int, host, database, collection, offsetkey, cronSched, filters, certfile string, execOnStart bool) (mr *MongoReader, err error) {

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
	mr = &MongoReader{
		meta:       meta,
		host:       host,
		database:   database,
		collection: collection,
		offsetkey:  offsetkey,
		readBatch:  readBatch, //这个参数目前没有用

		collectionFilters: map[string]CollectionFilter{},
		Cron:              cron.New(),
		status:            StatusInit,
		readChan:          make(chan []byte),
		execOnStart:       execOnStart,
		started:           false,
		mux:               sync.Mutex{},
		statsLock:         sync.RWMutex{},
	}
	if offsetkey == MongoDefaultOffsetKey {
		if bson.IsObjectIdHex(keyOrObj) {
			mr.offset = bson.ObjectIdHex(keyOrObj)
		} else {
			mr.offset = nil
		}
	} else {
		mr.offset = offset
	}

	if filters != "" {
		if jerr := json.Unmarshal([]byte(filters), &mr.collectionFilters); jerr != nil {
			err = errors.New("malformed collection_filters")
			return
		}
	}
	if len(cronSched) > 0 {
		cronSched = strings.ToLower(cronSched)
		if strings.HasPrefix(cronSched, Loop) {
			mr.loop = true
			mr.loopDuration, err = parseLoopDuration(cronSched)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", mr.meta.RunnerName, mr.Name(), err)
				err = nil
			}
		} else {
			err = mr.Cron.AddFunc(cronSched, mr.run)
			if err != nil {
				return
			}
			log.Infof("Runner[%v] %v Cron added with schedule <%v>", mr.meta.RunnerName, mr.Name(), cronSched)
		}
	}

	return mr, nil
}

func (mr *MongoReader) Name() string {
	return "MongoReader:" + mr.Source()
}

func (mr *MongoReader) Source() string {
	return mr.host + "_" + mr.database + "_" + mr.collection
}

func (mr *MongoReader) Status() utils.StatsInfo {
	mr.statsLock.RLock()
	defer mr.statsLock.RUnlock()
	return mr.stats
}

func (mr *MongoReader) setStatsError(err string) {
	mr.statsLock.Lock()
	defer mr.statsLock.Unlock()
	mr.stats.LastError = err
}

func (mr *MongoReader) Close() (err error) {
	mr.Cron.Stop()
	if mr.session != nil {
		mr.session.Close()
	}
	if atomic.CompareAndSwapInt32(&mr.status, StatusRunning, StatusStoping) {
		log.Infof("Runner[%v] %v stopping", mr.meta.RunnerName, mr.Name())
	} else {
		close(mr.readChan)
	}
	return
}

//Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
func (mr *MongoReader) Start() {
	mr.mux.Lock()
	defer mr.mux.Unlock()
	if mr.started {
		return
	}
	if mr.loop {
		go mr.LoopRun()
	} else {
		if mr.execOnStart {
			go mr.run()
		}
		mr.Cron.Start()
	}
	mr.started = true
	log.Infof("Runner[%v] %v pull data daemon started", mr.meta.RunnerName, mr.Name())
}

func (mr *MongoReader) LoopRun() {
	for {
		if atomic.LoadInt32(&mr.status) == StatusStoping {
			log.Warnf("Runner[%v] %v stopped from running", mr.meta.RunnerName, mr.Name())
			return
		}
		mr.run()
		time.Sleep(mr.loopDuration)
	}
}

func (mr *MongoReader) ReadLine() (data string, err error) {
	if !mr.started {
		mr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-mr.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (mr *MongoReader) run() {
	var err error
	// 防止并发run
	for {
		if atomic.LoadInt32(&mr.status) == StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&mr.status, StatusInit, StatusRunning) {
			break
		}
	}
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&mr.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&mr.status, StatusStoping, StatusStopped) {
			close(mr.readChan)
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", mr.meta.RunnerName, mr.Name())
		}
	}()

	// 开始work逻辑
	for {
		if atomic.LoadInt32(&mr.status) == StatusStoping {
			log.Warnf("Runner[%v] %v stopped from running", mr.meta.RunnerName, mr.Name())
			return
		}
		err = mr.exec()
		if err == nil {
			log.Infof("Runner[%v] %v successfully exec", mr.meta.RunnerName, mr.Name())
			return
		}
		log.Error(err)
		mr.setStatsError(err.Error())
		time.Sleep(3 * time.Second)
	}
}

func (mr *MongoReader) catQuery(c string, lastID interface{}, mgoSession *mgo.Session) *mgo.Query {
	query := bson.M{}
	if f, ok := mr.collectionFilters[c]; ok {
		query = bson.M(f)
	}
	if lastID != nil {
		query[mr.offsetkey] = bson.M{"$gt": lastID}
	}
	return mgoSession.DB(mr.database).C(c).Find(query).Sort(mr.offsetkey)
}

func (mr *MongoReader) exec() (err error) {
	if mr.session == nil {
		mr.session, err = utils.MongoDail(mr.host, "", 0)
		if err != nil {
			return
		}
		mr.session.SetSocketTimeout(time.Second * 5)
		mr.session.SetSyncTimeout(time.Second * 5)
	} else {
		err := mr.session.Ping()
		if err != nil {
			mr.session.Refresh()
			mr.session.SetSocketTimeout(time.Second * 5)
			mr.session.SetSyncTimeout(time.Second * 5)
		} else {
			time.Sleep(time.Second * 5)
		}
	}

	iter := mr.catQuery(mr.collection, mr.offset, mr.session).Iter()

	var result bson.M
	for iter.Next(&result) {
		if atomic.LoadInt32(&mr.status) == StatusStoping {
			log.Warnf("Runner[%v] %v stopped from running", mr.meta.RunnerName, mr.Name())
			return nil
		}
		if id, ok := result[mr.offsetkey]; ok {
			mr.offset = id
		}
		bytes, ierr := json.Marshal(result)
		if ierr != nil {
			log.Errorf("Runner[%v] %v json marshal inner error %v", mr.meta.RunnerName, result, ierr)
		}
		mr.readChan <- bytes
		result = bson.M{}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (mr *MongoReader) SyncMeta() {
	var key string
	var offset int64
	if mr.offsetkey == MongoDefaultOffsetKey {
		if id, ok := mr.offset.(bson.ObjectId); ok {
			key = id.Hex()
		}
	} else {
		key = mr.offsetkey
		if ofs, ok := mr.offset.(int64); ok {
			offset = ofs
		} else if ofs, ok := mr.offset.(int); ok {
			offset = int64(ofs)
		}
	}
	if err := mr.meta.WriteOffset(key, offset); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", mr.meta.RunnerName, mr.Name(), err)
	}
	return
}

func (mr *MongoReader) SetMode(mode string, v interface{}) error {
	return errors.New("MongoDB Reader not support read mode")
}
