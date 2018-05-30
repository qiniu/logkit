package redis

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

func init() {
	reader.RegisterConstructor(reader.ModeRedis, NewReader)
}

type Reader struct {
	meta   *reader.Meta
	opts   Options
	client *redis.Client

	readChan  chan string
	channelIn <-chan *redis.Message

	status  int32
	mux     sync.Mutex
	started bool

	stats     StatsInfo
	statsLock sync.RWMutex
}

type Options struct {
	dataType string
	db       int
	//key      string
	area     string
	key      []string
	address  string //host:port 列表
	password string
	//batchCount int
	//threads    int
	timeout time.Duration
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (rr reader.Reader, err error) {
	dataType, err := conf.GetString(reader.KeyRedisDataType)
	if err != nil {
		return
	}
	db, _ := conf.GetIntOr(reader.KeyRedisDB, 0)
	key, _ := conf.GetStringList(reader.KeyRedisKey)
	if err != nil {
		return
	}
	area, err := conf.GetString(reader.KeyRedisHashArea)
	address, _ := conf.GetStringOr(reader.KeyRedisAddress, "127.0.0.1:6379")
	password, _ := conf.GetStringOr(reader.KeyRedisPassword, "")
	KeyTimeoutDuration, _ := conf.GetStringOr(reader.KeyTimeoutDuration, "5s")
	timeout, err := time.ParseDuration(KeyTimeoutDuration)
	if err != nil {
		return
	}
	opt := Options{
		address:  address,
		password: password,
		db:       db,
		key:      key,
		area:     area,
		timeout:  timeout,
		dataType: dataType,
	}
	client := redis.NewClient(&redis.Options{
		Addr:     opt.address,
		DB:       opt.db,
		Password: opt.password,
	})

	rr = &Reader{
		meta:      meta,
		opts:      opt,
		client:    client,
		readChan:  make(chan string),
		status:    reader.StatusInit,
		mux:       sync.Mutex{},
		started:   false,
		statsLock: sync.RWMutex{},
	}
	return
}

func (rr *Reader) Name() string {
	return fmt.Sprintf("[%s],[%v],[%s]", rr.opts.dataType, rr.opts.db, rr.opts.key)
}

func (rr *Reader) setStatsError(err string) {
	rr.statsLock.Lock()
	defer rr.statsLock.Unlock()
	rr.stats.LastError = err
}

func (rr *Reader) Status() StatsInfo {
	rr.statsLock.RLock()
	defer rr.statsLock.RUnlock()
	return rr.stats
}

func (rr *Reader) Source() string {
	return fmt.Sprintf("[%s],[%v],[%s]", rr.opts.dataType, rr.opts.db, rr.opts.key)
}

func (rr *Reader) ReadLine() (data string, err error) {
	if !rr.started {
		rr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-rr.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return

}
func (rr *Reader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&rr.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] %v stopping", rr.meta.RunnerName, rr.Name())
	} else {
		atomic.CompareAndSwapInt32(&rr.status, reader.StatusInit, reader.StatusStopped)
		close(rr.readChan)
		rr.client.Close()
	}
	return
}

func (rr *Reader) SyncMeta() {
	log.Debugf("Runner[%v] %v redis reader do not support meta sync", rr.meta.RunnerName, rr.Name())
	return
}

func (rr *Reader) Start() {
	rr.mux.Lock()
	defer rr.mux.Unlock()
	if rr.started {
		return
	}
	rr.started = true
	switch rr.opts.dataType {
	case reader.DataTypeChannel:
		rr.channelIn = rr.client.Subscribe(rr.opts.key...).Channel()
	case reader.DataTypePatterChannel:
		rr.channelIn = rr.client.PSubscribe(rr.opts.key...).Channel()
	case reader.DataTypeList:
	case reader.DataTypeString:
	case reader.DataTypeSet:
	case reader.DateTypeSortedSet:
	case reader.DateTypeHash:
	default:
		err := fmt.Errorf("data Type < %v > not exist, exit", rr.opts.dataType)
		log.Error(err)
		return
	}
	go rr.run()
	log.Infof("Runner[%v] %v pull data daemon started", rr.meta.RunnerName, rr.Name())
}

func (rr *Reader) run() (err error) {
	// 防止并发run
	for {
		if atomic.LoadInt32(&rr.status) == reader.StatusStopped || atomic.LoadInt32(&rr.status) == reader.StatusStopping {
			return
		}
		if atomic.CompareAndSwapInt32(&rr.status, reader.StatusInit, reader.StatusRunning) {
			break
		}
	}
	//double check
	if atomic.LoadInt32(&rr.status) == reader.StatusStopped || atomic.LoadInt32(&rr.status) == reader.StatusStopping {
		return
	}
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&rr.status, reader.StatusRunning, reader.StatusInit)
		if atomic.CompareAndSwapInt32(&rr.status, reader.StatusStopping, reader.StatusStopped) {
			close(rr.readChan)
			rr.client.Close()
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", rr.meta.RunnerName, rr.Name())
		}
	}()
	// 开始work逻辑
	for {
		if atomic.LoadInt32(&rr.status) == reader.StatusStopping {
			log.Warnf("Runner[%v] %v stopped from running", rr.meta.RunnerName, rr.Name())
			return
		}
		switch rr.opts.dataType {
		case reader.DataTypeChannel, reader.DataTypePatterChannel:
			message := <-rr.channelIn
			if message != nil {
				rr.readChan <- message.Payload
			}
		case reader.DataTypeList:
			for _, key := range rr.opts.key {
				ans, subErr := rr.client.BLPop(rr.opts.timeout, key).Result()
				if subErr != nil && subErr != redis.Nil {
					log.Errorf("Runner[%v] %v BLPop redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
					rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " BLPop redis error " + subErr.Error())
				} else if len(ans) > 1 {
					rr.readChan <- ans[1]
				} else if len(ans) == 1 {
					log.Errorf("Runner[%v] %v list read only one result in arrary %v", rr.meta.RunnerName, rr.Name(), ans)
					rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " list read only one result in arrary: " + strings.Join(ans, ","))
				}
			}
			//Added string support for redis
		case reader.DataTypeString:
			for _, key := range rr.opts.key {
				anString, subErr := rr.client.Get(key).Result()
				if subErr != nil && subErr != redis.Nil {
					log.Errorf("Runner[%v] %v Get redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
					rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
				} else if anString != "" {
					//Avoid data duplication
					rr.client.Del(key)
					rr.readChan <- anString
				}
			}
			//Added set support for redis
		case reader.DataTypeSet:
			for _, key := range rr.opts.key {
				anSet, subErr := rr.client.SPop(key).Result()
				if subErr != nil && subErr != redis.Nil {
					log.Errorf("Runner[%v] %v SPop redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
					rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
				} else if anSet != "" {
					rr.readChan <- anSet
				}
			}
			//Added sortedSet support for redis
		case reader.DateTypeSortedSet:
			for _, key := range rr.opts.key {
				anSortedSet, subErr := rr.client.ZRange(key, 0, -1).Result()
				if subErr != nil && subErr != redis.Nil {
					log.Errorf("Runner[%v] %v ZRange redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
					rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
				} else if len(anSortedSet) > 0 {
					rr.client.Del(key)
					rr.readChan <- anSortedSet[0]
				}
			}
			//Added hash support for redis
		case reader.DateTypeHash:
			for _, key := range rr.opts.key {
				anHash, subErr := rr.client.HGet(key, rr.opts.area).Result() //redis key and area for hash
				if subErr != nil && subErr != redis.Nil {
					log.Errorf("Runner[%v] %v HGetAll redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
					rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
				} else if anHash != "" {
					rr.client.Del(key)
					rr.readChan <- anHash
				}
			}
		default:
			err = fmt.Errorf("data Type < %v > not exist, exit", rr.opts.dataType)
			log.Error(err)
			rr.setStatsError(err.Error())
			return
		}
	}
}

func (rr *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("RedisReader not support read mode")
}
