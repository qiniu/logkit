package reader

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"strings"

	"github.com/go-redis/redis"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
)

const (
	DateTypeHash          = "hash"
	DateTypeSortedSet     = "sortedSet"
	DataTypeSet           = "set"
	DataTypeString        = "string"
	DataTypeList          = "list"
	DataTypeChannel       = "channel"
	DataTypePatterChannel = "pattern_channel"
)

const (
	KeyRedisDataType   = "redis_datatype" // 必填
	KeyRedisDB         = "redis_db"       //默认 是0
	KeyRedisKey        = "redis_key"      //必填
	KeyRedisHashArea   = "redisHash_area" 
	KeyRedisAddress    = "redis_address"  // 默认127.0.0.1:6379
	KeyRedisPassword   = "redis_password"
	KeyTimeoutDuration = "redis_timeout"
)

type RedisReader struct {
	meta   *Meta
	opt    RedisOptionn
	client *redis.Client

	readChan  chan string
	channelIn <-chan *redis.Message

	status  int32
	mux     sync.Mutex
	started bool

	stats     utils.StatsInfo
	statsLock sync.RWMutex
}

type RedisOptionn struct {
	dataType string
	db       int
	key      string
	area     string
	//key     []string
	address  string //host:port 列表
	password string
	//batchCount int
	//threads    int
	timeout time.Duration
}

func NewRedisReader(meta *Meta, conf conf.MapConf) (rr *RedisReader, err error) {
	dataType, err := conf.GetString(KeyRedisDataType)
	if err != nil {
		return
	}
	db, _ := conf.GetIntOr(KeyRedisDB, 0)
	//key := keys
	key, err := conf.GetString(KeyRedisKey)
	if err != nil {
		return
	}
	area, err := conf.GetString(KeyRedisHashArea)
	address, _ := conf.GetStringOr(KeyRedisAddress, "127.0.0.1:6379")
	password, _ := conf.GetStringOr(KeyRedisPassword, "")
	KeyTimeoutDuration, _ := conf.GetStringOr(KeyTimeoutDuration, "5s")
	timeout, err := time.ParseDuration(KeyTimeoutDuration)
	if err != nil {
		return
	}
	opt := RedisOptionn{
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

	rr = &RedisReader{
		meta:   meta,
		opt:    opt,
		client: client,

		readChan:  make(chan string),
		status:    StatusInit,
		mux:       sync.Mutex{},
		started:   false,
		statsLock: sync.RWMutex{},
	}
	return
}

func (rr *RedisReader) Name() string {
	return fmt.Sprintf("[%s],[%v],[%s]", rr.opt.dataType, rr.opt.db, rr.opt.key)
}

func (rr *RedisReader) setStatsError(err string) {
	rr.statsLock.Lock()
	defer rr.statsLock.Unlock()
	rr.stats.LastError = err
}

func (rr *RedisReader) Status() utils.StatsInfo {
	rr.statsLock.RLock()
	defer rr.statsLock.RUnlock()
	return rr.stats
}

func (rr *RedisReader) Source() string {
	return fmt.Sprintf("[%s],[%v],[%s]", rr.opt.dataType, rr.opt.db, rr.opt.key)
}

func (rr *RedisReader) ReadLine() (data string, err error) {
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
func (rr *RedisReader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&rr.status, StatusRunning, StatusStoping) {
		log.Infof("Runner[%v] %v stopping", rr.meta.RunnerName, rr.Name())
	} else {
		close(rr.readChan)
		rr.client.Close()
	}
	return
}

func (rr *RedisReader) SyncMeta() {
	log.Debugf("Runner[%v] %v redis reader do not support meta sync", rr.meta.RunnerName, rr.Name())
	return
}

func (rr *RedisReader) Start() {
	rr.mux.Lock()
	defer rr.mux.Unlock()
	if rr.started {
		return
	}
	rr.started = true
	switch rr.opt.dataType {
	case DataTypeChannel:
		//rr.channelIn = rr.client.Subscribe(rr.opt.key[0],rr.opt.key[1]).Channel()
		rr.channelIn = rr.client.Subscribe(rr.opt.key).Channel()
	case DataTypePatterChannel:
		//rr.channelIn = rr.client.PSubscribe(rr.opt.key[0],rr.opt.key[1]).Channel()
		rr.channelIn = rr.client.PSubscribe(rr.opt.key).Channel()
	case DataTypeList:
	case DataTypeString:
	case DataTypeSet:
	case DateTypeSortedSet:
	case DateTypeHash:
	default:
		err := fmt.Errorf("data Type < %v > not exist, exit", rr.opt.dataType)
		log.Error(err)
		return
	}
	go rr.run()
	log.Infof("Runner[%v] %v pull data daemon started", rr.meta.RunnerName, rr.Name())
}

func (rr *RedisReader) run() (err error) {
	// 防止并发run
	for {
		if atomic.LoadInt32(&rr.status) == StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&rr.status, StatusInit, StatusRunning) {
			break
		}
	}
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&rr.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&rr.status, StatusStoping, StatusStopped) {
			close(rr.readChan)
			rr.client.Close()
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", rr.meta.RunnerName, rr.Name())
		}
	}()
	// 开始work逻辑
	for {
		if atomic.LoadInt32(&rr.status) == StatusStoping {
			log.Warnf("Runner[%v] %v stopped from running", rr.meta.RunnerName, rr.Name())
			return
		}
		switch rr.opt.dataType {
		case DataTypeChannel, DataTypePatterChannel:
			message := <-rr.channelIn
			if message != nil {
				rr.readChan <- message.Payload
			}
		case DataTypeList:
			//ans, subErr := rr.client.BLPop(rr.opt.timeout, rr.opt.key[0],rr.opt.key[1]).Result()
			ans, subErr := rr.client.BLPop(rr.opt.timeout, rr.opt.key).Result()
			if subErr != nil && subErr != redis.Nil {
				log.Errorf("Runner[%v] %v BLPop redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
				rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " BLPop redis error " + subErr.Error())
			} else if len(ans) > 1 {
				rr.readChan <- ans[1]
			} else if len(ans) == 1 {
				log.Errorf("Runner[%v] %v list read only one result in arrary %v", rr.meta.RunnerName, rr.Name(), ans)
				rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " list read only one result in arrary: " + strings.Join(ans, ","))
			}
			//Added string support for redis
		case DataTypeString:
			//anString, subErr := rr.client.Get(rr.opt.key[0]).Result()
			anString, subErr := rr.client.Get(rr.opt.key).Result()
			if subErr != nil && subErr != redis.Nil {
				log.Errorf("Runner[%v] %v Get redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
				rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
			} else if anString != "" {
				//Avoid data duplication
				//rr.client.Del(rr.opt.key[0])
				rr.client.Del(rr.opt.key)
				rr.readChan <- anString
			}
			//Added set support for redis
	    case DataTypeSet:
	        anSet, subErr := rr.client.SPop(rr.opt.key,).Result()
			if subErr != nil && subErr != redis.Nil {
				log.Errorf("Runner[%v] %v SPop redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
				rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
			} else if anSet == "" {
				log.Errorf("Runner[%v] %v set read [%s] data is not exist %v", rr.meta.RunnerName, rr.Name(), rr.opt.dataType, anSet)
			}else {
				rr.readChan <- anSet
			}
			//Added sortedSet support for redis
	    case DateTypeSortedSet:
		    anSortedSet, subErr := rr.client.ZRange(rr.opt.key,0,-1).Result()
		    if subErr != nil && subErr != redis.Nil {
				log.Errorf("Runner[%v] %v ZRange redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
				rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
			} else if len(anSortedSet) > 0 {
				rr.client.Del(rr.opt.key)
				rr.readChan <- anSortedSet[0]
			}
			//Added hash support for redis
	    case DateTypeHash:
		    anHash, subErr := rr.client.HGet(rr.opt.key,rr.opt.area).Result()  //redis key and area for hash
		    if subErr != nil && subErr != redis.Nil {
				log.Errorf("Runner[%v] %v HGetAll redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
				rr.setStatsError("Runner[" + rr.meta.RunnerName + "] " + rr.Name() + " Get redis error " + subErr.Error())
			}else if anHash != ""{
				rr.client.Del(rr.opt.key)
				rr.readChan <- anHash
			}
		default:
			err = fmt.Errorf("data Type < %v > not exist, exit", rr.opt.dataType)
			log.Error(err)
			rr.setStatsError(err.Error())
			return
		}
	}
}

func (rr *RedisReader) SetMode(mode string, v interface{}) error {
	return errors.New("RedisReader not support read mode")
}
