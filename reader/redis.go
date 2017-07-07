package reader

import (
	"errors"
	"sync/atomic"
	"time"

	"fmt"

	"sync"

	"github.com/go-redis/redis"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

const (
	DataTypeList          = "list"
	DataTypeChannel       = "channel"
	DataTypePatterChannel = "pattern_channel"
)

const (
	KeyRedisDataType   = "redis_datatype" // 必填
	KeyRedisDB         = "redis_db"       //默认 是0
	KeyRedisKey        = "redis_key"      //必填
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
}

type RedisOptionn struct {
	dataType string
	db       int
	key      string
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
	key, err := conf.GetString(KeyRedisKey)
	if err != nil {
		return
	}
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

		readChan: make(chan string),
		status:   StatusInit,
		mux:      sync.Mutex{},
		started:  false,
	}
	return
}

func (rr *RedisReader) Name() string {
	return fmt.Sprintf("[%s],[%v],[%s]", rr.opt.dataType, rr.opt.db, rr.opt.key)
}

func (rr *RedisReader) Source() string {
	return fmt.Sprintf("[%s],[%v],[%s]", rr.opt.dataType, rr.opt.db, rr.opt.key)
}

func (rr *RedisReader) ReadLine() (data string, err error) {
	if !rr.started {
		rr.Start()
	}
	timer := time.NewTicker(time.Second)
	select {
	case dat := <-rr.readChan:
		data = string(dat)
	case <-timer.C:
	}
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
		rr.channelIn = rr.client.Subscribe(rr.opt.key).Channel()
	case DataTypePatterChannel:
		rr.channelIn = rr.client.PSubscribe(rr.opt.key).Channel()
	case DataTypeList:
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
			ans, subErr := rr.client.BLPop(rr.opt.timeout, rr.opt.key).Result()
			if subErr != nil && subErr != redis.Nil {
				log.Errorf("Runner[%v] %v BLPop redis error %v", rr.meta.RunnerName, rr.Name(), subErr)
			} else if len(ans) > 1 {
				rr.readChan <- ans[1]
			} else if len(ans) == 1 {
				log.Errorf("Runner[%v] %v list read only one result in arrary %v", rr.meta.RunnerName, rr.Name(), ans)
			}
		default:
			err = fmt.Errorf("data Type < %v > not exist, exit", rr.opt.dataType)
			log.Error(err)
			return
		}
	}
}

func (rr *RedisReader) SetMode(mode string, v interface{}) error {
	return errors.New("RedisReader not support read mode")
}
