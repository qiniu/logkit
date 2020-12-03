package redis

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.Reader       = &Reader{}
)

func init() {
	reader.RegisterConstructor(ModeRedis, NewReader)
}

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
	readChan chan string
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	opts      Options
	client    *redis.Client
	channelIn <-chan *redis.Message
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

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	dataType, err := conf.GetString(KeyRedisDataType)
	if err != nil {
		return nil, err
	}
	db, _ := conf.GetIntOr(KeyRedisDB, 0)
	key, _ := conf.GetStringList(KeyRedisKey)
	if err != nil {
		return nil, err
	}
	area, _ := conf.GetStringOr(KeyRedisHashArea, "")
	address, _ := conf.GetStringOr(KeyRedisAddress, "127.0.0.1:6379")
	password, _ := conf.GetStringOr(KeyRedisPassword, "")
	keyTimeoutDuration, _ := conf.GetStringOr(KeyTimeoutDuration, "5s")
	timeout, err := time.ParseDuration(keyTimeoutDuration)
	if err != nil {
		return nil, err
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

	for _, val := range opt.key {
		keyType, _ := client.Type(val).Result()
		if dataType != keyType {
			return nil, fmt.Errorf("key[%v]'s type expect as %v,actual get %v", val, dataType, keyType)
		}
	}

	return &Reader{
		meta:          meta,
		status:        StatusInit,
		routineStatus: StatusInit,
		stopChan:      make(chan struct{}),
		readChan:      make(chan string),
		errChan:       make(chan error),
		opts:          opt,
		client:        client,
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *Reader) Name() string {
	return fmt.Sprintf("[%s],[%v],[%s]", r.opts.dataType, r.opts.db, r.opts.key)
}

func (*Reader) SetMode(_ string, _ interface{}) error {
	return errors.New("redis reader does not support read mode")
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
			log.Errorf("Reader %q was panicked and recovered from %v\nstack: %s", r.Name(), rec, debug.Stack())
		}
	}()
	r.errChan <- err
}

func (r *Reader) run() {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			log.Errorf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
		}
		return
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			if atomic.CompareAndSwapInt32(&r.routineStatus, StatusRunning, StatusStopping) {
				close(r.readChan)
				close(r.errChan)
				r.client.Close()
			}
			return
		}
		atomic.StoreInt32(&r.routineStatus, StatusInit)
	}()

	switch r.opts.dataType {
	case DataTypeChannel, DataTypePatterChannel:
		message := <-r.channelIn
		if message != nil {
			r.readChan <- message.Payload
		}
	case DataTypeList:
		for _, key := range r.opts.key {
			ans, subErr := r.client.BLPop(r.opts.timeout, key).Result()
			if subErr != nil && subErr != redis.Nil {
				err := fmt.Errorf("runner[%v] %v BLPop redis error %v", r.meta.RunnerName, r.Name(), subErr)
				log.Error(err)
				r.setStatsError(err.Error())
				r.sendError(err)
			} else if len(ans) > 1 {
				r.readChan <- ans[1]
			} else if len(ans) == 1 {
				err := fmt.Errorf("runner[%v] %v list read only one result in array %v", r.meta.RunnerName, r.Name(), ans)
				log.Error(err)
				r.sendError(err)
				r.setStatsError(err.Error())
			}
		}
		//Added string support for redis
	case DataTypeString:
		for _, key := range r.opts.key {
			anString, subErr := r.client.Get(key).Result()
			if subErr != nil && subErr != redis.Nil {
				err := fmt.Errorf("runner[%v] %v Get redis error %v", r.meta.RunnerName, r.Name(), subErr)
				log.Error(err)
				r.sendError(err)
				r.setStatsError(err.Error())
			} else if anString != "" {
				//Avoid data duplication
				r.client.Del(key)
				r.readChan <- anString
			}
		}
		//Added set support for redis
	case DataTypeSet:
		for _, key := range r.opts.key {
			anSet, subErr := r.client.SPop(key).Result()
			if subErr != nil && subErr != redis.Nil {
				err := fmt.Errorf("runner[%v] %v SPop redis error %v", r.meta.RunnerName, r.Name(), subErr)
				r.setStatsError(err.Error())
				r.sendError(err)
			} else if anSet != "" {
				r.readChan <- anSet
			}
		}
		//Added sortedSet support for redis
	case DataTypeSortedSet:
		for _, key := range r.opts.key {
			anSortedSet, subErr := r.client.ZRange(key, 0, -1).Result()
			if subErr != nil && subErr != redis.Nil {
				err := fmt.Errorf("runner[%v] %v ZRange redis error %v", r.meta.RunnerName, r.Name(), subErr)
				r.setStatsError(err.Error())
				r.sendError(err)
			} else if len(anSortedSet) > 0 {
				r.client.Del(key)
				for _, val := range anSortedSet {
					r.readChan <- val
				}
			}
		}
		//Added hash support for redis
	case DataTypeHash:
		for _, key := range r.opts.key {
			anHash, subErr := r.client.HGet(key, r.opts.area).Result() //redis key and area for hash
			if subErr != nil && subErr != redis.Nil {
				err := fmt.Errorf("runner[%v] %v HGetAll redis error %v", r.meta.RunnerName, r.Name(), subErr)
				r.setStatsError(err.Error())
				r.sendError(err)
			} else if anHash != "" {
				r.client.Del(key)
				r.readChan <- anHash
			}
		}
	default:
		err := fmt.Errorf("data Type < %v > not exist, exit", r.opts.dataType)
		log.Error(err)
		r.setStatsError(err.Error())
		r.sendError(err)
		return
	}
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	switch r.opts.dataType {
	case DataTypeChannel:
		r.channelIn = r.client.Subscribe(r.opts.key...).Channel()
	case DataTypePatterChannel:
		r.channelIn = r.client.PSubscribe(r.opts.key...).Channel()
	case DataTypeList:
	case DataTypeString:
	case DataTypeSet:
	case DataTypeSortedSet:
	case DataTypeHash:
	default:
		err := fmt.Errorf("data Type < %v > not exist, exit", r.opts.dataType)
		log.Error(err)
		return err
	}

	go func() {
		for {
			r.run()

			select {
			case <-r.stopChan:
				atomic.StoreInt32(&r.status, StatusStopped)
				log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
				return
			default:
			}
		}
	}()
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return fmt.Sprintf("[%s],[%v],[%s]", r.opts.dataType, r.opts.db, r.opts.key)
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case data := <-r.readChan:
		return data, nil
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

func (*Reader) SyncMeta() {}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusStopping) {
		close(r.readChan)
		close(r.errChan)
		r.client.Close()
	}
	return nil
}
