package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"
	elasticV6 "github.com/olivere/elastic"
	"github.com/robfig/cron"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"
	elasticV7 "gopkg.in/olivere/elastic.v7"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/utils/magic"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.Reader       = &Reader{}
	_ Resetable           = &Reader{}
)

const (
	KeyMetaFileName = ".es.log"
)

type Record struct {
	data       json.RawMessage
	cronOffset interface{}
}

type Reader struct {
	meta *reader.Meta // 记录offset的元数据
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
	readChan chan Record
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	Cron            *cron.Cron //定时任务
	execOnStart     bool
	isLoop          bool
	loopDuration    time.Duration
	offsetKey       string
	offsetKeyType   string
	offsetValue     interface{}
	metaFile        string
	esindex         string //es索引
	lastESIndex     string //上一次es索引
	lastScrollId    string //上一次scrollId
	estype          string //es type
	eshost          string //eshost+port
	authUsername    string
	authPassword    string
	readBatch       int    // 每次读取的数据量
	keepAlive       string //scrollID 保留时间
	esVersion       string //ElasticSearch version
	offset          string // 当前处理es的offset
	delayTimeStr    string // 延迟时间
	delayTime       int64  // 延迟时间
	delayTimeUnit   string // 时间戳单位
	dateShift       bool
	dateShiftOffset int
	elasticV3Client *elasticV3.Client
	elasticV5Client *elasticV5.Client
	elasticV6Client *elasticV6.Client
	elasticV7Client *elasticV7.Client
}

func init() {
	reader.RegisterConstructor(ModeElastic, NewReader)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	readBatch, _ := conf.GetIntOr(KeyESReadBatch, 100)
	estype, _ := conf.GetStringOr(KeyESType, "")

	esindex, err := conf.GetString(KeyESIndex)
	if err != nil {
		return nil, err
	}
	eshost, _ := conf.GetStringOr(KeyESHost, "http://localhost:9200")
	if !strings.HasPrefix(eshost, "http://") && !strings.HasPrefix(eshost, "https://") {
		eshost = "http://" + eshost
	}

	dateshift, _ := conf.GetBoolOr(KeyESDateShift, false)
	dateshiftoffset, _ := conf.GetIntOr(KeyESDateOffset, 0)

	esVersion, _ := conf.GetStringOr(KeyESVersion, ElasticVersion5)
	authUsername, _ := conf.GetStringOr(KeyAuthUsername, "")
	authPassword, _ := conf.GetPasswordEnvStringOr(KeyAuthPassword, "")
	keepAlive, _ := conf.GetStringOr(KeyESKeepAlive, "6h")
	cronSched, _ := conf.GetStringOr(KeyESCron, "")
	execOnStart, _ := conf.GetBoolOr(KeyESExecOnstart, true)
	sniff, _ := conf.GetBoolOr(KeyESSniff, false)
	offsetKey, _ := conf.GetStringOr(KeyESOffsetKey, "")
	offsetKeyType, _ := conf.GetStringOr(KeyESOffsetKeyType, "")
	startTime, _ := conf.GetStringOr(KeyESOffsetStartTime, "")
	delayTimeStr, _ := conf.GetStringOr(KeyESDelayTime, "now+1y")
	if delayTimeStr == "" {
		delayTimeStr = "now+1y"
	}
	delayTime, _ := conf.GetInt64Or(KeyESDelayTime, 0)
	delayTimeUnit, _ := conf.GetStringOr(KeyESDelayTimeUnit, "second")
	metaFile := filepath.Join(meta.Dir, KeyMetaFileName)

	offset, _, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
	}
	if offsetKey == "" {
		offset = ""
	}

	// 初始化 client
	var elasticV3Client *elasticV3.Client
	var elasticV5Client *elasticV5.Client
	var elasticV6Client *elasticV6.Client
	var elasticV7Client *elasticV7.Client
	switch esVersion {
	case ElasticVersion7:
		optFns := []elasticV7.ClientOptionFunc{
			elasticV7.SetHealthcheck(false),
			elasticV7.SetURL(eshost),
			elasticV7.SetSniff(sniff),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV7.SetBasicAuth(authUsername, authPassword))
		}

		elasticV7Client, err = elasticV7.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	case ElasticVersion6:
		optFns := []elasticV6.ClientOptionFunc{
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(eshost),
			elasticV6.SetSniff(sniff),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV6.SetBasicAuth(authUsername, authPassword))
		}

		elasticV6Client, err = elasticV6.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	case ElasticVersion3:
		optFns := []elasticV3.ClientOptionFunc{
			elasticV3.SetSniff(sniff),
			elasticV3.SetHealthcheck(false),
			elasticV3.SetURL(eshost),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV3.SetBasicAuth(authUsername, authPassword))
		}

		elasticV3Client, err = elasticV3.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	default:
		optFns := []elasticV5.ClientOptionFunc{
			elasticV5.SetSniff(false),
			elasticV5.SetHealthcheck(false),
			elasticV5.SetURL(eshost),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV5.SetBasicAuth(authUsername, authPassword))
		}

		elasticV5Client, err = elasticV5.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	}

	r := &Reader{
		meta:            meta,
		status:          StatusInit,
		routineStatus:   StatusInit,
		stopChan:        make(chan struct{}),
		readChan:        make(chan Record),
		errChan:         make(chan error),
		esindex:         esindex,
		estype:          estype,
		eshost:          eshost,
		authUsername:    authUsername,
		authPassword:    authPassword,
		esVersion:       esVersion,
		readBatch:       readBatch,
		keepAlive:       keepAlive,
		offset:          offset,
		dateShift:       dateshift,
		dateShiftOffset: dateshiftoffset,
		Cron:            cron.New(),
		offsetKey:       offsetKey,
		offsetKeyType:   offsetKeyType,
		delayTime:       delayTime,
		delayTimeStr:    delayTimeStr,
		delayTimeUnit:   delayTimeUnit,
		execOnStart:     execOnStart,
		metaFile:        metaFile,
		elasticV3Client: elasticV3Client,
		elasticV5Client: elasticV5Client,
		elasticV6Client: elasticV6Client,
		elasticV7Client: elasticV7Client,
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
				r.loopDuration = time.Second
			}
			if utils.IsExist(metaFile) {
				content, err := ioutil.ReadFile(metaFile)
				if err != nil {
					log.Warnf("Runner[%v] %v failed to read offset file[%v]: %v,reset offset and read all data", meta.RunnerName, ModeElastic, metaFile, err)
				}
				r.offsetValue = string(content)
			}
		} else {
			err = r.Cron.AddFunc(cronSched, r.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron added with schedule <%v>", r.meta.RunnerName, r.Name(), cronSched)
		}
	}
	if startTime != "" && r.offsetValue == nil {
		r.offsetValue = startTime
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
	return "ESReader_" + r.Source()
}

func (r *Reader) run() {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %s daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			errMsg := fmt.Sprintf("Runner[%v] %s daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
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

	// 判断上层是否已经关闭，先判断 routineStatus 再判断 status 可以保证同时只有一个 r.run 会运行到此处
	if r.isStopping() || r.hasStopped() {
		log.Warnf("Runner[%v] %s daemon has stopped, task is interrupted", r.meta.RunnerName, r.Name())
		return
	}
	var err error
	err = r.execQuery()
	if err == nil {
		log.Infof("Runner[%v] %s task has been successfully executed", r.meta.RunnerName, r.Name())
		return
	}
	r.setStatsError(err.Error())
	r.sendError(err)

	log.Errorf("Runner[%v] %s task execution failed: %v ", r.meta.RunnerName, r.Name(), err)
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
			log.Errorf("Reader %s was panicked and recovered from %v\nstack: %s", r.Name(), rec, debug.Stack())
		}
	}()
	r.errChan <- err
}

func (r *Reader) getIndexShift() string {
	return magic.GoMagic(r.esindex, time.Now().Add(-1*time.Duration(r.dateShiftOffset)*time.Hour))
}

// 定时读取，支持增量读取，需要指定具有自增属性的offset字段
func (r *Reader) execQuery() error {
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Runner[%v] recover when exec with cron\npanic: %v\nstack: %s", r.meta.RunnerName, rec, debug.Stack())
		}
	}()
	var index = r.esindex
	if r.dateShift {
		index = r.getIndexShift()
	}
	if r.lastESIndex != index {
		r.offset = ""
		r.lastESIndex = index
	}

	var endTime interface{}
	switch r.offsetKeyType {
	case "long":
		endTime = int64(^uint64(0) >> 1)
		if r.delayTime != 0 {
			if r.delayTimeUnit == "second" {
				endTime = time.Now().Unix() - r.delayTime
			} else if r.delayTimeUnit == "millisecond" {
				endTime = time.Now().Unix()*1000 - r.delayTime
			}
		}
	case "date":
		endTime = r.delayTimeStr
	default:
	}

	switch r.esVersion {
	case ElasticVersion7:
		var scroll *elasticV7.ScrollService
		if r.offsetKey != "" {
			rangeQuery := elasticV7.NewRangeQuery(r.offsetKey).Gt(r.offsetValue).Lte(endTime)
			scroll = r.elasticV7Client.Scroll(index).Query(rangeQuery).Sort(r.offsetKey, true).Size(r.readBatch).KeepAlive(r.keepAlive)
		} else {
			scroll = r.elasticV7Client.Scroll(index).Size(r.readBatch).KeepAlive(r.keepAlive)
		}
		if r.estype != "" {
			scroll = scroll.Type(r.estype)
		}
		for {
			results, err := scroll.ScrollId(r.offset).Do(context.Background())
			if err == io.EOF {
				scrollIds := make([]string, 0)
				if results.ScrollId != r.offset {
					scrollIds = append(scrollIds, results.ScrollId)
				}
				if r.offsetKey != "" && r.offset != "" {
					scrollIds = append(scrollIds, r.offset)
					r.offset = ""
				}
				_, err = r.elasticV7Client.ClearScroll(scrollIds...).Do(context.Background())
				return err
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				m := make(map[string]interface{})
				jsoniter.Unmarshal(hit.Source, &m)
				r.readChan <- Record{
					data:       hit.Source,
					cronOffset: m[r.offsetKey],
				}
			}
			r.offset = results.ScrollId
			if r.isStopping() || r.hasStopped() {
				return nil
			}
		}
	case ElasticVersion6:
		var scroll *elasticV6.ScrollService
		if r.offsetKey != "" {
			rangeQuery := elasticV6.NewRangeQuery(r.offsetKey).Gte(r.offsetValue).Lte(endTime)
			scroll = r.elasticV6Client.Scroll(index).Query(rangeQuery).Sort(r.offsetKey, true).Size(r.readBatch).KeepAlive(r.keepAlive)
		} else {
			scroll = r.elasticV6Client.Scroll(index).Size(r.readBatch).KeepAlive(r.keepAlive)
		}
		if r.estype != "" {
			scroll = scroll.Type(r.estype)
		}
		for {
			results, err := scroll.ScrollId(r.offset).Do(context.Background())
			if err == io.EOF {
				scrollIds := make([]string, 0)
				if results.ScrollId != r.offset {
					scrollIds = append(scrollIds, results.ScrollId)
				}
				if r.offsetKey != "" && r.offset != "" {
					scrollIds = append(scrollIds, r.offset)
					r.offset = ""
				}
				_, err = r.elasticV6Client.ClearScroll(scrollIds...).Do(context.Background())
				return err
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				m := make(map[string]interface{})
				jsoniter.Unmarshal(*hit.Source, &m)
				r.readChan <- Record{
					data:       *hit.Source,
					cronOffset: m[r.offsetKey],
				}
			}
			r.offset = results.ScrollId
			if r.isStopping() || r.hasStopped() {
				return nil
			}
		}
	case ElasticVersion3:
		var scroll *elasticV3.ScrollService
		if r.offsetKey != "" {
			rangeQuery := elasticV3.NewRangeQuery(r.offsetKey).Gte(r.offsetValue).Lte(endTime)
			scroll = r.elasticV3Client.Scroll(index).Query(rangeQuery).Sort(r.offsetKey, true).Size(r.readBatch).KeepAlive(r.keepAlive)
		} else {
			scroll = r.elasticV3Client.Scroll(index).Size(r.readBatch).KeepAlive(r.keepAlive)
		}
		if r.estype != "" {
			scroll = scroll.Type(r.estype)
		}
		for {
			if r.offset != "" {
				scroll = scroll.ScrollId(r.offset)
			}
			results, err := scroll.Do()
			if err == io.EOF {
				scrollIds := make([]string, 0)
				if results.ScrollId != r.offset {
					scrollIds = append(scrollIds, results.ScrollId)
				}
				if r.offsetKey != "" && r.offset != "" {
					scrollIds = append(scrollIds, r.offset)
					r.offset = ""
				}
				_, err = r.elasticV3Client.ClearScroll(scrollIds...).DoC(context.Background())
				return err
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				m := make(map[string]interface{})
				jsoniter.Unmarshal(*hit.Source, &m)
				r.readChan <- Record{
					data:       *hit.Source,
					cronOffset: m[r.offsetKey],
				}
			}
			r.offset = results.ScrollId
			if r.isStopping() || r.hasStopped() {
				return nil
			}
		}
	default:
		var scroll *elasticV5.ScrollService
		if r.offsetKey != "" {
			rangeQuery := elasticV5.NewRangeQuery(r.offsetKey).Gte(r.offsetValue).Lte(endTime)
			scroll = r.elasticV5Client.Scroll(index).Query(rangeQuery).Sort(r.offsetKey, true).Size(r.readBatch).KeepAlive(r.keepAlive)
		} else {
			scroll = r.elasticV5Client.Scroll(index).Size(r.readBatch).KeepAlive(r.keepAlive)
		}
		if r.estype != "" {
			scroll = scroll.Type(r.estype)
		}
		for {
			results, err := scroll.ScrollId(r.offset).Do(context.Background())
			if err == io.EOF {
				scrollIds := make([]string, 0)
				if results.ScrollId != r.offset {
					scrollIds = append(scrollIds, results.ScrollId)
				}
				if r.offsetKey != "" && r.offset != "" {
					scrollIds = append(scrollIds, r.offset)
					r.offset = ""
				}
				_, err = r.elasticV5Client.ClearScroll(scrollIds...).Do(context.Background())
				return err
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				m := make(map[string]interface{})
				jsoniter.Unmarshal(*hit.Source, &m)
				r.readChan <- Record{
					data:       *hit.Source,
					cronOffset: m[r.offsetKey],
				}
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
	} else if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
		log.Warnf("Runner[%v] %s daemon has already started and is running", r.meta.RunnerName, r.Name())
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
					log.Infof("Runner[%v] %s daemon has stopped from running", r.meta.RunnerName, r.Name())
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
	log.Infof("Runner[%v] %s daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	var index = r.esindex
	if r.dateShift {
		index = r.getIndexShift()
	}
	source := r.eshost + "_" + index
	if r.estype != "" {
		source = source + "_" + r.estype
	}
	return source
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case rec := <-r.readChan:
		longValue, ok := rec.cronOffset.(float64)
		if ok {
			r.offsetValue = int64(longValue)
		} else {
			r.offsetValue = rec.cronOffset
		}
		return string(rec.data), nil
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

func (r *Reader) Reset() error {
	if err := os.RemoveAll(r.metaFile); err != nil {
		return err
	}
	return nil
}

// SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复
func (r *Reader) SyncMeta() {
	if err := r.meta.WriteOffset(r.offset, 0); err != nil {
		log.Errorf("Runner[%v] reader %s sync meta failed: %v", r.meta.RunnerName, r.Name(), err)
	}
	err := ioutil.WriteFile(r.metaFile, []byte(fmt.Sprintf("%v", r.offsetValue)), 0644)
	if err != nil {
		log.Errorf("Runner[%v] %v failed to sync meta: %v", r.meta.RunnerName, r.Name(), err)
	}
	return
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %s is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %s daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	if r.elasticV3Client != nil {
		r.elasticV3Client.Stop()
	}
	if r.elasticV5Client != nil {
		r.elasticV5Client.Stop()
	}
	if r.elasticV6Client != nil {
		r.elasticV6Client.Stop()
	}
	if r.elasticV7Client != nil {
		r.elasticV7Client.Stop()
	}

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
}
