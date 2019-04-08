package httpfetch

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron"

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

var waitTime = time.Minute

const pageSize = "$(pageSize)"
const pageNo = "$(pageNo)"

func init() {
	reader.RegisterConstructor(ModeHTTPFETCH, NewReader)
}

type Reader struct {
	meta   *reader.Meta
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

	httpPageSize     int64
	httpPageNo       int64
	httpFetchMethod  string
	httpFetchAddress string
	httpFetchPath    string
	httpHeaders      map[string]string
	httpBody         string
	isLoop           bool
	loopDuration     time.Duration
	execOnStart      bool
	Cron             *cron.Cron //定时任务

	httpClient *http.Client
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	cronSchedule, _ := conf.GetStringOr(KeyHttpCron, "")
	httpFetchMethod, _ := conf.GetStringOr(KeyHttpMethod, "GET")
	httpFetchAddress, err := conf.GetString(KeyHTTPServiceAddress)
	execOnStart, _ := conf.GetBoolOr(KeyHttpExecOnStart, true)

	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(httpFetchAddress, "http://") && !strings.HasPrefix(httpFetchAddress, "https://") {
		httpFetchAddress = "http://" + httpFetchAddress
	}
	httpFetchPath, err := conf.GetString(KeyHTTPServicePath)
	if err != nil {
		return nil, err
	}

	body, _ := conf.GetStringOr(KeyHttpBody, "")
	var headers map[string]string
	headerStr, err := conf.GetStringOr(KeyHttpHeaders, "")
	if headerStr != "" {
		if err := json.Unmarshal([]byte(headerStr), &headers); err != nil {
			return nil, err
		}
	}

	dialTimeout, _ := conf.GetIntOr(KeyHttpDialTimeout, 30)
	if dialTimeout <= 0 {
		dialTimeout = 30
	}
	respTimeout, _ := conf.GetIntOr(KeyHttpResponseTimeout, 30)
	if respTimeout <= 0 {
		respTimeout = 30
	}
	var t = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   time.Duration(dialTimeout) * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: time.Duration(respTimeout) * time.Second,
		TLSClientConfig:       &tls.Config{},
	}

	var pageNo int64 = 1
	var pageSize int64 = 10
	pageSizeStr, pageNo, err := meta.ReadOffset()
	// err 不为空，表示第一次启动，从配置中读取pageSize 和 pageNum
	if err != nil {
		pageNo, _ = conf.GetInt64Or(KeyHttpPageNo, 1)
		pageSize, _ = conf.GetInt64Or(KeyHttpPageSize, 10)
		log.Errorf("Runner[%v] %v -meta data is corrupted err: %v, omit meta data...", meta.RunnerName, meta.MetaFile(), err)
	} else {
		pageSize, _ = strconv.ParseInt(pageSizeStr, 10, 64)
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageNo < 0 {
		pageNo = 1
	}
	r := &Reader{
		meta:             meta,
		status:           StatusInit,
		routineStatus:    StatusInit,
		stopChan:         make(chan struct{}),
		readChan:         make(chan string),
		errChan:          make(chan error),
		httpPageNo:       pageNo,
		httpPageSize:     pageSize,
		httpFetchMethod:  httpFetchMethod,
		httpFetchAddress: httpFetchAddress,
		httpFetchPath:    httpFetchPath,
		httpHeaders:      headers,
		httpBody:         body,
		execOnStart:      execOnStart,
		Cron:             cron.New(),

		httpClient: &http.Client{Transport: t},
	}

	// 定时任务配置串
	if len(cronSchedule) > 0 {
		cronSchedule = strings.ToLower(cronSchedule)
		if strings.HasPrefix(cronSchedule, Loop) {
			r.isLoop = true
			r.loopDuration, err = reader.ParseLoopDuration(cronSchedule)
			if err != nil {
				log.Warnf("Runner[%v] %v %v", r.meta.RunnerName, r.Name(), err)
			}
			if r.loopDuration.Nanoseconds() <= 0 {
				r.loopDuration = time.Second
			}
		} else {
			err = r.Cron.AddFunc(cronSchedule, r.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron job added with schedule <%v>", r.meta.RunnerName, r.Name(), cronSchedule)
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
	return r.httpFetchMethod + " " + r.httpFetchAddress + r.httpFetchPath
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("HttpFetch reader does not support read mode")
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
	return r.httpFetchAddress + r.httpFetchPath
}

func (r *Reader) url() string {
	url := r.httpFetchAddress + r.httpFetchPath
	url = strings.Replace(url, pageSize, strconv.FormatInt(r.httpPageSize, 10), -1)
	url = strings.Replace(url, pageNo, strconv.FormatInt(atomic.LoadInt64(&r.httpPageNo), 10), -1)
	return url
}

func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case data := <-r.readChan:
		atomic.AddInt64(&r.httpPageNo, 1)
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

func (r *Reader) SyncMeta() {
	if err := r.meta.WriteOffset(strconv.FormatInt(r.httpPageSize, 10), r.httpPageNo); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
	}
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	r.Cron.Stop()

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
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

		content, err := r.curl()
		if err == nil {
			log.Infof("Runner[%v] %q task has been successfully curl", r.meta.RunnerName, r.Name())
			r.readChan <- content
			return
		}

		log.Errorf("Runner[%v] %q task curl failed: %v ", r.meta.RunnerName, r.Name(), err)
		r.setStatsError(err.Error())
		r.sendError(err)

		if r.isLoop {
			return // 循环执行的任务上层逻辑已经等同重试
		}
		time.Sleep(3 * time.Second)
	}
	log.Errorf("Runner[%v] %q task execution failed and gave up after 10 tries", r.meta.RunnerName, r.Name())
}

func (r *Reader) curl() (string, error) {
	var body io.Reader
	if r.httpBody != "" {
		body = bytes.NewBufferString(r.httpBody)
	}
	req, err := http.NewRequest(r.httpFetchMethod, r.url(), body)
	if err != nil {
		return "", err
	}
	if r.httpHeaders != nil {
		for k, v := range r.httpHeaders {
			req.Header.Add(k, v)
		}
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", errors.New(string(content))
	}
	return string(content), nil
}
