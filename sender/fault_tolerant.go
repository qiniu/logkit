package sender

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/queue"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/reqid"
)

const (
	mb                = 1024 * 1024 // 1MB
	defaultWriteLimit = 10          // 默认写速限制为10MB
	maxBytesPerFile   = 100 * mb
	qNameSuffix       = "_local_save"
	directSuffix      = "_direct"
	defaultMaxProcs   = 1         // 默认没有并发
	DefaultSplitSize  = 64 * 1024 // 默认分割为 64 kb
)

var _ SkipDeepCopySender = &FtSender{}

// FtSender fault tolerance sender wrapper
type FtSender struct {
	stopped     int32
	exitChan    chan struct{}
	innerSender Sender
	logQueue    queue.BackendQueue
	BackupQueue queue.BackendQueue
	writeLimit  int // 写入速度限制，单位MB
	strategy    string
	procs       int //发送并发数
	runnerName  string
	opt         *FtOption
	stats       StatsInfo
	statsMutex  *sync.RWMutex
	jsontool    jsoniter.API
}

type FtOption struct {
	saveLogPath       string
	syncEvery         int64
	writeLimit        int
	strategy          string
	procs             int
	memoryChannel     bool
	memoryChannelSize int
	longDataDiscard   bool
}

type datasContext struct {
	Datas []Data `json:"datas"`
}

// NewFtSender Fault tolerant sender constructor
func NewFtSender(ftSender Sender, conf conf.MapConf, ftSaveLogPath string) (*FtSender, error) {
	memoryChannel, _ := conf.GetBoolOr(KeyFtMemoryChannel, false)
	memoryChannelSize, _ := conf.GetIntOr(KeyFtMemoryChannelSize, 100)
	logPath, _ := conf.GetStringOr(KeyFtSaveLogPath, ftSaveLogPath)
	syncEvery, _ := conf.GetIntOr(KeyFtSyncEvery, DefaultFtSyncEvery)
	writeLimit, _ := conf.GetIntOr(KeyFtWriteLimit, defaultWriteLimit)
	strategy, _ := conf.GetStringOr(KeyFtStrategy, KeyFtStrategyBackupOnly)
	longDataDiscard, _ := conf.GetBoolOr(KeyFtLongDataDiscard, false)
	switch strategy {
	case KeyFtStrategyAlwaysSave, KeyFtStrategyBackupOnly, KeyFtStrategyConcurrent:
	default:
		return nil, errors.New("no match ft_strategy")
	}
	procs, _ := conf.GetIntOr(KeyFtProcs, defaultMaxProcs)
	runnerName, _ := conf.GetStringOr(KeyRunnerName, UnderfinedRunnerName)

	opt := &FtOption{
		saveLogPath:       logPath,
		syncEvery:         int64(syncEvery),
		writeLimit:        writeLimit,
		strategy:          strategy,
		procs:             procs,
		memoryChannel:     memoryChannel,
		memoryChannelSize: memoryChannelSize,
		longDataDiscard:   longDataDiscard,
	}

	return newFtSender(ftSender, runnerName, opt)
}

func newFtSender(innerSender Sender, runnerName string, opt *FtOption) (*FtSender, error) {
	var lq, bq queue.BackendQueue
	err := CreateDirIfNotExist(opt.saveLogPath)
	if err != nil {
		return nil, err
	}
	if opt.strategy == KeyFtStrategyConcurrent {
		lq = queue.NewDirectQueue("stream" + directSuffix)
	} else if !opt.memoryChannel {
		lq = queue.NewDiskQueue("stream"+qNameSuffix, opt.saveLogPath, maxBytesPerFile, 0, maxBytesPerFile, opt.syncEvery, opt.syncEvery, time.Second*2, opt.writeLimit*mb, false, 0)
	} else {
		lq = queue.NewDiskQueue("stream"+qNameSuffix, opt.saveLogPath, maxBytesPerFile, 0, maxBytesPerFile, opt.syncEvery, opt.syncEvery, time.Second*2, opt.writeLimit*mb, true, opt.memoryChannelSize)
	}
	bq = queue.NewDiskQueue("backup"+qNameSuffix, opt.saveLogPath, maxBytesPerFile, 0, maxBytesPerFile, opt.syncEvery, opt.syncEvery, time.Second*2, opt.writeLimit*mb, false, 0)
	ftSender := FtSender{
		exitChan:    make(chan struct{}),
		innerSender: innerSender,
		logQueue:    lq,
		BackupQueue: bq,
		writeLimit:  opt.writeLimit,
		strategy:    opt.strategy,
		procs:       opt.procs,
		runnerName:  runnerName,
		opt:         opt,
		statsMutex:  new(sync.RWMutex),
		jsontool:    jsoniter.Config{EscapeHTML: true, UseNumber: true}.Froze(),
	}
	go ftSender.asyncSendLogFromDiskQueue()
	return &ftSender, nil
}

func (ft *FtSender) Name() string {
	return ft.innerSender.Name()
}

func (ft *FtSender) Send(datas []Data) error {
	se := &StatsError{Ft: true}
	if ft.strategy == KeyFtStrategyBackupOnly {
		// 尝试直接发送数据，当数据失败的时候会加入到本地重试队列。外部不需要重试
		isRetry := false
		backDataContext, err := ft.trySendDatas(datas, 1, isRetry)
		if err != nil {
			err = fmt.Errorf("Runner[%v] Sender[%v] try Send Datas err: %v, will put to backup queue and retry later...", ft.runnerName, ft.innerSender.Name(), err)
			log.Error(err)
			se.FtNotRetry = true
		}
		// 容错队列会保证重试，此处不向外部暴露发送错误信息
		se.ErrorDetail = err
		se.FtQueueLag = ft.BackupQueue.Depth()
		if backDataContext != nil {
			var nowDatas []Data
			for _, v := range backDataContext {
				nowDatas = append(nowDatas, v.Datas...)
			}
			if nowDatas != nil {
				se.FtNotRetry = false
				se.ErrorDetail = reqerr.NewSendError("save data to backend queue error", ConvertDatasBack(nowDatas), reqerr.TypeDefault)
				ft.statsMutex.Lock()
				ft.stats.LastError = se.ErrorDetail.Error()
				ft.statsMutex.Unlock()
			}
		}
	} else {
		err := ft.saveToFile(datas)
		if err != nil {
			se.FtNotRetry = false
			se.ErrorDetail = err
			ft.statsMutex.Lock()
			ft.stats.LastError = err.Error()
			ft.stats.Errors += int64(len(datas))
			ft.statsMutex.Unlock()
		} else {
			se.ErrorDetail = nil
		}
		se.FtQueueLag = ft.BackupQueue.Depth() + ft.logQueue.Depth()
	}
	return se
}

func (ft *FtSender) Stats() StatsInfo {
	ft.statsMutex.RLock()
	defer ft.statsMutex.RUnlock()
	return ft.stats
}

func (ft *FtSender) Restore(info *StatsInfo) {
	ft.statsMutex.Lock()
	defer ft.statsMutex.Unlock()
	ft.stats = *info
}

func (ft *FtSender) Reset() error {
	if ft.opt == nil {
		log.Errorf("Runner[%v] ft %v option is nill", ft.runnerName, ft.Name())
		return nil
	}
	return os.RemoveAll(ft.opt.saveLogPath)
}

func (ft *FtSender) Close() error {
	atomic.AddInt32(&ft.stopped, 1)
	log.Warnf("Runner[%v] wait for Sender[%v] to completely exit", ft.runnerName, ft.Name())
	// 等待错误恢复流程退出
	<-ft.exitChan
	// 等待正常发送流程退出
	for i := 0; i < ft.procs; i++ {
		<-ft.exitChan
	}

	log.Warnf("Runner[%v] Sender[%v] has been completely exited", ft.runnerName, ft.Name())

	// persist queue's meta data
	ft.logQueue.Close()
	ft.BackupQueue.Close()

	return ft.innerSender.Close()
}

func (ft *FtSender) TokenRefresh(mapConf conf.MapConf) (err error) {
	if tokenSender, ok := ft.innerSender.(TokenRefreshable); ok {
		err = tokenSender.TokenRefresh(mapConf)
	}
	return
}

// marshalData 将数据序列化
func (ft *FtSender) marshalData(datas []Data) ([]byte, error) {
	bs, err := jsoniter.Marshal(&datasContext{
		Datas: datas,
	})
	if err != nil {
		return nil, reqerr.NewSendError("Cannot marshal data :"+err.Error(), ConvertDatasBack(datas), reqerr.TypeDefault)
	}
	return bs, nil
}

// unmarshalData 如何将数据从磁盘中反序列化出来
func (ft *FtSender) unmarshalData(dat []byte) (datas []Data, err error) {
	ctx := new(datasContext)
	err = ft.jsontool.Unmarshal(dat, &ctx)
	if err != nil {
		return
	}
	datas = ctx.Datas
	return
}

func (ft *FtSender) saveToFile(datas []Data) error {
	if dqueue, ok := ft.logQueue.(queue.DataQueue); ok {
		return dqueue.PutDatas(datas)
	}

	bs, err := ft.marshalData(datas)
	if err != nil {
		return err
	}

	err = ft.logQueue.Put(bs)
	if err != nil {
		return reqerr.NewSendError(ft.innerSender.Name()+" Cannot put data into backendQueue: "+err.Error(), ConvertDatasBack(datas), reqerr.TypeDefault)
	}
	return nil
}

func (ft *FtSender) asyncSendLogFromDiskQueue() {
	for i := 0; i < ft.procs; i++ {
		readDatasChan := make(<-chan []Data)
		if dqueue, ok := ft.logQueue.(queue.DataQueue); ok {
			readDatasChan = dqueue.ReadDatasChan()
		}
		go ft.sendFromQueue(ft.logQueue.Name(), ft.logQueue.ReadChan(), readDatasChan, false)
	}

	readDatasChan := make(<-chan []Data)
	go ft.sendFromQueue(ft.BackupQueue.Name(), ft.BackupQueue.ReadChan(), readDatasChan, true)
}

// trySend 从bytes反序列化数据后尝试发送数据
func (ft *FtSender) trySendBytes(dat []byte, failSleep int, isRetry bool) (backDataContext []*datasContext, err error) {
	datas, err := ft.unmarshalData(dat)
	if err != nil {
		return
	}
	return ft.trySendDatas(datas, failSleep, isRetry)
}

// trySendDatas 尝试发送数据，如果失败，将失败数据加入backup queue，并睡眠指定时间。返回结果为是否正常发送
func (ft *FtSender) trySendDatas(datas []Data, failSleep int, isRetry bool) (backDataContext []*datasContext, err error) {
	err = ft.innerSender.Send(datas)
	ft.statsMutex.Lock()
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
		if isRetry {
			ft.stats.Errors -= c.Success
		} else {
			ft.stats.Errors += c.Errors
		}
		ft.stats.Success += c.Success
	} else if err != nil {
		if !isRetry {
			ft.stats.Errors += int64(len(datas))
		}
	} else {
		ft.stats.Success += int64(len(datas))
		if isRetry {
			ft.stats.Errors -= int64(len(datas))
		}
	}
	if err != nil {
		se, succ := err.(*reqerr.SendError)
		if !succ {
			ft.stats.LastError = err.Error()
		} else {
			ft.stats.LastError = se.Error()
		}
	} else {
		ft.stats.LastError = ""
	}
	ft.statsMutex.Unlock()
	if err != nil {
		retDatasContext := ft.handleSendError(err, datas)
		for _, v := range retDatasContext {
			nnBytes, _ := jsoniter.Marshal(v)
			err := ft.BackupQueue.Put(nnBytes)
			if err != nil {
				log.Errorf("Runner[%v] Sender[%v] cannot write points back to queue %v: %v", ft.runnerName, ft.innerSender.Name(), ft.BackupQueue.Name(), err)
				backDataContext = append(backDataContext, v)
			}
		}
		time.Sleep(time.Second * time.Duration(failSleep))
	}
	return
}

func (ft *FtSender) handleSendError(err error, datas []Data) (retDatasContext []*datasContext) {
	failCtx := new(datasContext)
	var binaryUnpack bool
	var errMessage string
	se, succ := err.(*reqerr.SendError)
	if !succ {
		// 如果不是SendError 默认所有的数据都发送失败
		errMessage = "error type is not *SendError! reSend all datas by default"
		failCtx.Datas = datas
	} else {
		failCtx.Datas = ConvertDatas(se.GetFailDatas())
		if se.ErrorType == reqerr.TypeBinaryUnpack {
			binaryUnpack = true
			errMessage = "error type is binaryUnpack, will divid to 2 parts and retry"
		} else if se.ErrorType == reqerr.TypeSchemaFreeRetry {
			errMessage = "maybe this is because of server schema cache, will send all data again"
		} else {
			errMessage = "error type is default, will send all data again"
		}
	}
	log.Errorf("Runner[%v] Sender[%v] cannot write points: %v, failDatas size: %v, %s", ft.runnerName, ft.innerSender.Name(), err, len(failCtx.Datas), errMessage)
	log.Debugf("Runner[%v] Sender[%v] failed datas [[%v]]", ft.runnerName, ft.innerSender.Name(), failCtx.Datas)
	if binaryUnpack {
		lens := len(failCtx.Datas) / 2
		if lens > 0 {
			newFailCtx := new(datasContext)
			newFailCtx.Datas = failCtx.Datas[0:lens]
			failCtx.Datas = failCtx.Datas[lens:]
			retDatasContext = append(retDatasContext, newFailCtx)
			retDatasContext = append(retDatasContext, failCtx)
			return
		}
		if len(failCtx.Datas) == 1 {
			failCtxData := failCtx.Datas[0]
			// 小于 2M 时，放入 pandora_stash中
			dataBytes, err := jsoniter.Marshal(failCtxData)
			if err != nil {
				retDatasContext = append(retDatasContext, failCtx)
				return
			}
			if int64(len(string(dataBytes))) < DefaultMaxBatchSize {
				// 此处将 data 改为 raw 放在 pandora_stash 中
				if _, ok := failCtxData[KeyPandoraStash]; !ok {
					log.Infof("Runner[%v] Sender[%v] try to convert data to string", ft.runnerName, ft.innerSender.Name())
					byteData, err := json.Marshal(failCtx.Datas[0])
					if err != nil {
						log.Warnf("Runner[%v] marshal data to string error %v", ft.runnerName, err)
					} else {
						data := make([]Data, 1)
						data[0] = Data{
							KeyPandoraStash: string(byteData),
						}
						failCtx.Datas = data
					}
				}
				retDatasContext = append(retDatasContext, failCtx)
				return
			}

			if ft.opt.longDataDiscard {
				log.Infof("Runner[%v] Sender[%v] discard long data (more than 2M), length: ", ft.runnerName, ft.innerSender.Name(), len(string(dataBytes)))
				return
			}

			// 大于 2M 时，切片发送
			remainData := make(Data, 0)
			separateData := make(Data, 0)
			for failCtxDataKey, failCtxDataVal := range failCtxData {
				byteVal, err := json.Marshal(failCtxDataVal)
				if err != nil {
					log.Warnf("Runner[%v] marshal data to string error %v", ft.runnerName, err)
				}

				if len(byteVal) < DefaultMaxBatchSize {
					remainData[failCtxDataKey] = failCtxDataVal
					continue
				}

				_, ok := failCtxDataVal.(string)
				if ok {
					separateData[failCtxDataKey] = failCtxDataVal
				}
			}

			// failCtxData 的 key value 中找到 string 类型的 value 大于 2M，进行切片
			if len(separateData) != 0 {
				newFailCtx := new(datasContext)
				for separateDataKey, separateDataVal := range separateData {
					strVal, _ := separateDataVal.(string)
					valArray := SplitData(strVal, int64(DefaultSplitSize))

					separateId := reqid.Gen()
					for idx, val := range valArray {
						retData := make(Data, 0)
						for remainDataKey, remainDataVal := range remainData {
							retData[remainDataKey] = remainDataVal
						}
						retData[KeyPandoraSeparateId] = separateId + "_" + separateDataKey + "_" + strconv.Itoa(idx)
						retData[separateDataKey] = val
						newFailCtx.Datas = append(newFailCtx.Datas, retData)
					}
				}
				retDatasContext = append(retDatasContext, newFailCtx)
				return
			}

			// failCtxData 的 key value 中未找到 string 类型且大于 2M 的 value
			// 此时将 failCtxData 进行 Marshal 之后进行切片，放入pandaora_stash中
			valArray := SplitData(string(dataBytes), int64(DefaultSplitSize))
			newFailCtx := new(datasContext)
			separateId := reqid.Gen()
			for idx, val := range valArray {
				data := Data{
					KeyPandoraStash:      val,
					KeyPandoraSeparateId: separateId + "_" + strconv.Itoa(idx),
				}
				newFailCtx.Datas = append(newFailCtx.Datas, data)
			}
			retDatasContext = append(retDatasContext, newFailCtx)
			return
		}

		return
	}

	retDatasContext = append(retDatasContext, failCtx)
	return
}

func (ft *FtSender) sendFromQueue(queueName string, readChan <-chan []byte, readDatasChan <-chan []Data, isRetry bool) {
	timer := time.NewTicker(time.Second)
	numWaits := 1
	var curDataContext, otherDataContext []*datasContext
	var curIdx int
	var backDataContext []*datasContext
	var err error
	for {
		if atomic.LoadInt32(&ft.stopped) > 0 {
			ft.exitChan <- struct{}{}
			return
		}
		if curIdx < len(curDataContext) {
			backDataContext, err = ft.trySendDatas(curDataContext[curIdx].Datas, numWaits, isRetry)
			curIdx++
		} else {
			select {
			case bytes := <-readChan:
				backDataContext, err = ft.trySendBytes(bytes, numWaits, isRetry)
			case datas := <-readDatasChan:
				backDataContext, err = ft.trySendDatas(datas, numWaits, isRetry)
			case <-timer.C:
				continue
			}
		}
		if err == nil {
			numWaits = 1
			//此处的成功发送没有被stats统计
		} else {
			log.Errorf("Runner[%v] Sender[%v] cannot send points from queue %v, error is %v", ft.runnerName, ft.innerSender.Name(), queueName, err)
			//此处的发送错误没有被stats统计
			numWaits++
			if numWaits > 10 {
				numWaits = 10
			}
		}
		if backDataContext != nil {
			otherDataContext = append(otherDataContext, backDataContext...)
		}
		if curIdx == len(curDataContext) {
			curDataContext = otherDataContext
			otherDataContext = make([]*datasContext, 0)
			curIdx = 0
		}
	}
}

func (_ *FtSender) SkipDeepCopy() {}

func SplitData(data string, splitSize int64) (valArray []string) {
	if splitSize <= 0 {
		return []string{data}
	}

	valArray = make([]string, 0)
	dataConverse := []rune(data)
	lenData := int64(len(dataConverse)) / splitSize

	for i := int64(1); i <= lenData; i++ {
		start := (i - 1) * splitSize
		end := i * splitSize
		valArray = append(valArray, string(dataConverse[start:end]))
	}

	end := lenData * splitSize
	remainData := string(dataConverse[end:])
	if len(remainData) != 0 {
		valArray = append(valArray, string(dataConverse[end:]))
	}
	return valArray
}
