package sender

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/queue"
	. "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/reqid"
)

const (
	defaultWriteLimit = 10 // 默认写速限制为10MB
	qNameSuffix       = "_local_save"
	directSuffix      = "_direct"
	defaultMaxProcs   = 1         // 默认没有并发
	// TypeMarshalError 表示marshal出错
	TypeMarshalError = reqerr.SendErrorType("Data Marshal failed")
)

var _ SkipDeepCopySender = &FtSender{}
var _ RawSender = &FtSender{}

// FtSender fault tolerance sender wrapper
type FtSender struct {
	stopped         int32
	exitChan        chan struct{}
	innerSender     Sender
	logQueue        queue.BackendQueue
	BackupQueue     queue.BackendQueue
	writeLimit      int // 写入速度限制，单位MB
	strategy        string
	procs           int //发送并发数
	runnerName      string
	opt             *FtOption
	stats           StatsInfo
	statsMutex      *sync.RWMutex
	jsontool        jsoniter.API
	pandoraKeyCache map[string]KeyInfo
	discardErr      bool
	maxLineLen      int64
	isBlock         bool
	backoff         *utils.Backoff
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
	innerSenderType   string
	pandoraSenderType string
	maxDiskUsedBytes  int64
	maxSizePerFile    int32
	discardErr        bool
	sendRaw           bool
	maxLineLen        int64
	isBlock           bool
}

type datasContext struct {
	Datas []Data   `json:"datas"`
	Lines []string `json:"lines"`
}

// NewFtSender Fault tolerant sender constructor
func NewFtSender(innerSender Sender, conf conf.MapConf, ftSaveLogPath string) (*FtSender, error) {
	memoryChannel, _ := conf.GetBoolOr(KeyFtMemoryChannel, false)
	memoryChannelSize, _ := conf.GetIntOr(KeyFtMemoryChannelSize, 100)
	logPath, _ := conf.GetStringOr(KeyFtSaveLogPath, ftSaveLogPath)
	syncEvery, _ := conf.GetIntOr(KeyFtSyncEvery, DefaultFtSyncEvery)
	writeLimit, _ := conf.GetIntOr(KeyFtWriteLimit, defaultWriteLimit)
	strategy, _ := conf.GetStringOr(KeyFtStrategy, KeyFtStrategyBackupOnly)
	longDataDiscard, _ := conf.GetBoolOr(KeyFtLongDataDiscard, false)
	senderType, _ := conf.GetStringOr(KeySenderType, "") //此处不会没有SenderType，在调用NewFtSender时已经检查
	pandoraSendType, _ := conf.GetStringOr(KeyPandoraSendType, "")
	switch strategy {
	case KeyFtStrategyAlwaysSave, KeyFtStrategyBackupOnly, KeyFtStrategyConcurrent:
	default:
		return nil, errors.New("no match ft_strategy")
	}
	runnerName, _ := conf.GetStringOr(KeyRunnerName, UnderfinedRunnerName)
	maxDiskUsedBytes, _ := conf.GetInt64Or(KeyMaxDiskUsedBytes, MaxDiskUsedBytes)
	maxSizePerFile, _ := conf.GetInt32Or(KeyMaxSizePerFile, MaxBytesPerFile)
	discardErr, _ := conf.GetBoolOr(KeyFtDiscardErr, false)
	if MaxProcs <= 0 {
		MaxProcs = NumCPU
	}
	procs, _ := conf.GetIntOr(KeyFtProcs, MaxProcs)
	sendraw, _ := conf.GetBoolOr(InnerSendRaw, false)
	if sendraw {
		_, ok := innerSender.(RawSender)
		if !ok {
			return nil, errors.New("inner sender is not RawSender, can not use RawSend")
		}
	}
	maxLineLen, _ := conf.GetInt64Or(KeyRunnerMaxLineLen, 0)
	isBlock, _ := conf.GetBoolOr(KeyRunnerIsBlock, false)

	opt := &FtOption{
		saveLogPath:       logPath,
		syncEvery:         int64(syncEvery),
		writeLimit:        writeLimit,
		strategy:          strategy,
		procs:             procs,
		memoryChannel:     memoryChannel,
		memoryChannelSize: memoryChannelSize,
		longDataDiscard:   longDataDiscard,
		innerSenderType:   senderType,
		pandoraSenderType: pandoraSendType,
		maxDiskUsedBytes:  maxDiskUsedBytes,
		maxSizePerFile:    maxSizePerFile,
		discardErr:        discardErr,
		sendRaw:           sendraw,
		maxLineLen:        maxLineLen,
		isBlock:           isBlock,
	}

	return newFtSender(innerSender, runnerName, opt)
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
		lq = queue.NewDiskQueue(queue.NewDiskQueueOptions{
			Name:             "stream" + qNameSuffix,
			DataPath:         opt.saveLogPath,
			MaxBytesPerFile:  int64(opt.maxSizePerFile),
			MaxMsgSize:       opt.maxSizePerFile,
			SyncEveryWrite:   opt.syncEvery,
			SyncEveryRead:    opt.syncEvery,
			SyncTimeout:      2 * time.Second,
			WriteRateLimit:   opt.writeLimit * MB,
			MaxDiskUsedBytes: opt.maxDiskUsedBytes,
		})
	} else {
		lq = queue.NewDiskQueue(queue.NewDiskQueueOptions{
			Name:              "stream" + qNameSuffix,
			DataPath:          opt.saveLogPath,
			MaxBytesPerFile:   int64(opt.maxSizePerFile),
			MaxMsgSize:        opt.maxSizePerFile,
			SyncEveryWrite:    opt.syncEvery,
			SyncEveryRead:     opt.syncEvery,
			SyncTimeout:       2 * time.Second,
			WriteRateLimit:    opt.writeLimit * MB,
			EnableMemoryQueue: true,
			MemoryQueueSize:   int64(opt.memoryChannelSize),
			MaxDiskUsedBytes:  opt.maxDiskUsedBytes,
		})
	}
	bq = queue.NewDiskQueue(queue.NewDiskQueueOptions{
		Name:             "backup" + qNameSuffix,
		DataPath:         opt.saveLogPath,
		MaxBytesPerFile:  int64(opt.maxSizePerFile),
		MaxMsgSize:       opt.maxSizePerFile,
		SyncEveryWrite:   opt.syncEvery,
		SyncEveryRead:    opt.syncEvery,
		SyncTimeout:      2 * time.Second,
		WriteRateLimit:   opt.writeLimit * MB,
		MaxDiskUsedBytes: opt.maxDiskUsedBytes,
	})
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
		discardErr:  opt.discardErr,
		maxLineLen:  opt.maxLineLen,
		isBlock:     opt.isBlock,
		backoff:     utils.NewBackoff(2, 1, 1*time.Second, 5*time.Minute),
	}

	if opt.innerSenderType == TypePandora {
		ftSender.pandoraKeyCache = make(map[string]KeyInfo)
	}
	go ftSender.asyncSendLogFromQueue()
	return &ftSender, nil
}

func (ft *FtSender) Name() string {
	return ft.innerSender.Name()
}
func (ft *FtSender) RawSend(datas []string) error {
	if !ft.opt.sendRaw {
		return errors.New("ft sender is not initialized by send raw, config sendRaw to use SendRaw")
	}
	se := &StatsError{Ft: true, FtNotRetry: true}
	if ft.strategy == KeyFtStrategyBackupOnly {
		// 尝试直接发送数据，当数据失败的时候会加入到本地重试队列。外部不需要重试
		isRetry := false
		backDataContext, err := ft.trySendRaws(datas, 1, isRetry)
		if err == nil {
			return nil
		}
		if ste, ok := err.(*StatsError); ok {
			se.Errors = ste.Errors
			se.Success = ste.Success
			se.LastError = ste.LastError
			se.SendError = ste.SendError
		} else {
			se.AddErrorsNum(len(datas))
			se.LastError = err.Error()
		}

		err = fmt.Errorf("Runner[%v] Sender[%v] try Send Datas err: %v, will put to backup queue and retry later... ", ft.runnerName, ft.innerSender.Name(), err)
		log.Error(err)
		// 容错队列会保证重试，此处不向外部暴露发送错误信息
		se.FtQueueLag = ft.BackupQueue.Depth()
		if backDataContext != nil {
			var nowDatas []Data
			for _, v := range backDataContext {
				nowDatas = append(nowDatas, v.Datas...)
			}
			if nowDatas != nil {
				se.FtNotRetry = false
				se.SendError = reqerr.NewSendError("save data to backend queue error", ConvertDatasBack(nowDatas), reqerr.TypeDefault)
				se.LastError = se.SendError.Error()
				ft.statsMutex.Lock()
				ft.stats.LastError = se.SendError.Error()
				ft.statsMutex.Unlock()
			}
		}
	} else {
		err := ft.saveRawToFile(datas)
		if err != nil {
			se.FtNotRetry = false
			if sendError, ok := err.(*reqerr.SendError); ok {
				se.SendError = sendError
			}
			se.AddErrorsNum(len(datas))
			ft.statsMutex.Lock()
			ft.stats.LastError = err.Error()
			ft.stats.Errors += int64(len(datas))
			ft.statsMutex.Unlock()
			time.Sleep(ft.backoff.Duration())
		} else {
			// se 中的 lasterror 和 senderror 都为空，需要使用 se.FtQueueLag
			se.AddSuccessNum(len(datas))
			ft.backoff.Reset()
		}
		se.FtQueueLag = ft.BackupQueue.Depth() + ft.logQueue.Depth()
	}
	return se
}

func (ft *FtSender) Send(datas []Data) error {
	if ft.opt.sendRaw {
		return errors.New("ft sender is initialized by send raw, can not use Send(), please use SendRaw")
	}
	switch ft.opt.innerSenderType {
	case TypePandora:
		if ft.opt.pandoraSenderType != "raw" {
			for i, v := range datas {
				datas[i] = DeepConvertKeyWithCache(v, ft.pandoraKeyCache)
			}
		}
	default:
	}

	se := &StatsError{Ft: true, FtNotRetry: true}
	if ft.isBlock || ft.strategy == KeyFtStrategyBackupOnly {
		// 尝试直接发送数据，当数据失败的时候会加入到本地重试队列。外部不需要重试
		isRetry := false
		backDataContext, err := ft.backOffReTrySend(datas, 1, isRetry)
		if err == nil {
			return nil
		}

		if ste, ok := err.(*StatsError); ok {
			se.Errors = ste.Errors
			se.Success = ste.Success
			se.LastError = ste.LastError
			se.SendError = ste.SendError
		} else {
			se.AddErrorsNum(len(datas))
			se.LastError = err.Error()
		}

		if ft.isBlock {
			log.Error("Runner[%v] Sender[%v] try Send Datas err: %v", ft.runnerName, ft.innerSender.Name(), err)
			return se;
		}

		err = fmt.Errorf("Runner[%v] Sender[%v] try Send Datas err: %v, will put to backup queue and retry later... ", ft.runnerName, ft.innerSender.Name(), err)
		log.Error(err)
		// 容错队列会保证重试，此处不向外部暴露发送错误信息
		se.FtQueueLag = ft.BackupQueue.Depth()
		if backDataContext != nil {
			var nowDatas []Data
			for _, v := range backDataContext {
				nowDatas = append(nowDatas, v.Datas...)
			}
			if nowDatas != nil {
				se.FtNotRetry = false
				se.SendError = reqerr.NewSendError("save data to backend queue error", ConvertDatasBack(nowDatas), reqerr.TypeDefault)
				se.LastError = se.SendError.Error()
				ft.statsMutex.Lock()
				ft.stats.LastError = se.SendError.Error()
				ft.statsMutex.Unlock()
			}
		}
		return se
	}

	err := ft.saveToFile(datas)
	if err != nil {
		se.FtNotRetry = false
		if sendError, ok := err.(*reqerr.SendError); ok {
			se.SendError = sendError
		}
		se.AddErrorsNum(len(datas))
		ft.statsMutex.Lock()
		ft.stats.LastError = err.Error()
		ft.stats.Errors += int64(len(datas))
		ft.statsMutex.Unlock()
		time.Sleep(ft.backoff.Duration())
	} else {
		// se 中的 lasterror 和 senderror 都为空，需要使用 se.FtQueueLag
		se.AddSuccessNum(len(datas))
		ft.backoff.Reset()
	}
	se.FtQueueLag = ft.BackupQueue.Depth() + ft.logQueue.Depth()
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
func (ft *FtSender) marshalRaws(datas []string) ([]byte, error) {
	bs, err := jsoniter.Marshal(&datasContext{
		Lines: datas,
	})
	if err != nil {
		return nil, reqerr.NewRawSendError("Cannot marshal data :"+err.Error(), datas, reqerr.TypeDefault)
	}
	return bs, nil
}

// unmarshalData 如何将数据从磁盘中反序列化出来
func (ft *FtSender) unmarshalRaws(dat []byte) (datas []string, err error) {
	ctx := new(datasContext)
	err = ft.jsontool.Unmarshal(dat, &ctx)
	if err != nil {
		return nil, err
	}
	return ctx.Lines, nil
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
		return nil, err
	}
	return ctx.Datas, nil
}

func (ft *FtSender) saveRawToFile(datas []string) error {
	if dqueue, ok := ft.logQueue.(queue.LinesQueue); ok {
		return dqueue.PutLines(datas)
	}
	bs, err := ft.marshalRaws(datas)
	if err != nil {
		return err
	}
	err = ft.logQueue.Put(bs)
	if err != nil {
		return reqerr.NewRawSendError(ft.innerSender.Name()+" Cannot put data into backendQueue: "+err.Error(), datas, TypeMarshalError)
	}
	return nil
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
		return reqerr.NewSendError(ft.innerSender.Name()+" Cannot put data into backendQueue: "+err.Error(), ConvertDatasBack(datas), TypeMarshalError)
	}
	return nil
}

func (ft *FtSender) asyncSendLogFromQueue() {
	for i := 0; i < ft.procs; i++ {
		if ft.opt.sendRaw {
			readLinesChan := make(<-chan []string)
			if dqueue, ok := ft.logQueue.(queue.LinesQueue); ok {
				readLinesChan = dqueue.ReadLinesChan()
			}
			go ft.sendRawFromQueue(ft.logQueue.Name(), ft.logQueue.ReadChan(), readLinesChan, false)
		} else {
			readDatasChan := make(<-chan []Data)
			if dqueue, ok := ft.logQueue.(queue.DataQueue); ok {
				readDatasChan = dqueue.ReadDatasChan()
			}
			go ft.sendFromQueue(ft.logQueue.Name(), ft.logQueue.ReadChan(), readDatasChan, false)
		}
	}
	if ft.opt.sendRaw {
		readLinesChan := make(<-chan []string)
		go ft.sendRawFromQueue(ft.BackupQueue.Name(), ft.BackupQueue.ReadChan(), readLinesChan, true)
	} else {
		readDatasChan := make(<-chan []Data)
		go ft.sendFromQueue(ft.BackupQueue.Name(), ft.BackupQueue.ReadChan(), readDatasChan, true)
	}
}

// trySend 从bytes反序列化数据后尝试发送数据
func (ft *FtSender) trySendBytes(dat []byte, failSleep int, isRetry bool) (backDataContext []*datasContext, err error) {
	if ft.opt.sendRaw {
		datas, err := ft.unmarshalRaws(dat)
		if err != nil {
			return nil, err
		}
		return ft.trySendRaws(datas, failSleep, isRetry)
	}
	datas, err := ft.unmarshalData(dat)
	if err != nil {
		return nil, err
	}

	return ft.backOffReTrySend(datas, failSleep, isRetry)
}

func (ft *FtSender) trySendRaws(datas []string, failSleep int, isRetry bool) (backDataContext []*datasContext, err error) {
	rawSender, ok := ft.innerSender.(RawSender)
	if !ok {
		return nil, errors.New("inner sender not support Raw Sender")
	}
	err = rawSender.RawSend(datas)
	dataLen := int64(len(datas))

	err = ft.handleStat(err, isRetry, dataLen)
	if empty := isErrorEmpty(err); empty {
		return nil, nil
	}

	retDatasContext := ft.handleRawSendError(err, datas)
	for _, v := range retDatasContext {
		nnBytes, err := jsoniter.Marshal(v)
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] marshal %v failed: %v", ft.runnerName, ft.innerSender.Name(), *v, err)
			continue
		}

		err = ft.BackupQueue.Put(nnBytes)
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] cannot write points back to queue %v: %v", ft.runnerName, ft.innerSender.Name(), ft.BackupQueue.Name(), err)
			backDataContext = append(backDataContext, v)
		}
	}
	time.Sleep(time.Second * time.Duration(math.Pow(2, float64(failSleep))))

	return backDataContext, err
}

// trySendDatas 尝试发送数据，如果失败，将失败数据加入backup queue，并睡眠指定时间。返回结果为是否正常发送
// isRetry 只有在 FtStrategy 为 BackupOnly 或
func (ft *FtSender) trySendDatas(datas []Data, failSleep int, isRetry bool) (backDataContext []*datasContext, err error) {
	err = ft.innerSender.Send(datas)
	dataLen := int64(0)
	if datas != nil {
		dataLen = int64(len(datas))
	}

	err = ft.handleStat(err, isRetry, dataLen)
	if empty := isErrorEmpty(err); empty {
		return nil, nil
	}

	retDatasContext := ft.handleSendError(err, datas)
	if retDatasContext == nil {
		return nil, nil
	}

	if ft.isBlock {
		return retDatasContext, err
	}

	for _, v := range retDatasContext {
		nnBytes, err := jsoniter.Marshal(v)
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] marshal %v failed: %v", ft.runnerName, ft.innerSender.Name(), *v, err)
			continue
		}

		err = ft.BackupQueue.Put(nnBytes)
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] cannot write points back to queue %v: %v", ft.runnerName, ft.innerSender.Name(), ft.BackupQueue.Name(), err)
			backDataContext = append(backDataContext, v)
		}
	}
	time.Sleep(time.Second * time.Duration(math.Pow(2, float64(failSleep))))
	// 发送出错超过10次，并且设置了丢弃发送出错的数据时，丢弃数据
	if failSleep >= 8 && ft.discardErr {
		return nil, nil
	}

	return backDataContext, err
}

func (ft *FtSender) handleStat(err error, isRetry bool, dataLen int64) error {
	ft.statsMutex.Lock()
	defer ft.statsMutex.Unlock()

	if err == nil {
		if isRetry {
			ft.stats.Errors -= dataLen
		}
		ft.stats.Success += dataLen
		ft.stats.LastError = ""
		return nil
	}

	var errRes StatsError
	c, ok := err.(*StatsError)
	if !ok {
		ft.stats.Errors += dataLen
		ft.stats.LastError = err.Error()
		errRes.Errors += dataLen
		return err
	}

	if c.SendError == nil && c.LastError == "" {
		if isRetry {
			ft.stats.Errors -= dataLen
		}
		ft.stats.Success += dataLen
		ft.stats.LastError = ""
		return nil
	}

	// sender error 为空时（last error非空），一定是全部发送失败
	if c.SendError == nil {
		if !isRetry {
			ft.stats.Errors += dataLen
		}
		ft.stats.LastError = c.LastError
		return err
	}

	// sender error 不为空时，可能部分成功部分失败
	if !isRetry {
		ft.stats.Errors += c.Errors
	} else {
		ft.stats.Errors -= c.Success
	}
	ft.stats.Success += c.Success
	ft.stats.LastError = c.SendError.Error()
	return err
}

func (ft *FtSender) handleRawSendError(err error, datas []string) (retDatasContext []*datasContext) {
	failCtx := new(datasContext)
	var binaryUnpack bool
	var errMessage string
	se, succ := err.(*StatsError)
	if !succ {
		// 如果不是SendError 默认所有的数据都发送失败
		errMessage = "error type is not *StatsError! reSend all datas by default"
		failCtx.Lines = datas
	} else {
		if se.SendError == nil {
			// 如果不是SendError 默认所有的数据都发送失败
			errMessage = "error type is not *SendError! reSend all datas by default"
			failCtx.Lines = datas
		} else {
			failCtx.Lines = datas
			switch se.SendError.ErrorType {
			case reqerr.TypeBinaryUnpack:
				binaryUnpack = true
				errMessage = "error type is binaryUnpack, will be divided to 2 parts and retry"
			case reqerr.TypeSchemaFreeRetry:
				errMessage = "maybe this is because of server schema cache, will send all data again"
			case reqerr.TypeContainInvalidPoint:
				binaryUnpack = true
				errMessage = "error type is invalidPoint, all failed data will be divided to 2 parts and retry"
			default:
				errMessage = "error type is default, will send all failed data again"
			}
		}
	}

	log.Errorf("Runner[%v] Sender[%v] cannot write points: %v, failDatas size: %v, %s", ft.runnerName, ft.innerSender.Name(), err, len(failCtx.Lines), errMessage)
	log.Debugf("Runner[%v] Sender[%v] failed datas [[%v]]", ft.runnerName, ft.innerSender.Name(), failCtx.Lines)
	if binaryUnpack {
		lens := len(failCtx.Lines) / 2
		if lens > 0 {
			newFailCtx := new(datasContext)
			newFailCtx.Lines = failCtx.Lines[0:lens]
			failCtx.Lines = failCtx.Lines[lens:]
			retDatasContext = append(retDatasContext, newFailCtx)
			retDatasContext = append(retDatasContext, failCtx)
			return retDatasContext
		}
		//当数据仅有一条且还要binary Unpack时，只能丢弃
		if len(failCtx.Lines) == 1 {
			log.Infof("Runner[%s] Sender[%s] discard long data (more than 2M), length: %d", ft.runnerName, ft.innerSender.Name(), len(failCtx.Lines[0]))
		}
		return retDatasContext
	}

	retDatasContext = append(retDatasContext, failCtx)
	return retDatasContext
}

func (ft *FtSender) handleSendError(err error, datas []Data) (retDatasContext []*datasContext) {
	failCtx := new(datasContext)
	var binaryUnpack bool
	var errMessage string
	se, succ := err.(*StatsError)
	if !succ {
		// 如果不是SendError 默认所有的数据都发送失败
		errMessage = "error type is not *StatsError! reSend all datas by default"
		failCtx.Datas = datas
	} else {
		if se.SendError == nil {
			// 如果不是SendError 默认所有的数据都发送失败
			errMessage = "error type is not *SendError! reSend all datas by default"
			failCtx.Datas = datas
		} else {
			failCtx.Datas = ConvertDatas(se.SendError.GetFailDatas())
			if se.SendError.ErrorType == reqerr.TypeBinaryUnpack {
				binaryUnpack = true
				errMessage = "error type is binaryUnpack, will be divided to 2 parts and retry"
			} else if se.SendError.ErrorType == reqerr.TypeSchemaFreeRetry {
				errMessage = "maybe this is because of server schema cache, will send all data again"
			} else {
				errMessage = "error type is default, will send all failed data again"
			}
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
			return retDatasContext
		}
		if len(failCtx.Datas) == 1 {
			//当数据仅有一条且discardErr为true时，丢弃数据
			if ft.discardErr {
				return nil
			}
			failCtxData := failCtx.Datas[0]
			// 小于 2M 时，放入 pandora_stash中
			dataBytes, err := jsoniter.Marshal(failCtxData)
			if err != nil {
				log.Errorf("binaryUnpack marshal failed, err: %v", err)
				retDatasContext = append(retDatasContext, failCtx)
				return retDatasContext
			}
			dataBytesLen := int64(len(string(dataBytes)))
			if ft.maxLineLen > 0 && dataBytesLen > ft.maxLineLen {
				return nil
			}

			if dataBytesLen < DefaultMaxBatchSize {
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
				return retDatasContext
			}

			if ft.opt.longDataDiscard {
				log.Infof("Runner[%s] Sender[%s] discard long data (more than 2M), length: %d", ft.runnerName, ft.innerSender.Name(), len(string(dataBytes)))
				return retDatasContext
			}

			// 大于 2M 时，切片发送
			remainData := make(Data, 0)
			separateData := make(Data, 0)
			for failCtxDataKey, failCtxDataVal := range failCtxData {
				byteVal, err := json.Marshal(failCtxDataVal)
				if err != nil {
					log.Warnf("Runner[%s] marshal data to string error %v", ft.runnerName, err)
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
				newFailCtx.Datas = append(newFailCtx.Datas, remainData)
				for separateDataKey, separateDataVal := range separateData {
					strVal, _ := separateDataVal.(string)
					valArray := SplitData(strVal)
					log.Infof("Runner[%s] Sender[%s] split long data (more than 2M) %d to array, array length %d", ft.runnerName, ft.innerSender.Name(), len(strVal), len(valArray))
					separateId := reqid.Gen()
					for idx, val := range valArray {
						retData := make(Data, 0)
						retData[KeyPandoraSeparateId] = separateId + "_" + separateDataKey + "_" + strconv.Itoa(idx)
						retData[separateDataKey] = val
						newFailCtx.Datas = append(newFailCtx.Datas, retData)
					}
				}
				retDatasContext = append(retDatasContext, newFailCtx)
				return retDatasContext
			}

			// failCtxData 的 key value 中未找到 string 类型且大于 2M 的 value
			// 此时将 failCtxData 进行 Marshal 之后进行切片，放入pandaora_stash中
			valArray := SplitData(string(dataBytes))
			log.Infof("Runner[%s] Sender[%s] split long data (more than 2M) %d to array, array length %d", ft.runnerName, ft.innerSender.Name(), len(string(dataBytes)), len(valArray))
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
			return retDatasContext
		}

		return retDatasContext
	}

	retDatasContext = append(retDatasContext, failCtx)
	return retDatasContext
}

func (ft *FtSender) sendRawFromQueue(queueName string, readChan <-chan []byte, readDatasChan <-chan []string, isRetry bool) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
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
			backDataContext, err = ft.backOffReTrySendRaw(curDataContext[curIdx].Lines, numWaits, isRetry)
			curIdx++
		} else {
			select {
			case bytes := <-readChan:
				backDataContext, err = ft.trySendBytes(bytes, numWaits, isRetry)
			case datas := <-readDatasChan:
				backDataContext, err = ft.backOffReTrySendRaw(datas, numWaits, isRetry)
			case <-timer.C:
				continue
			}
		}
		if err == nil {
			numWaits = 1
			//此处的成功发送没有被stats统计
		} else {
			log.Errorf("Runner[%s] Sender[%s] cannot send points from queue %s, error is %v", ft.runnerName, ft.innerSender.Name(), queueName, err)
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

func (ft *FtSender) sendFromQueue(queueName string, readChan <-chan []byte, readDatasChan <-chan []Data, isRetry bool) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
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
			backDataContext, err = ft.backOffReTrySend(curDataContext[curIdx].Datas, numWaits, isRetry)
			curIdx++
		} else {
			select {
			case bytes := <-readChan:
				backDataContext, err = ft.trySendBytes(bytes, numWaits, isRetry)
			case datas := <-readDatasChan:
				backDataContext, err = ft.backOffReTrySend(datas, numWaits, isRetry)
			case <-timer.C:
				continue
			}
		}
		if err == nil {
			numWaits = 1
			//此处的成功发送没有被stats统计
		} else {
			log.Errorf("Runner[%s] Sender[%s] cannot send points from queue %s, error is %v", ft.runnerName, ft.innerSender.Name(), queueName, err)
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

func (ft *FtSender) SkipDeepCopy() bool {
	ss, ok := ft.innerSender.(SkipDeepCopySender)
	if ok {
		return ss.SkipDeepCopy()
	}
	return false
}

//优先使用'\n'对数据进行切分，切分后单个分片仍大于batchsize再按指定大小进行切分
func SplitData(data string) (valArray []string) {
	start := 0
	last := 0
	offset := start
	for index := 0; index != -1 && index+1 < len(data); {
		if offset-start < DefaultMaxBatchSize {
			last = offset
			index = strings.IndexByte(data[offset:], '\n')
			offset += index + 1
			continue
		}
		//单个slice大于2M
		if start == last {
			valArray = append(valArray, SplitDataWithSplitSize(data[start:offset], DefaultSplitSize)...)
			start = offset
			continue
		}
		valArray = append(valArray, data[start:last])
		start = last
	}
	if start != last {
		valArray = append(valArray, data[start:last])
	}
	//防止加上最后一个slice后大于2M
	valArray = append(valArray, SplitDataWithSplitSize(data[last:], DefaultSplitSize)...)
	return valArray
}

func SplitDataWithSplitSize(data string, splitSize int64) (valArray []string) {
	if splitSize <= 0 {
		return []string{data}
	}

	valArray = make([]string, 0)
	lenData := int64(len(data)) / splitSize

	for i := int64(1); i <= lenData; i++ {
		start := (i - 1) * splitSize
		end := i * splitSize
		valArray = append(valArray, string(data[start:end]))
	}

	end := lenData * splitSize
	remainData := string(data[end:])
	if len(remainData) != 0 {
		valArray = append(valArray, string(data[end:]))
	}
	return valArray
}

func isErrorEmpty(err error) bool {
	if err == nil {
		return true
	}

	se, succ := err.(*StatsError)
	if !succ {
		return false
	}
	if se.LastError == "" && se.SendError == nil {
		return true
	}

	return false
}

// 阻塞式全量发送
func (ft *FtSender) backOffReTrySend(datas []Data, failSleep int, isRetry bool) (res []*datasContext, err error) {
	if !ft.isBlock {
		return ft.trySendDatas(datas, failSleep, isRetry)
	}

	backoff := utils.NewBackoff(2, 1, time.Second, 5*time.Minute)
	for {
		if atomic.LoadInt32(&ft.stopped) > 0 {
			for _, v := range res {
				nnBytes, err := jsoniter.Marshal(v)
				if err != nil {
					log.Errorf("Runner[%v] Sender[%v] marshal %v failed: %v", ft.runnerName, ft.innerSender.Name(), *v, err)
					continue
				}

				err = ft.BackupQueue.Put(nnBytes)
				if err != nil {
					log.Errorf("Runner[%v] Sender[%v] cannot write points back to queue %v: %v", ft.runnerName, ft.innerSender.Name(), ft.BackupQueue.Name(), err)
				}
			}
			if len(res) != 0 {
				time.Sleep(2 * time.Second)
			}
			ft.exitChan <- struct{}{}
			return
		}
		if len(res) != 0 {
			resResult := make([]*datasContext, 0, 20)
			for _, resDatas := range res {
				for _, sendData := range resDatas.Datas {
					resTmp, err := ft.trySendDatas([]Data{sendData}, 1, isRetry)
					if err != nil {
						resResult = append(resResult, resTmp...)
					}
				}
			}
			res = resResult
			if len(res) == 0 {
				return nil, nil
			}
		} else {
			if res, err = ft.trySendDatas(datas, 1, isRetry); err == nil {
				return nil, nil
			}
		}

		// 非阻塞式全量发送
		if len(res) > 1 {
			return res, err
		}
		time.Sleep(backoff.Duration())
		// 阻塞
		if backoff.Attempt() >= 8 {
			if ft.discardErr {
				return nil, nil
			}
			backoff.Reset()
		}
	}
}

// 阻塞式全量发送
func (ft *FtSender) backOffReTrySendRaw(datas []string, failSleep int, isRetry bool) (res []*datasContext, err error) {
	if !ft.isBlock {
		return ft.trySendRaws(datas, failSleep, isRetry)
	}
	for idx := 0; idx < 10; idx++ {
		if atomic.LoadInt32(&ft.stopped) > 0 {
			for _, v := range res {
				nnBytes, err := jsoniter.Marshal(v)
				if err != nil {
					log.Errorf("Runner[%v] Sender[%v] marshal %v failed: %v", ft.runnerName, ft.innerSender.Name(), *v, err)
					continue
				}

				err = ft.BackupQueue.Put(nnBytes)
				if err != nil {
					log.Errorf("Runner[%v] Sender[%v] cannot write points back to queue %v: %v", ft.runnerName, ft.innerSender.Name(), ft.BackupQueue.Name(), err)
				}
			}
			if len(res) != 0 {
				time.Sleep(2 * time.Second)
			}
			ft.exitChan <- struct{}{}
			return
		}
		res, err = ft.trySendRaws(datas, idx, isRetry)
		if err == nil {
			return nil, nil
		}

		time.Sleep(time.Second * time.Duration(math.Pow(2, float64(idx))))
		// 阻塞
		if idx == 9 && !ft.discardErr {
			idx = 0
		}
	}
	// 发送10次失败，并且配置了丢弃
	if err != nil && ft.discardErr {
		return nil, nil
	}
	return res, err
}
