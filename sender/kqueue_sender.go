package sender

import (
	"sync/atomic"
	"time"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/queue"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/json-iterator/go"
	"github.com/qbox/base/com/src/qbox.us/errors"
	"sync"
	"fmt"
)

// FtSender fault tolerance sender wrapper
type KQueueSender struct {
	stopped      int32
	exitChan     chan struct{}
	logQueue     queue.BackendQueue
	backupQueue  queue.BackendQueue
	runnerName   string
	procs        int
	opt          *KqOption
	jsontool     jsoniter.API
	innerSenders map[string]Sender
}

type KqOption struct {
	hosts []string
	parts int
}

var (
	Kqs *KQueueSender
	mtx = new(sync.RWMutex)

	SendTypeRaw    = "raw"
)

const (
	KEYKAFKAQUEUEHOST = "kafka_queue_hosts"
	KeyKAFKAPROCS = "ft_procs"
	DEFAULTPARTS  = 5
	DEFAULTPROCS  = 1

	DATA_AK   = "_ak"
	DATA_SK   = "_sk"
	DATA_REPO = "_repo"

	DEFAULTREGION = "nb"
	DEFAULTHOST = "http://10.200.20.40:9999"
)

// NewFtSender Fault tolerant sender constructor
func NewKQueueSender(sender Sender, conf conf.MapConf) (*KQueueSender, error) {
	mtx.Lock()
	defer mtx.Unlock()
	if Kqs == nil {
		log.Info("start create kqueuesender")
		runnerName, _ := conf.GetStringOr(KeyRunnerName, UnderfinedRunnerName)

		hosts, err := conf.GetStringList(KEYKAFKAQUEUEHOST)
		if err != nil {
			return nil, err
		}

		procs, _ := conf.GetIntOr(KeyKAFKAPROCS, DEFAULTPARTS)
		opt := &KqOption{
			hosts: hosts,
			parts: procs,
		}

		kqs, err := newKQueueSender(sender, runnerName, opt)
		if err != nil {
			return nil, err
		}

		Kqs = kqs
	}

	return Kqs, nil
}

func newKQueueSender(innerSender Sender, runnerName string, opt *KqOption) (*KQueueSender, error) {
	var lq, bq queue.BackendQueue

	exitChan := make(chan struct{})

	lq = queue.NewKafkaQueue("stream"+qNameSuffix, opt.hosts, exitChan)
	if lq == nil {
		return nil, errors.New("cannot create kafka queue")
	}

	kqSender := KQueueSender{
		exitChan:     exitChan,
		innerSenders: make(map[string]Sender),
		logQueue:     lq,
		backupQueue:  bq,
		runnerName:   runnerName,
		opt:          opt,
		procs:        DEFAULTPROCS,
		jsontool:     jsoniter.Config{EscapeHTML: true, UseNumber: true}.Froze(),
	}
	go kqSender.asyncSendLogFromKafkaQueue()

	return &kqSender, nil
}

func (kq *KQueueSender) Name() string {
	return "Kafka Queue Sender"
}

func (kq *KQueueSender) Send(datas []Data) error {
	return kq.saveToKafka(datas)
}

func (kq *KQueueSender) Close() error {
	var err error

	atomic.AddInt32(&kq.stopped, 1)
	log.Warnf("Runner[%v] wait for Sender[%v] to completely exit", kq.runnerName, kq.Name())
	// 等待错误恢复流程退出
	<-kq.exitChan
	// 等待正常发送流程退出
	for i := 0; i < kq.procs; i++ {
		<-kq.exitChan
	}

	log.Warnf("Runner[%v] Sender[%v] has been completely exited", kq.runnerName, kq.Name())

	kq.logQueue.Close()
	//kq.backupQueue.Close()

	for _, sender := range kq.innerSenders {
		tmp := sender.Close()
		if tmp != nil {
			err = tmp
		}
	}

	return err
}

func (ft *KQueueSender) TokenRefresh(mapConf conf.MapConf) (err error) {
	return
}

// marshalData 将数据序列化
func (kq *KQueueSender) marshalData(datas []Data) (bs []byte, err error) {
	ctx := new(datasContext)
	ctx.Datas = datas
	bs, err = jsoniter.Marshal(ctx)
	if err != nil {
		err = reqerr.NewSendError("Cannot marshal data :"+err.Error(), ConvertDatasBack(datas), reqerr.TypeDefault)
		return
	}
	return
}

// unmarshalData 如何将数据从磁盘中反序列化出来
func (kq *KQueueSender) unmarshalData(dat []byte) (datas []Data, err error) {
	ctx := new(datasContext)
	err = kq.jsontool.Unmarshal(dat, &ctx)
	if err != nil {
		return
	}
	datas = ctx.Datas
	return
}

func (kq *KQueueSender) saveToKafka(datas []Data) error {
	log.Info("KQueueSender: Start to send to kafka")
	bs, err := kq.marshalData(datas)
	if err != nil {
		return err
	}
	err = kq.logQueue.Put(bs)
	if err != nil {
		return reqerr.NewSendError(" Cannot put data into kafka: "+err.Error(), ConvertDatasBack(datas), reqerr.TypeDefault)
	}
	return nil
}

func (kq *KQueueSender) asyncSendLogFromKafkaQueue() {
	for i := 0; i < kq.procs; i++ {
		go kq.sendFromQueue(kq.logQueue, false)
	}
	//go kq.sendFromQueue(kq.backupQueue, true)
}

// trySend 从bytes反序列化数据后尝试发送数据
func (kq *KQueueSender) trySendBytes(dat []byte, failSleep int, isRetry bool) (err error) {
	datas, err := kq.unmarshalData(dat)
	if err != nil {
		return
	}
	return kq.trySendDatas(datas, failSleep, isRetry)
}

// trySendDatas 尝试发送数据，如果失败，将失败数据加入backup queue，并睡眠指定时间。返回结果为是否正常发送
func (kq *KQueueSender) trySendDatas(datas []Data, failSleep int, isRetry bool) (err error) {
	var ak, sk, repo string
	if value, ok := datas[0][DATA_AK]; ok {
		ak = value.(string)
	} else {
		return errors.New("The data do not contain ak")
	}

	if value, ok := datas[0][DATA_SK]; ok {
		sk = value.(string)
	} else {
		return errors.New("The data do not contain sk")
	}

	if value, ok := datas[0][DATA_REPO]; ok {
		repo = value.(string)
	} else {
		return errors.New("The data do not contain repo")
	}
	log.Info("KQueueSender: call inner sender")
	sender := kq.innerSenders[ak+repo]
	if sender == nil {
		conf := make(conf.MapConf)
		conf[KeyPandoraRepoName] = repo
		conf[KeyPandoraAk] = ak
		conf[KeyPandoraSk] = sk
		conf[KeyPandoraSendType] = SendTypeRaw
		conf[KeyPandoraRegion] = DEFAULTREGION
		conf[KeyPandoraHost] = DEFAULTHOST

		constructor, exist := registeredConstructors[TypePandora]
		if !exist {
			return fmt.Errorf("pandora sender type unsupported")
		}
		sender, err = constructor(conf)
		if err != nil {
			return
		}
		kq.innerSenders[ak+repo] = sender
	}
	err = sender.Send(datas)

	if err != nil {
		log.Info("KQueueSender: inner sender is fail")
	}

	return
}

/*
func (kq *KQueueSender) handleSendError(err error, datas []Data) (retDatasContext []*datasContext) {

	failCtx := new(datasContext)
	var binaryUnpack bool
	se, succ := err.(*reqerr.SendError)
	if !succ {
		// 如果不是SendError 默认所有的数据都发送失败
		log.Infof("Runner[%v] Sender[%v] error type is not *SendError! reSend all datas by default", ft.runnerName, ft.innerSender.Name())
		failCtx.Datas = datas
		kq.stats.LastError = err.Error()
	} else {
		failCtx.Datas = ConvertDatas(se.GetFailDatas())
		if se.ErrorType == reqerr.TypeBinaryUnpack {
			binaryUnpack = true
		}
		kq.stats.LastError = se.Error()
	}
	if binaryUnpack {
		lens := len(failCtx.Datas) / 2
		if lens > 0 {
			newFailCtx := new(datasContext)
			newFailCtx.Datas = failCtx.Datas[0:lens]
			failCtx.Datas = failCtx.Datas[lens:]
			retDatasContext = append(retDatasContext, newFailCtx)
		} else if len(failCtx.Datas) == 1 {
			// 此处将 data 改为 raw 放在 pandora_stash 中
			if _, ok := failCtx.Datas[0][KeyPandoraStash]; !ok {
				data := make([]Data, 1)
				byteData, err := json.Marshal(failCtx.Datas[0])
				if err != nil {
					log.Warnf("Runner[%v] marshal data to string error %v", kq.runnerName, err)
				} else {
					data[0] = Data{
						KeyPandoraStash: string(byteData),
					}
					failCtx.Datas = data
				}
			}
		}
	}
	retDatasContext = append(retDatasContext, failCtx)
	return
}*/

func (kq *KQueueSender) sendFromQueue(queue queue.BackendQueue, isRetry bool) {
	readChan := queue.ReadChan()
	timer := time.NewTicker(time.Second)
	waitCnt := 1

	for {
		if atomic.LoadInt32(&kq.stopped) > 0 {
			kq.exitChan <- struct{}{}
			return
		}
		select {
		case dat := <-readChan:
			log.Info("KQueueSender: read msg from kafka")
			err := kq.trySendBytes(dat, waitCnt, isRetry)
			if err != nil {
				log.Error()
			}
			kq.logQueue.SyncMeta()
		case <-timer.C:
			continue
		}
	}
}
