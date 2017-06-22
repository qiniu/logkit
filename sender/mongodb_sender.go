package sender

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/log"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MongoAccSender Mongodb 根据UpdateKey 做对AccumulateKey $inc 累加的Sender
type MongoAccSender struct {
	sync.RWMutex

	name           string
	host           string
	dbName         string
	collectionName string
	stopped        bool
	collection     utils.Collection
	updateKey      []conf.AliasKey
	accumulateKey  []conf.AliasKey
}

// 可选参数 当sender_type 为mongodb_* 的时候，需要必填的字段
const (
	KeyMongodbHost       = "mongodb_host"
	KeyMongodbDB         = "mongodb_db"
	KeyMongodbCollection = "mongodb_collection"
)

// 可选参数 当sender_type 为mongodb_acc 的时候，需要必填的字段
const (
	KeyMongodbUpdateKey = "mongodb_acc_updkey"
	KeyMongodbAccKey    = "mongodb_acc_acckey"
)

// NewMongodbAccSender mongodb accumulate sender constructor
func NewMongodbAccSender(conf conf.MapConf) (sender Sender, err error) {
	host, err := conf.GetString(KeyMongodbHost)
	if err != nil {
		return
	}
	dbName, err := conf.GetString(KeyMongodbDB)
	if err != nil {
		return
	}
	updKey, err := conf.GetAliasList(KeyMongodbUpdateKey)
	if err != nil {
		return
	}
	accKey, err := conf.GetAliasList(KeyMongodbAccKey)
	if err != nil {
		return
	}
	collectionName, err := conf.GetString(KeyMongodbCollection)
	if err != nil {
		return
	}
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("mongodb_acc:(%v,db:%v,collection:%v)", host, dbName, collectionName))
	return newMongoAccSender(name, host, dbName, collectionName, updKey, accKey)
}

func newMongoAccSender(name, host, dbName, collectionName string, updKey, accKey []conf.AliasKey) (s *MongoAccSender, err error) {
	// init mongodb collection
	cfg := utils.MongoConfig{
		Host: host,
		DB:   dbName,
	}
	var session *mgo.Session
	session, err = utils.MongoDail(cfg.Host, cfg.Mode, cfg.SyncTimeoutInS)
	if err != nil {
		return
	}
	db := session.DB(cfg.DB)
	coll := db.C(collectionName)
	if err != nil {
		session.Close()
		session = nil
		return
	}
	collection := utils.Collection{coll}

	if len(updKey) <= 0 || len(accKey) <= 0 {
		return nil, errors.New("The updateKey and accumulateKey should not be empty")
	}
	s = &MongoAccSender{
		name:           name,
		host:           host,
		dbName:         dbName,
		collectionName: collectionName,
		collection:     collection,
		updateKey:      updKey,
		accumulateKey:  accKey,
	}
	go s.mongoSesssionKeeper(s.collection.Database.Session)
	return s, nil
}

// Send 依次尝试发送数据到mongodb，返回错误中包含所有写失败的数据
// 如果要保证每次send的原子性，必须保证datas长度为1，否则当程序宕机
// 总会出现丢失数据的问题
func (s *MongoAccSender) Send(datas []Data) (se error) {
	failure := []Data{}
	var err error
	var lastErr error
	ss := &utils.StatsError{}
	for _, d := range datas {
		selector := bson.D{}
		for _, key := range s.updateKey {
			v, exist := d[key.Key]
			if !exist {
				log.Errorf("Cannot find out key %v", key)
				continue
			}
			selector = append(selector, bson.DocElem{Name: key.Alias, Value: v})
		}
		updator := bson.M{}
		for _, key := range s.accumulateKey {
			v, exist := d[key.Key]
			if !exist {
				log.Errorf("Cannot find out key %v", key)
				continue
			}
			updator[key.Alias] = v
		}
		_, err = s.collection.Upsert(selector, bson.M{"$inc": updator})
		if err != nil {
			ss.AddErrors()
			lastErr = err
			failure = append(failure, d)
		} else {
			ss.AddSuccess()
		}
	}
	if len(failure) > 0 && lastErr != nil {
		ss.ErrorDetail = reqerr.NewSendError("Write failure, last err is: "+lastErr.Error(), convertDatasBack(failure), reqerr.TypeDefault)
	}
	return ss
}

func (s *MongoAccSender) Name() string {
	if len(s.name) <= 0 {
		return fmt.Sprintf("mongodb://%s/%s/%s", s.host, s.dbName, s.collectionName)
	}
	return s.name
}

func (s *MongoAccSender) Close() error {
	s.Lock()
	s.stopped = true
	s.Unlock()
	return s.collection.CloseSession()
}

func (s *MongoAccSender) mongoSesssionKeeper(session *mgo.Session) {
	session.SetSocketTimeout(time.Second * 5)
	session.SetSyncTimeout(time.Second * 5)
	for {
		s.RLock()
		if s.stopped {
			s.RUnlock()
			break
		}
		s.RUnlock()

		err := session.Ping()
		if err != nil {
			session.Refresh()
			session.SetSocketTimeout(time.Second * 5)
			session.SetSyncTimeout(time.Second * 5)
		} else {
			time.Sleep(time.Second * 5)
		}
	}
}
