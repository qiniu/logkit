package mongodb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var _ sender.SkipDeepCopySender = &Sender{}

// mongo sender Mongodb 根据UpdateKey 做对AccumulateKey $inc 累加的Sender
type Sender struct {
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

func init() {
	sender.RegisterConstructor(TypeMongodbAccumulate, NewSender)
}

// NewMongodbAccSender mongodb accumulate sender constructor
func NewSender(conf conf.MapConf) (mongodbSender sender.Sender, err error) {
	host, err := conf.GetPasswordEnvString(KeyMongodbHost)
	if err != nil {
		return nil, err
	}

	dbName, err := conf.GetString(KeyMongodbDB)
	if err != nil {
		return nil, err
	}
	updKey, err := conf.GetAliasList(KeyMongodbUpdateKey)
	if err != nil {
		return nil, err
	}
	accKey, err := conf.GetAliasList(KeyMongodbAccKey)
	if err != nil {
		return nil, err
	}
	collectionName, err := conf.GetString(KeyMongodbCollection)
	if err != nil {
		return nil, err
	}
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("mongodb_acc:(%v,db:%v,collection:%v)", host, dbName, collectionName))
	return newSender(name, host, dbName, collectionName, updKey, accKey)
}

func newSender(name, host, dbName, collectionName string, updKey, accKey []conf.AliasKey) (s *Sender, err error) {
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
	s = &Sender{
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
func (s *Sender) Send(datas []Data) (se error) {
	failure := []Data{}
	var err error
	var lastErr error
	ss := &StatsError{}
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
		ss.SendError = reqerr.NewSendError("Write failure, last err is: "+lastErr.Error(), sender.ConvertDatasBack(failure), reqerr.TypeDefault)
	}

	return nil
}

func (s *Sender) Name() string {
	if len(s.name) <= 0 {
		return fmt.Sprintf("mongodb://%s/%s/%s", s.host, s.dbName, s.collectionName)
	}
	return s.name
}

func (s *Sender) Close() error {
	s.Lock()
	s.stopped = true
	s.Unlock()
	return s.collection.CloseSession()
}

func (s *Sender) mongoSesssionKeeper(session *mgo.Session) {
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

func (_ *Sender) SkipDeepCopy() bool { return true }
