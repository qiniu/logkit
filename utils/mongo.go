package utils

import (
	"strings"
	"time"

	"github.com/qiniu/log"
	"gopkg.in/mgo.v2"
)

var g_modes = map[string]int{
	"eventual":  0,
	"monotonic": 1,
	"mono":      1,
	"strong":    2,
}

type MongoConfig struct {
	Host           string `json:"host"`
	DB             string `json:"db"`
	Mode           string `json:"mode"`
	SyncTimeoutInS int64  `json:"timeout"` // 以秒为单位
	Direct         bool   `json:"direct"`
}

type Collection struct {
	*mgo.Collection
}

func (c Collection) CloseSession() (err error) {
	c.Database.Session.Close()
	return nil
}

// ------------------------------------------------------------------------
func MongoDail(host string, mode string, syncTimeoutInS int64) (session *mgo.Session, err error) {

	session, err = mgo.Dial(host)
	if err != nil {
		log.Error("Connect MongoDB failed:", err, "- host:", host)
		return
	}
	if mode != "" {
		SetMode(session, mode, true)
	}
	if syncTimeoutInS != 0 {
		session.SetSyncTimeout(time.Duration(int64(time.Second) * syncTimeoutInS))
	}
	return
}

func SetMode(s *mgo.Session, modeFriendly string, refresh bool) {
	mode, ok := g_modes[strings.ToLower(modeFriendly)]
	if !ok {
		log.Fatal("invalid mgo mode")
	}
	switch mode {
	case 0:
		s.SetMode(mgo.Eventual, refresh)
	case 1:
		s.SetMode(mgo.Monotonic, refresh)
	case 2:
		s.SetMode(mgo.Strong, refresh)
	}
}
