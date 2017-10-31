package mgr

import (
	"errors"
	"net/http"
	"sync"

	"github.com/qiniu/logkit/utils"

	"bytes"
	"encoding/json"
	"io/ioutil"

	"fmt"
	"time"

	"github.com/labstack/echo"
	"github.com/qiniu/log"
)

type ClusterConfig struct {
	MasterUrl []string `json:"master_url"`
	IsMaster  bool     `json:"is_master"`
	Enable    bool     `json:"enable"`
}

type Cluster struct {
	ClusterConfig
	slaves       []Slave
	myaddress    string
	mytag        string
	mutex        *sync.RWMutex
	statusUpdate time.Time
}

type Slave struct {
	Url       string    `json:"url"`
	Tag       string    `json:"tag"`
	Status    string    `json:"status"`
	LastTouch time.Time `json:"last_touch"`
}

type ClusterStatus struct {
	Status map[string]RunnerStatus `json:"status"`
	Tag    string                  `json:"tag"`
	Err    error                   `json:"error"`
}

const (
	StatusOK   = "ok"
	StatusBad  = "bad"
	StatusLost = "lost"
)

func NewCluster(cc *ClusterConfig) *Cluster {
	cl := new(Cluster)
	cl.ClusterConfig = *cc
	cl.slaves = make([]Slave, 0)
	cl.mutex = new(sync.RWMutex)
	return cl
}

func (cc *Cluster) RunRegisterLoop() error {
	if err := Register(cc.MasterUrl, cc.myaddress, "default"); err != nil {
		return fmt.Errorf("master %v is unavaliable", cc.MasterUrl)
	}
	go func() {
		time.Sleep(15 * time.Second)
		if err := Register(cc.MasterUrl, cc.myaddress, "default"); err != nil {
			log.Errorf("master %v is unavaliable", cc.MasterUrl)
		}
	}()
	return nil
}

func (cc *Cluster) AddSlave(url, tag string) {
	for idx, v := range cc.slaves {
		if v.Url == url {
			v.Tag = tag
			v.LastTouch = time.Now()
			v.Status = StatusOK
			cc.mutex.Lock()
			cc.slaves[idx] = v
			cc.mutex.Unlock()
			return
		}
	}
	cc.mutex.Lock()
	cc.slaves = append(cc.slaves, Slave{url, tag, StatusOK, time.Now()})
	cc.mutex.Unlock()
	return
}

func (cc *Cluster) UpdateSlaveStatus() {
	now := time.Now()
	if now.Sub(cc.statusUpdate) < 15*time.Second {
		return
	}
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for idx, v := range cc.slaves {
		if now.Sub(v.LastTouch) < 31*time.Second {
			v.Status = StatusOK
		} else if now.Sub(v.LastTouch) <= time.Minute {
			v.Status = StatusBad
		} else {
			v.Status = StatusLost
		}
		cc.slaves[idx] = v
	}
	cc.statusUpdate = now
}

// master API
// get /logkit/cluster/ping
func (rs *RestService) Ping() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, nil)
	}
}

// master API
// get /logkit/cluster/slaves?tag=tagvalue
func (rs *RestService) Slaves() echo.HandlerFunc {
	return func(c echo.Context) error {
		if rs.cluster == nil || !rs.cluster.Enable {
			err := errors.New("cluster function not configed")
			return c.JSON(http.StatusBadRequest, err)
		}
		req := c.Request()
		if err := req.ParseForm(); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		tagvalue := req.Form.Get("tag")
		var slaves []Slave
		rs.cluster.UpdateSlaveStatus()
		rs.cluster.mutex.RLock()
		for _, v := range rs.cluster.slaves {
			if tagvalue != "" && v.Tag != tagvalue {
				continue
			}
			slaves = append(slaves, v)
		}
		rs.cluster.mutex.RUnlock()
		return c.JSON(http.StatusOK, slaves)
	}
}

// master API
// get /logkit/cluster/status?tag=tagvalue
func (rs *RestService) ClusterStatus() echo.HandlerFunc {
	return func(c echo.Context) error {
		if rs.cluster == nil || !rs.cluster.Enable {
			err := errors.New("cluster function not configed")
			return c.JSON(http.StatusBadRequest, err)
		}
		req := c.Request()
		if err := req.ParseForm(); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		tagvalue := req.Form.Get("tag")
		var slaves []Slave
		rs.cluster.mutex.RLock()
		for _, v := range rs.cluster.slaves {
			if tagvalue != "" && v.Tag != tagvalue {
				continue
			}
			slaves = append(slaves, v)
		}
		rs.cluster.mutex.RUnlock()

		allstatus := make(map[string]ClusterStatus)
		for _, v := range slaves {
			var cs ClusterStatus
			cs.Tag = v.Tag
			rss, err := QueryStatus(v.Url)
			if err != nil {
				cs.Err = err
			} else {
				cs.Status = rss
			}
			allstatus[v.Url] = cs
		}
		return c.JSON(http.StatusOK, allstatus)
	}
}

func QueryStatus(uri string) (rss map[string]RunnerStatus, err error) {
	resp, err := http.Get(uri + "/logkit/status")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	retb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	rss = make(map[string]RunnerStatus)
	if err = json.Unmarshal(retb, &rss); err != nil {
		err = fmt.Errorf("unmarshal query result error %v, body is %v", err, string(retb))
		return
	}
	return
}

type RegisterReq struct {
	Url string `json:"url"`
	Tag string `json:"tag"`
}

// master API
// POST /logkit/cluster/register
func (rs *RestService) PostRegister() echo.HandlerFunc {
	return func(c echo.Context) error {
		var req RegisterReq
		if err := c.Bind(&req); err != nil {
			return err
		}
		req.Url = utils.AddHttpProtocal(req.Url)
		if rs.cluster == nil || !rs.cluster.Enable {
			err := errors.New("this is not master")
			return c.JSON(http.StatusBadRequest, err)
		}
		rs.cluster.AddSlave(req.Url, req.Tag)
		return c.JSON(http.StatusOK, nil)
	}
}

type TagReq struct {
	Tag string `json:"tag"`
}

// slave API
// POST /logkit/cluster/tag
func (rs *RestService) PostTag() echo.HandlerFunc {
	return func(c echo.Context) error {
		var req TagReq
		if err := c.Bind(&req); err != nil {
			return err
		}
		if rs.cluster == nil || !rs.cluster.Enable {
			err := errors.New("cluster function not configed")
			return c.JSON(http.StatusBadRequest, err)
		}
		if err := Register(rs.cluster.MasterUrl, rs.cluster.myaddress, req.Tag); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		rs.cluster.mytag = req.Tag
		return c.JSON(http.StatusOK, nil)
	}
}

func Register(masters []string, myhost, tag string) error {
	var msg string
	hasSuccess := false
	for _, master := range masters {
		err := registerOne(master, myhost, tag)
		if err != nil {
			msg += "register " + master + " error " + err.Error()
		} else {
			hasSuccess = true
		}
	}
	if msg != "" {
		if hasSuccess {
			log.Error(msg)
			return nil
		}
		return errors.New(msg)
	}
	return nil
}

func registerOne(master, myhost, tag string) error {
	if master == "" {
		return errors.New("master host is not configed")
	}
	req := RegisterReq{Url: myhost, Tag: tag}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := http.Post(master+"/logkit/cluster/register", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	bd, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return errors.New(string(bd))
}
