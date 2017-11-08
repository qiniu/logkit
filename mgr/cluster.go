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

type SlaveConfig struct {
	Configs map[string]RunnerConfig `json:"configs"`
	Tag     string                  `json:"tag"`
	Err     error                   `json:"error"`
}

const (
	StatusOK   = "ok"
	StatusBad  = "bad"
	StatusLost = "lost"
)

const (
	ContentType     = "Content-Type"
	ApplicationJson = "application/json"
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
			rss := make(map[string]RunnerStatus)
			url := fmt.Sprintf("%v/logkit/status", v.Url)
			respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
			if err != nil || respCode != http.StatusOK {
				errInfo := fmt.Errorf("%v %v", string(respBody), err)
				cs.Err = errInfo
			} else {
				if err = json.Unmarshal(respBody, &rss); err != nil {
					cs.Err = fmt.Errorf("unmarshal query result error %v, body is %v", err, string(respBody))
				} else {
					cs.Status = rss
				}
			}
			allstatus[v.Url] = cs
		}
		return c.JSON(http.StatusOK, allstatus)
	}
}

// master API
// Get /logkit/cluster/configs?tag=tagValue
func (rs *RestService) GetClusterConfigs() echo.HandlerFunc {
	return func(c echo.Context) error {
		if rs.cluster == nil || !rs.cluster.Enable {
			err := errors.New("cluster function not configed")
			return c.JSON(http.StatusBadRequest, err)
		}
		req := c.Request()
		if err := req.ParseForm(); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		tagValue := req.Form.Get("tag")
		var slaves []Slave
		rs.cluster.mutex.RLock()
		for _, v := range rs.cluster.slaves {
			if v.Tag == tagValue || tagValue == "" {
				slaves = append(slaves, v)
			}
		}
		rs.cluster.mutex.RUnlock()

		allConfigs := make(map[string]SlaveConfig)
		for _, v := range slaves {
			var sc SlaveConfig
			sc.Tag = v.Tag
			rss := make(map[string]RunnerConfig)
			url := fmt.Sprintf("%v/logkit/configs", v.Url)
			respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
			if err != nil || respCode != http.StatusOK {
				errInfo := fmt.Errorf("%v %v", string(respBody), err)
				sc.Err = errInfo
			} else {
				if err = json.Unmarshal(respBody, &rss); err != nil {
					sc.Err = fmt.Errorf("unmarshal query result error %v, body is %v", err, string(respBody))
				} else {
					sc.Configs = rss
				}
			}
			allConfigs[v.Url] = sc
		}
		return c.JSON(http.StatusOK, allConfigs)
	}
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

// POST /logkit/cluster/configs/<name>?tag=tagValue
func (rs *RestService) PostClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, tag, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		mgrType := "add"
		rs.cluster.mutex.RLock()
		defer rs.cluster.mutex.RUnlock()
		if err := executeToClusters(rs.cluster.slaves, name, tag, configBytes, mgrType); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		return c.JSON(http.StatusOK, nil)
	}
}

// PUT /logkit/cluster/configs/<name>?tag=tagValue
func (rs *RestService) PutClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, tag, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		mgrType := "update"
		rs.cluster.mutex.RLock()
		defer rs.cluster.mutex.RUnlock()
		if err := executeToClusters(rs.cluster.slaves, name, tag, configBytes, mgrType); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		return c.JSON(http.StatusOK, nil)
	}
}

// DELETE /logkti/cluster/configs/<name>?tag=tagValue
func (rs *RestService) DeleteClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, tag, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		mgrType := "delete"
		rs.cluster.mutex.RLock()
		defer rs.cluster.mutex.RUnlock()
		if err := executeToClusters(rs.cluster.slaves, name, tag, configBytes, mgrType); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		return c.JSON(http.StatusOK, nil)
	}
}

// POST /logkit/cluster/configs/<name>/stop?tag=tagValue
func (rs *RestService) PostClusterConfigStop() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, tag, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		mgrType := "stop"
		rs.cluster.mutex.RLock()
		defer rs.cluster.mutex.RUnlock()
		if err := executeToClusters(rs.cluster.slaves, name, tag, configBytes, mgrType); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		return c.JSON(http.StatusOK, nil)
	}
}

// POST /logkit/cluster/configs/<name>/start?tag=tagValue
func (rs *RestService) PostClusterConfigStart() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, tag, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		mgrType := "start"
		rs.cluster.mutex.RLock()
		defer rs.cluster.mutex.RUnlock()
		if err := executeToClusters(rs.cluster.slaves, name, tag, configBytes, mgrType); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		return c.JSON(http.StatusOK, nil)
	}
}

// POST /logkit/cluster/configs/<name>/reset?tag=tagValue
func (rs *RestService) PostClusterConfigReset() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, tag, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		mgrType := "reset"
		rs.cluster.mutex.RLock()
		defer rs.cluster.mutex.RUnlock()
		if err := executeToClusters(rs.cluster.slaves, name, tag, configBytes, mgrType); err != nil {
			return c.JSON(http.StatusServiceUnavailable, err)
		}
		return c.JSON(http.StatusOK, nil)
	}
}

func (rs *RestService) checkClusterRequest(c echo.Context) (name, tag string, configBytes []byte, err interface{}) {
	if rs.cluster == nil || !rs.cluster.Enable {
		err = map[string]string{"error": "cluster function not configed"}
		return
	}
	name = c.Param("name")
	if name == "" {
		err = map[string]string{"error": "name is empty"}
		return
	}
	var tmpErr error
	req := c.Request()
	if tmpErr := req.ParseForm(); tmpErr != nil {
		err = map[string]string{"error": "can not get form " + tmpErr.Error()}
		return
	}
	if configBytes, tmpErr = ioutil.ReadAll(req.Body); tmpErr != nil {
		err = map[string]string{"error": "get response body failed " + tmpErr.Error()}
		return
	}
	tag = req.Form.Get("tag")
	return
}

func executeToClusters(slaves []Slave, runnerName, tag string, configBytes []byte, mgrType string) map[string]interface{} {
	urlStr := "%v" + PREFIX + "/configs/%v"
	if mgrType == "start" || mgrType == "stop" || mgrType == "reset" {
		urlStr = urlStr + "/" + mgrType
	}
	method := http.MethodPost
	if mgrType == "update" {
		method = http.MethodPut
	}
	if mgrType == "delete" {
		method = http.MethodDelete
	}
	errInfo := make(map[string]interface{})
	for _, v := range slaves {
		if v.Tag == tag || v.Tag == "" {
			url := fmt.Sprintf(urlStr, v.Url, runnerName)
			respCode, respBody, err := executeToOneCluster(url, method, configBytes)
			if respCode != http.StatusOK || err != nil {
				log.Errorf("url %v %v runner %v failed, %v", v.Url, mgrType, runnerName, err)
				errorInfo := fmt.Sprintf("%v %v", string(respBody), err)
				errInfo[v.Url] = map[string]string{
					"url":        v.Url,
					"tag":        v.Tag,
					"mgrType":    mgrType,
					"runnerName": runnerName,
					"error":      errorInfo,
				}
			}
		}
	}
	if len(errInfo) > 0 {
		return errInfo
	}
	return nil
}

func executeToOneCluster(url, method string, configBytes []byte) (respCode int, respBody []byte, err error) {
	config := bytes.NewReader(configBytes)
	req, err := http.NewRequest(method, url, config)
	if err != nil {
		return
	}
	req.Header.Set(ContentType, ApplicationJson)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	respCode = resp.StatusCode
	respBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return
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
	resp, err := http.Post(master+"/logkit/cluster/register", ApplicationJson, bytes.NewReader(data))
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
