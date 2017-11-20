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
	"strings"
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

type respRunnerStatus struct {
	Code string                  `json:"code"`
	Data map[string]RunnerStatus `json:"data"`
}

type respRunnerConfigs struct {
	Code string                  `json:"code"`
	Data map[string]RunnerConfig `json:"data"`
}

type respErrorMessage struct {
	Code string `json:"code"`
	Data string `json:"data"`
}

const (
	StatusOK   = "ok"
	StatusBad  = "bad"
	StatusLost = "lost"
)

const (
	DefaultMyTag    = "default"
	ContentType     = "Content-Type"
	ApplicationJson = "application/json"
)

func NewCluster(cc *ClusterConfig) *Cluster {
	cl := new(Cluster)
	cl.ClusterConfig = *cc
	cl.mytag = DefaultMyTag
	cl.slaves = make([]Slave, 0)
	cl.mutex = new(sync.RWMutex)
	return cl
}

func (cc *Cluster) RunRegisterLoop() error {
	if err := Register(cc.MasterUrl, cc.myaddress, cc.mytag); err != nil {
		return fmt.Errorf("master %v is unavaliable", cc.MasterUrl)
	}
	go func() {
		for {
			time.Sleep(15 * time.Second)
			if err := Register(cc.MasterUrl, cc.myaddress, cc.mytag); err != nil {
				log.Errorf("master %v is unavaliable", cc.MasterUrl)
			}
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
		return RespSuccess(c, nil)
	}
}

// master API
// get /logkit/cluster/slaves?tag=tagValue&url=urlValue
func (rs *RestService) Slaves() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterSlaves, err.Error())
		}
		rs.cluster.UpdateSlaveStatus()
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		return RespSuccess(c, slaves)
	}
}

// master API
// get /logkit/cluster/status?tag=tagValue&url=urlValue
func (rs *RestService) ClusterStatus() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterStatus, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		allstatus := make(map[string]ClusterStatus)
		for _, v := range slaves {
			var cs ClusterStatus
			cs.Tag = v.Tag
			var respRss respRunnerStatus
			url := fmt.Sprintf("%v/logkit/status", v.Url)
			respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
			if err != nil || respCode != http.StatusOK {
				errInfo := fmt.Errorf("%v %v", string(respBody), err)
				cs.Err = errInfo
			} else {
				if err = json.Unmarshal(respBody, &respRss); err != nil {
					cs.Err = fmt.Errorf("unmarshal query result error %v, body is %v", err, string(respBody))
				} else {
					cs.Status = respRss.Data
				}
			}
			allstatus[v.Url] = cs
		}
		return RespSuccess(c, allstatus)
	}
}

// master API
// Get /logkit/cluster/configs?tag=tagValue&url=urlValue
func (rs *RestService) GetClusterConfigs() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterConfigs, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		allConfigs := make(map[string]SlaveConfig)
		for _, v := range slaves {
			var sc SlaveConfig
			sc.Tag = v.Tag
			var respRss respRunnerConfigs
			url := fmt.Sprintf("%v/logkit/configs", v.Url)
			respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
			if err != nil || respCode != http.StatusOK {
				errInfo := fmt.Errorf("%v %v", string(respBody), err)
				sc.Err = errInfo
			} else {
				if err = json.Unmarshal(respBody, &respRss); err != nil {
					sc.Err = fmt.Errorf("unmarshal query result error %v, body is %v", err, string(respBody))
				} else {
					sc.Configs = respRss.Data
				}
			}
			allConfigs[v.Url] = sc
		}
		return RespSuccess(c, allConfigs)
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
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRegister, err.Error())
		}
		req.Url = utils.AddHttpProtocal(req.Url)
		if rs.cluster == nil || !rs.cluster.Enable {
			errMsg := "this is not master"
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRegister, errMsg)
		}
		rs.cluster.AddSlave(req.Url, req.Tag)
		return RespSuccess(c, nil)
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
			return RespError(c, http.StatusBadRequest, utils.ErrClusterTag, err.Error())
		}
		if rs.cluster == nil || !rs.cluster.Enable {
			errMsg := "cluster function not configed"
			return RespError(c, http.StatusBadRequest, utils.ErrClusterTag, errMsg)
		}
		if err := Register(rs.cluster.MasterUrl, rs.cluster.myaddress, req.Tag); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterTag, err.Error())
		}
		rs.cluster.mutex.Lock()
		rs.cluster.mytag = req.Tag
		rs.cluster.mutex.Unlock()
		return RespSuccess(c, nil)
	}
}

// POST /logkit/cluster/configs/<name>?tag=tagValue&url=urlValue
func (rs *RestService) PostClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		configName, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerAdd, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterRunnerAdd, errMsg)
		}
		method := http.MethodPost
		mgrType := "add runner " + configName
		urlPattern := "%v" + PREFIX + "/configs/" + configName
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterRunnerAdd, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// PUT /logkit/cluster/configs/<name>?tag=tagValue&url=urlValue
func (rs *RestService) PutClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		configName, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerUpdate, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterRunnerUpdate, errMsg)
		}
		method := http.MethodPut
		mgrType := "update runner " + configName
		urlPattern := "%v" + PREFIX + "/configs/" + configName
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterRunnerUpdate, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// DELETE /logkti/cluster/configs/<name>?tag=tagValue&url=urlValue
func (rs *RestService) DeleteClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		configName, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerDelete, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterRunnerDelete, errMsg)
		}
		method := http.MethodDelete
		mgrType := "delete runner " + configName
		urlPattern := "%v" + PREFIX + "/configs/" + configName
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterRunnerDelete, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/cluster/configs/<name>/stop?tag=tagValue&url=urlValue
func (rs *RestService) PostClusterConfigStop() echo.HandlerFunc {
	return func(c echo.Context) error {
		configName, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerStop, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterRunnerStop, errMsg)
		}
		method := http.MethodPost
		mgrType := "stop runner " + configName
		urlPattern := "%v" + PREFIX + "/configs/" + configName + "/stop"
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterRunnerStop, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/cluster/configs/<name>/start?tag=tagValue&url=urlValue
func (rs *RestService) PostClusterConfigStart() echo.HandlerFunc {
	return func(c echo.Context) error {
		configName, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerStart, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterRunnerStart, errMsg)
		}
		method := http.MethodPost
		mgrType := "start runner " + configName
		urlPattern := "%v" + PREFIX + "/configs/" + configName + "/start"
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterRunnerStart, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/cluster/configs/<name>/reset?tag=tagValue&url=urlValue
func (rs *RestService) PostClusterConfigReset() echo.HandlerFunc {
	return func(c echo.Context) error {
		configName, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerReset, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterRunnerReset, errMsg)
		}
		method := http.MethodPost
		mgrType := "reset runner " + configName
		urlPattern := "%v" + PREFIX + "/configs/" + configName + "/reset"
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterRunnerReset, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// DELETE /logkit/cluster/slaves?tag=tagValue&url=urlValue
func (rs *RestService) DeleteSlaves() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterSlavesDelete, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := make([]Slave, 0)
		for _, s := range rs.cluster.slaves {
			if ("" == tag || tag == s.Tag) && ("" == url || url == s.Url) {
				continue
			}
			slaves = append(slaves, s)
		}
		rs.cluster.mutex.RUnlock()
		rs.cluster.mutex.Lock()
		rs.cluster.slaves = slaves
		rs.cluster.mutex.Unlock()
		return RespSuccess(c, nil)
	}
}

// POST /logkit/cluster/slaves/tag?tag=tagValue&url=urlValue
func (rs *RestService) PostSlaveTag() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, configBytes, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterSlavesTag, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if len(slaves) == 0 {
			errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') are not found"
			return RespError(c, http.StatusNotFound, utils.ErrClusterSlavesTag, errMsg)
		}
		mgrType := "change tag"
		method := http.MethodPost
		urlPattern := "%v" + PREFIX + "/cluster/tag"
		if err := executeToClusters(slaves, urlPattern, method, mgrType, configBytes); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterSlavesTag, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

func getQualifySlaves(slaves []Slave, tag, url string) []Slave {
	slave := make([]Slave, 0)
	//(u == "" && t == "") || (u == "" && s.t == t) || (u == s.u && t == "") || (u == s.u && t == s.t)
	for _, s := range slaves {
		if (url == "" || url == s.Url) && (tag == "" || tag == s.Tag) {
			slave = append(slave, s)
		}
	}
	return slave
}

func (rs *RestService) checkClusterRequest(c echo.Context) (name, tag, url string, configBytes []byte, err error) {
	if rs.cluster == nil || !rs.cluster.Enable {
		err = errors.New("cluster function not configed")
		return
	}
	var tmpErr error
	req := c.Request()
	if tmpErr := req.ParseForm(); tmpErr != nil {
		err = errors.New("can not get form from request, err is " + tmpErr.Error())
		return
	}
	if configBytes, tmpErr = ioutil.ReadAll(req.Body); tmpErr != nil {
		err = errors.New("get response body failed, err is " + tmpErr.Error())
		return
	}
	name = c.Param("name")
	tag = req.Form.Get("tag")
	url = req.Form.Get("url")
	return
}

func executeToClusters(slaves []Slave, urlP, method, mgr string, reqBd []byte) (err error) {
	errInfo := make([]string, 0)
	for _, v := range slaves {
		url := fmt.Sprintf(urlP, v.Url)
		respCode, respBody, err := executeToOneCluster(url, method, reqBd)
		if respCode != http.StatusOK || err != nil {
			log.Errorf("url %v %v occurred an error, resp is %v, err is %v", v.Url, mgr, string(respBody), err)
			errMsg := fmt.Sprintf("url %v %v occurred an error, resp is %v, err is %v", v.Url, mgr, string(respBody), err)
			errInfo = append(errInfo, errMsg)
		}
	}
	if len(errInfo) == 0 {
		return nil
	}
	return errors.New(strings.Join(errInfo, "\n"))
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
