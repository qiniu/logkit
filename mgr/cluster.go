package mgr

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils"
)

type ClusterConfig struct {
	MasterUrl []string `json:"master_url"`
	IsMaster  bool     `json:"is_master"`
	Enable    bool     `json:"enable"`
	Address   string   `json:"address"`
	Tag       string   `json:"tag"`
}

type Cluster struct {
	ClusterConfig
	slaves       []Slave
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
	Err    string                  `json:"error"`
}

type SlaveConfig struct {
	Configs map[string]RunnerConfig `json:"configs"`
	Tag     string                  `json:"tag"`
	Err     string                  `json:"error"`
}

type respRunnersNameList struct {
	Code string   `json:"code"`
	Data []string `json:"data"`
}

type respRunnerStatus struct {
	Code string                  `json:"code"`
	Data map[string]RunnerStatus `json:"data"`
}

type respRunnerConfig struct {
	Code string       `json:"code"`
	Data RunnerConfig `json:"data"`
}

type respRunnerConfigs struct {
	Code string                  `json:"code"`
	Data map[string]RunnerConfig `json:"data"`
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
	if cl.Tag == "" {
		cl.Tag = DefaultMyTag
	}
	cl.slaves = make([]Slave, 0)
	cl.mutex = new(sync.RWMutex)
	return cl
}

func (cc *Cluster) RunRegisterLoop() error {
	if err := Register(cc.MasterUrl, cc.Address, cc.Tag); err != nil {
		return fmt.Errorf("master %v is unavaliable", cc.MasterUrl)
	}
	go func() {
		for {
			time.Sleep(15 * time.Second)
			if err := Register(cc.MasterUrl, cc.Address, cc.Tag); err != nil {
				log.Errorf("master %v is unavaliable", cc.MasterUrl)
			}
		}
	}()
	return nil
}

func (cc *Cluster) AddSlave(url, tag string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for idx, v := range cc.slaves {
		if v.Url == url {
			v.Tag = tag
			v.LastTouch = time.Now()
			v.Status = StatusOK
			cc.slaves[idx] = v
			return
		}
	}
	cc.slaves = append(cc.slaves, Slave{url, tag, StatusOK, time.Now()})
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
// GET /logkit/cluster/ping
func (rs *RestService) Ping() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, nil)
	}
}

// master API
// GET /logkit/cluster/ismaster
func (rs *RestService) IsMaster() echo.HandlerFunc {
	return func(c echo.Context) error {
		isMaster := true
		if rs.cluster == nil || !rs.cluster.Enable || !rs.cluster.IsMaster {
			isMaster = false
		}
		return RespSuccess(c, isMaster)
	}
}

// master API
// GET /logkit/cluster/slaves?tag=tagValue&url=urlValue
func (rs *RestService) Slaves() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterSlaves, err.Error())
		}
		rs.cluster.UpdateSlaveStatus()
		rs.cluster.mutex.RLock()
		slaves, _ := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		return RespSuccess(c, slaves)
	}
}

// master API
// GET /logkit/cluster/runners?tag=tagValue&url=urlValue
func (rs *RestService) GetClusterRunners() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterSlaves, err.Error())
		}
		rs.cluster.UpdateSlaveStatus()
		rs.cluster.mutex.RLock()
		slaves, _ := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		mutex := new(sync.Mutex)
		wg := new(sync.WaitGroup)
		runnerNameSet := utils.NewHashSet()
		for _, v := range slaves {
			wg.Add(1)
			go func(v Slave) {
				defer wg.Done()
				var respRss respRunnersNameList
				url := fmt.Sprintf("%v/logkit/runners", v.Url)
				respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
				if err != nil || respCode != http.StatusOK {
					log.Errorf("get slave(tag='%v', url='%v') runner name list failed, resp is %v, error is %v", v.Tag, v.Url, string(respBody), err.Error())
					return
				} else {
					if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
						log.Errorf("unmarshal slave(tag='%v', url='%v') runner name list failed, error is %v", v.Tag, v.Url, err.Error())
					} else {
						mutex.Lock()
						runnerNameSet.AddStringArray(respRss.Data)
						mutex.Unlock()
					}
				}

			}(v)
		}
		wg.Wait()
		return RespSuccess(c, runnerNameSet.Elements())
	}
}

// master API
// GET /logkit/cluster/status?tag=tagValue&url=urlValue
func (rs *RestService) ClusterStatus() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterStatus, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves, _ := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		mutex := new(sync.Mutex)
		wg := new(sync.WaitGroup)
		allStatus := make(map[string]ClusterStatus)
		for _, v := range slaves {
			wg.Add(1)
			go func(v Slave) {
				defer wg.Done()
				var cs ClusterStatus
				cs.Tag = v.Tag
				var respRss respRunnerStatus
				if v.Status != StatusOK {
					cs.Status = map[string]RunnerStatus{}
					mutex.Lock()
					allStatus[v.Url] = cs
					mutex.Unlock()
					return
				}
				url := fmt.Sprintf("%v/logkit/status", v.Url)
				respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
				if err != nil || respCode != http.StatusOK {
					errInfo := fmt.Errorf("%v %v", string(respBody), err)
					cs.Err = errInfo.Error()
				} else {
					if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
						cs.Err = fmt.Sprintf("unmarshal query result error %v, body is %v", err, string(respBody))
					} else {
						cs.Status = respRss.Data
					}
				}
				mutex.Lock()
				allStatus[v.Url] = cs
				mutex.Unlock()
			}(v)
		}
		wg.Wait()
		return RespSuccess(c, allStatus)
	}
}

// master API
// Get /logkit/cluster/configs:name?tag=tagValue&url=urlValue
func (rs *RestService) GetClusterConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		runnerName, tag, url, _, err := rs.checkClusterRequest(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterConfig, err.Error())
		}
		rs.cluster.mutex.RLock()
		slaves, _ := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		var config RunnerConfig
		var lastErrMsg string
		for _, v := range slaves {
			var respRss respRunnerConfig
			if v.Status != StatusOK {
				lastErrMsg = "the slaves(tag = '" + tag + "', url = '" + url + "') status is " + v.Status
				continue
			}
			url := fmt.Sprintf("%v/logkit/configs/"+runnerName, v.Url)
			respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
			if err != nil || respCode != http.StatusOK {
				lastErrMsg = fmt.Sprintf("get slave(tag = '%v'', url = '%v') config failed resp is %v, error is %v", tag, url, string(respBody), err)
				continue
			} else {
				if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
					lastErrMsg = fmt.Sprintf("get slave(tag = '%v'', url = '%v') config unmarshal failed, resp is %v, error is %v", tag, url, string(respBody), err)
					continue
				} else {
					config = respRss.Data
					return RespSuccess(c, config)
				}
			}
		}
		return RespError(c, http.StatusBadRequest, utils.ErrClusterConfig, lastErrMsg)
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
		slaves, _ := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		mutex := new(sync.Mutex)
		wg := new(sync.WaitGroup)
		allConfigs := make(map[string]SlaveConfig)
		for _, v := range slaves {
			wg.Add(1)
			go func(v Slave) {
				defer wg.Done()
				var sc SlaveConfig
				sc.Tag = v.Tag
				var respRss respRunnerConfigs
				if v.Status != StatusOK {
					sc.Configs = map[string]RunnerConfig{}
					mutex.Lock()
					allConfigs[v.Url] = sc
					mutex.Unlock()
					return
				}
				url := fmt.Sprintf("%v/logkit/configs", v.Url)
				respCode, respBody, err := executeToOneCluster(url, http.MethodGet, []byte{})
				if err != nil || respCode != http.StatusOK {
					errInfo := fmt.Errorf("%v %v", string(respBody), err)
					sc.Err = errInfo.Error()
				} else {
					if err = jsoniter.Unmarshal(respBody, &respRss); err != nil {
						sc.Err = fmt.Sprintf("unmarshal query result error %v, body is %v", err, string(respBody))
					} else {
						sc.Configs = respRss.Data
					}
				}
				mutex.Lock()
				allConfigs[v.Url] = sc
				mutex.Unlock()
			}(v)
		}
		wg.Wait()
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
		if err := Register(rs.cluster.MasterUrl, rs.cluster.Address, req.Tag); err != nil {
			return RespError(c, http.StatusServiceUnavailable, utils.ErrClusterTag, err.Error())
		}
		rs.cluster.mutex.Lock()
		rs.cluster.Tag = req.Tag
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerAdd, err.Error())
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerUpdate, err.Error())
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerDelete, err.Error())
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerStop, err.Error())
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerStart, err.Error())
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterRunnerReset, err.Error())
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
		slaves, err := getQualifySlaves(rs.cluster.slaves, tag, url)
		rs.cluster.mutex.RUnlock()
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrClusterSlavesTag, err.Error())
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

func getQualifySlaves(slaves []Slave, tag, url string) ([]Slave, error) {
	errInfo := make([]string, 0)
	slave := make([]Slave, 0)
	//(u == "" && t == "") || (u == "" && s.t == t) || (u == s.u && t == "") || (u == s.u && t == s.t)
	for _, s := range slaves {
		if (url == "" || url == s.Url) && (tag == "" || tag == s.Tag) {
			if s.Status != StatusOK {
				errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') status is " + s.Status + ", options are terminated"
				errInfo = append(errInfo, errMsg)
			}
			slave = append(slave, s)
		}
	}
	// 没有找到 slave 的错误只有当没有其他错误的时候才报
	if len(slave)+len(errInfo) <= 0 {
		errMsg := "the slaves(tag = '" + tag + "', url = '" + url + "') is not found"
		errInfo = append(errInfo, errMsg)
	}
	if len(errInfo) > 0 {
		return slave, errors.New(strings.Join(errInfo, "\n"))
	}
	return slave, nil
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
	mutex := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	errInfo := make([]string, 0)
	for _, v := range slaves {
		wg.Add(1)
		url := fmt.Sprintf(urlP, v.Url)
		go func(url string) {
			defer wg.Done()
			respCode, respBody, err := executeToOneCluster(url, method, reqBd)
			if respCode != http.StatusOK || err != nil {
				log.Errorf("url %v %v occurred an error, resp is %v, err is %v", v.Url, mgr, string(respBody), err)
				errMsg := fmt.Sprintf("url %v %v occurred an error, resp is %v, err is %v", v.Url, mgr, string(respBody), err)
				mutex.Lock()
				errInfo = append(errInfo, errMsg)
				mutex.Unlock()
			}
		}(url)
	}
	wg.Wait()
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
	data, err := jsoniter.Marshal(req)
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
