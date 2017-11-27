package mgr

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sync"

	"errors"
	"github.com/labstack/echo"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils"
)

var DEFAULT_PORT = 3000

const (
	StatsShell = "stats"
	PREFIX     = "/logkit"
)

type RestService struct {
	mgr     *Manager
	l       net.Listener
	cluster *Cluster
	address string
}

func NewRestService(mgr *Manager, router *echo.Echo) *RestService {

	if mgr.Cluster.Enable && !mgr.Cluster.IsMaster {
		if len(mgr.Cluster.MasterUrl) < 1 {
			log.Fatalf("cluster is enabled but master url is empty")
		}
		for i := range mgr.Cluster.MasterUrl {
			if !strings.HasPrefix(mgr.Cluster.MasterUrl[i], "http://") {
				mgr.Cluster.MasterUrl[i] = "http://" + mgr.Cluster.MasterUrl[i]
			}
		}
	}

	rs := &RestService{
		mgr:     mgr,
		cluster: NewCluster(&mgr.Cluster),
	}
	rs.cluster.mutex = new(sync.RWMutex)
	router.GET(PREFIX+"/status", rs.Status())

	// error code humanize
	router.GET(PREFIX+"/errorcode", rs.GetErrorCodeHumanize())

	//configs API
	router.GET(PREFIX+"/configs", rs.GetConfigs())
	router.GET(PREFIX+"/configs/:name", rs.GetConfig())
	router.POST(PREFIX+"/configs/:name", rs.PostConfig())
	router.POST(PREFIX+"/configs/:name/stop", rs.PostConfigStop())
	router.POST(PREFIX+"/configs/:name/start", rs.PostConfigStart())
	router.POST(PREFIX+"/configs/:name/reset", rs.PostConfigReset())
	router.PUT(PREFIX+"/configs/:name", rs.PutConfig())
	router.DELETE(PREFIX+"/configs/:name", rs.DeleteConfig())

	// runners API
	router.GET(PREFIX+"/runners", rs.GetRunners())

	//reader API
	router.GET(PREFIX+"/reader/usages", rs.GetReaderUsages())
	router.GET(PREFIX+"/reader/options", rs.GetReaderKeyOptions())
	router.POST(PREFIX+"/reader/check", rs.PostReaderCheck())

	//parser API
	router.GET(PREFIX+"/parser/usages", rs.GetParserUsages())
	router.GET(PREFIX+"/parser/options", rs.GetParserKeyOptions())
	router.POST(PREFIX+"/parser/parse", rs.PostParse())
	router.GET(PREFIX+"/parser/samplelogs", rs.GetParserSampleLogs())
	router.POST(PREFIX+"/parser/check", rs.PostParserCheck())

	//sender API
	router.GET(PREFIX+"/sender/usages", rs.GetSenderUsages())
	router.GET(PREFIX+"/sender/options", rs.GetSenderKeyOptions())
	router.POST(PREFIX+"/sender/check", rs.PostSenderCheck())

	//transformer API
	router.GET(PREFIX+"/transformer/usages", rs.GetTransformerUsages())
	router.GET(PREFIX+"/transformer/options", rs.GetTransformerOptions())
	router.GET(PREFIX+"/transformer/sampleconfigs", rs.GetTransformerSampleConfigs())
	router.POST(PREFIX+"/transformer/transform", rs.PostTransform())

	//metric API
	router.GET(PREFIX+"/metric/keys", rs.GetMetricKeys())
	router.GET(PREFIX+"/metric/usages", rs.GetMetricUsages())
	router.GET(PREFIX+"/metric/options", rs.GetMetricOptions())

	//version
	router.GET(PREFIX+"/version", rs.GetVersion())

	//cluster API
	router.GET(PREFIX+"/cluster/ping", rs.Ping())
	router.GET(PREFIX+"/cluster/ismaster", rs.IsMaster())
	router.POST(PREFIX+"/cluster/register", rs.PostRegister())
	router.POST(PREFIX+"/cluster/tag", rs.PostTag())
	router.GET(PREFIX+"/cluster/slaves", rs.Slaves())
	router.DELETE(PREFIX+"/cluster/slaves", rs.DeleteSlaves())
	router.POST(PREFIX+"/cluster/slaves/tag", rs.PostSlaveTag())
	router.GET(PREFIX+"/cluster/status", rs.ClusterStatus())
	router.GET(PREFIX+"/cluster/runners", rs.GetClusterRunners())
	router.GET(PREFIX+"/cluster/configs", rs.GetClusterConfigs())
	router.POST(PREFIX+"/cluster/configs/:name", rs.PostClusterConfig())
	router.PUT(PREFIX+"/cluster/configs/:name", rs.PutClusterConfig())
	router.DELETE(PREFIX+"/cluster/configs/:name", rs.DeleteClusterConfig())
	router.POST(PREFIX+"/cluster/configs/:name/stop", rs.PostClusterConfigStop())
	router.POST(PREFIX+"/cluster/configs/:name/start", rs.PostClusterConfigStart())
	router.POST(PREFIX+"/cluster/configs/:name/reset", rs.PostClusterConfigReset())

	var (
		port       = DEFAULT_PORT
		address    string
		listener   net.Listener
		err        error
		httpschema = "http://"
	)

	for {
		if port > 10000 {
			log.Fatal("bind port failed too many times, exit...")
		}
		address = ":" + strconv.Itoa(port)
		if mgr.BindHost != "" {
			address, httpschema = utils.RemoveHttpProtocal(mgr.BindHost)
		}
		listener, err = httpserve(address, router)
		if err != nil {
			err = fmt.Errorf("bind address %v for RestService error %v", address, err)
			if mgr.BindHost != "" {
				log.Fatal(err)
			} else {
				log.Warnf("%v, try next port", err)
			}
			port++
			continue
		}
		break
	}
	rs.l = listener
	log.Infof("successfully start RestService and bind address on %v", address)
	err = generateStatsShell(address, PREFIX)
	if err != nil {
		log.Warn(err)
	}
	rs.address = address
	if rs.cluster.Enable {
		rs.cluster.myaddress, err = GetMySlaveUrl(address, httpschema)
		if err != nil {
			log.Fatalf("get slave url bindaddress[%v] error %v", address, err)
		}
	}
	return rs
}

func GetMySlaveUrl(address, schema string) (uri string, err error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return
	}
	if host == "" {
		host, err = utils.GetLocalIP()
		if err != nil {
			return
		}
	}
	return schema + host + ":" + port, nil
}

func generateStatsShell(address, prefix string) (err error) {
	if strings.HasPrefix(address, ":") {
		address = fmt.Sprintf("127.0.0.1%v", address)
	}
	sh := fmt.Sprintf("#!/bin/bash\ncurl %v%v/status", address, prefix)
	err = ioutil.WriteFile(StatsShell, []byte(sh), 0666)
	if err != nil {
		err = fmt.Errorf("writefile error %v, address: 127.0.0.1%v%v/status", err, address, prefix)
		return
	}
	err = os.Chmod(StatsShell, 0755)
	if err != nil {
		err = fmt.Errorf("change mode for %v error %v", StatsShell, err)
		return
	}
	return
}

func RespError(c echo.Context, respCode int, errCode, errMsg string) error {
	errInfo := map[string]string{
		"code":    errCode,
		"message": errMsg,
	}
	return c.JSON(respCode, errInfo)
}

func RespSuccess(c echo.Context, data interface{}) error {
	respData := map[string]interface{}{
		"code": utils.ErrNothing,
	}
	if data != nil {
		respData["data"] = data
	}
	return c.JSON(http.StatusOK, respData)
}

// get /logkit/status
func (rs *RestService) Status() echo.HandlerFunc {
	return func(c echo.Context) error {
		rss := rs.mgr.Status()
		if rs.cluster.Enable {
			for k, v := range rss {
				v.Tag = rs.cluster.mytag
				v.Url = rs.cluster.myaddress
				rss[k] = v
			}
		}
		return RespSuccess(c, rss)
	}
}

// get /logkit/runners
func (rs *RestService) GetRunners() echo.HandlerFunc {
	return func(c echo.Context) error {
		runnerNameList := make([]string, 0)
		for _, conf := range rs.mgr.runnerConfig {
			runnerNameList = append(runnerNameList, conf.RunnerName)
		}
		return RespSuccess(c, runnerNameList)
	}
}

// get /logkit/configs
func (rs *RestService) GetConfigs() echo.HandlerFunc {
	return func(c echo.Context) error {
		rs.mgr.lock.RLock()
		defer rs.mgr.lock.RUnlock()
		rss := make(map[string]RunnerConfig)
		for k, v := range rs.mgr.runnerConfig {
			if filepath.Dir(k) == rs.mgr.RestDir {
				v.IsInWebFolder = true
			}
			rss[k] = v
		}
		return RespSuccess(c, rss)
	}
}

// get /logkit/configs/:name
func (rs *RestService) GetConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, runnerConfig, _, err := rs.checkNameAndConfig(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrConfigName, err.Error())
		}
		return RespSuccess(c, runnerConfig)
	}
}

func convertWebParserConfig(conf conf.MapConf) conf.MapConf {
	if conf == nil {
		return conf
	}
	rawCustomPatterns, _ := conf.GetStringOr(parser.KeyGrokCustomPatterns, "")
	if rawCustomPatterns != "" {
		CustomPatterns, err := base64.StdEncoding.DecodeString(rawCustomPatterns)
		if err != nil {
			return conf
		}
		conf[parser.KeyGrokCustomPatterns] = string(CustomPatterns)
	}
	return conf
}

func convertWebTransformerConfig(conf map[string]interface{}) map[string]interface{} {
	if conf == nil {
		return conf
	}
	//TODO do some pre process
	return conf
}

func (rs *RestService) backupRunnerConfig(rconf interface{}, filename string) error {
	confBytes, err := json.MarshalIndent(rconf, "", "    ")
	if err != nil {
		log.Warnf("runner config %v marshal failed, err is %v", rconf, err)
		return nil
	}
	// 判断默认备份文件夹是否存在，不存在就尝试创建
	if _, err := os.Stat(rs.mgr.RestDir); err != nil {
		if os.IsNotExist(err) {
			if err = os.Mkdir(rs.mgr.RestDir, 0755); err != nil && !os.IsExist(err) {
				log.Warnf("rest default dir not exists and make dir failed, err is %v", err)
				return nil
			}
		}
	}
	err = ioutil.WriteFile(filename, confBytes, 0644)
	if err != nil {
		log.Warnf("backup runner config %v failed, err is %v", filename, err)
	}
	return nil
}

func (rs *RestService) checkNameAndConfig(c echo.Context) (name string, conf RunnerConfig, file string, err error) {
	name = c.Param("name")
	if name == "" {
		err = errors.New("config name is empty")
		return
	}
	var exist bool
	rs.mgr.lock.RLock()
	defer rs.mgr.lock.RUnlock()
	file = rs.mgr.RestDir + "/" + name + ".conf"
	if conf, exist = rs.mgr.runnerConfig[file]; !exist {
		err = errors.New("config " + name + " is not found")
		return
	}
	return
}

// post /logkit/configs/<name>
func (rs *RestService) PostConfig() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		name := c.Param("name")
		if name == "" {
			errMsg := "runner name is empty"
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerAdd, errMsg)
		}

		var nconf RunnerConfig
		if err = c.Bind(&nconf); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerAdd, err.Error())
		}
		nconf.CreateTime = time.Now().Format(time.RFC3339Nano)
		nconf.RunnerName = name
		filename := rs.mgr.RestDir + "/" + nconf.RunnerName + ".conf"
		if rs.mgr.isRunning(filename) {
			errMsg := "file " + filename + " runner is running"
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerAdd, errMsg)
		}
		nconf.ParserConf = convertWebParserConfig(nconf.ParserConf)
		nconf.IsInWebFolder = true
		if err = rs.mgr.ForkRunner(filename, nconf, true); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerAdd, err.Error())
		}
		if err := rs.backupRunnerConfig(nconf, filename); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerAdd, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// put /logkit/configs/<name>
func (rs *RestService) PutConfig() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		name := c.Param("name")
		if name == "" {
			errMsg := "config name is empty"
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerUpdate, errMsg)
		}

		var nconf RunnerConfig
		if err = c.Bind(&nconf); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerUpdate, err.Error())
		}
		nconf.CreateTime = time.Now().Format(time.RFC3339Nano)
		nconf.RunnerName = name
		filename := rs.mgr.RestDir + "/" + nconf.RunnerName + ".conf"
		if rs.mgr.isRunning(filename) {
			if subErr := rs.mgr.Remove(filename); subErr != nil {
				log.Errorf("remove runner %v error %v", filename, subErr)
			}
			os.Remove(filename)
		}
		nconf.ParserConf = convertWebParserConfig(nconf.ParserConf)
		nconf.IsInWebFolder = true
		if err = rs.mgr.ForkRunner(filename, nconf, true); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerUpdate, err.Error())
		}
		if err = rs.backupRunnerConfig(nconf, filename); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerUpdate, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/configs/<name>/reset
func (rs *RestService) PostConfigReset() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, runnerConfig, filename, err := rs.checkNameAndConfig(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerReset, err.Error())
		}
		if runnerConfig.IsStopped {
			runnerConfig.IsStopped = false
			err = rs.mgr.ForkRunner(filename, runnerConfig, true)
			if err != nil {
				errMsg := "runner " + name + " reset failed " + err.Error()
				return RespError(c, http.StatusBadRequest, utils.ErrRunnerReset, errMsg)
			}
		}
		runner, runnerOk := rs.mgr.runners[filename]
		if !runnerOk {
			errMsg := "runner " + name + " is not found"
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerReset, errMsg)
		}
		if subErr := rs.mgr.Remove(filename); subErr != nil {
			log.Errorf("remove runner %v error %v", filename, subErr)
		}
		runnerConfig.CreateTime = time.Now().Format(time.RFC3339Nano)
		os.Remove(filename)

		if runnerReset, ok := runner.(Resetable); ok {
			err = runnerReset.Reset()
		}
		if err = rs.mgr.ForkRunner(filename, runnerConfig, true); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerReset, err.Error())
		}
		if err = rs.backupRunnerConfig(runnerConfig, filename); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerReset, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/configs/<name>/start
func (rs *RestService) PostConfigStart() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, conf, filename, err := rs.checkNameAndConfig(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStart, err.Error())
		}
		conf.IsStopped = false
		if err = rs.mgr.ForkRunner(filename, conf, true); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStart, err.Error())
		}
		if err = rs.backupRunnerConfig(conf, filename); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStart, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/configs/<name>/stop
func (rs *RestService) PostConfigStop() echo.HandlerFunc {
	return func(c echo.Context) error {
		name, runnerConfig, filename, err := rs.checkNameAndConfig(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStop, err.Error())
		}
		if !rs.mgr.isRunning(filename) {
			errMsg := "the runner " + name + " is not running"
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStop, errMsg)
		}
		if err = rs.mgr.RemoveWithConfig(filename, false); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStop, err.Error())
		}
		runnerConfig.IsStopped = true
		rs.mgr.lock.Lock()
		rs.mgr.runnerConfig[filename] = runnerConfig
		rs.mgr.lock.Unlock()
		if err = rs.backupRunnerConfig(runnerConfig, filename); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerStop, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// delete /logkit/configs/<name>
func (rs *RestService) DeleteConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, runnerConfig, filename, err := rs.checkNameAndConfig(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerDelete, err.Error())
		}
		if runnerConfig.IsStopped {
			rs.mgr.lock.Lock()
			delete(rs.mgr.runnerConfig, filename)
			rs.mgr.lock.Unlock()
		} else {
			if err := rs.mgr.Remove(filename); err != nil {
				return RespError(c, http.StatusBadRequest, utils.ErrRunnerDelete, err.Error())
			}
		}
		if err = os.Remove(filename); err != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrRunnerDelete, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// get /logkit/errorcode
func (rs *RestService) GetErrorCodeHumanize() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, utils.ErrorCodeHumanize)
	}
}

type Version struct {
	Version string `json:"version"`
}

func (rs *RestService) GetVersion() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, &Version{Version: rs.mgr.Version})
	}
}

func (rs *RestService) Register() error {
	if rs.cluster.Enable {
		return rs.cluster.RunRegisterLoop()
	}
	return nil
}

// Stop will stop RestService
func (rs *RestService) Stop() {
	rs.l.Close()
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

func httpserve(addr string, mux http.Handler) (listener net.Listener, err error) {
	if addr == "" {
		addr = ":http"
	}
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		return
	}

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Error(srv.Serve(tcpKeepAliveListener{listener.(*net.TCPListener)}))
	}()
	return
}
