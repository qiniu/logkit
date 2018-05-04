package mgr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"

	"github.com/labstack/echo"
	"github.com/qiniu/log"
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

	if mgr.Cluster.Enable {
		if !mgr.Cluster.IsMaster && len(mgr.Cluster.MasterUrl) < 1 {
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
	router.POST(PREFIX+"/reader/read", rs.PostRead())
	router.POST(PREFIX+"/reader/check", rs.PostReaderCheck())

	//parser API
	router.GET(PREFIX+"/parser/usages", rs.GetParserUsages())
	router.GET(PREFIX+"/parser/options", rs.GetParserKeyOptions())
	router.POST(PREFIX+"/parser/parse", rs.PostParse())
	router.GET(PREFIX+"/parser/samplelogs", rs.GetParserSampleLogs())
	router.POST(PREFIX+"/parser/check", rs.PostParserCheck())

	//transformer API
	router.GET(PREFIX+"/transformer/usages", rs.GetTransformerUsages())
	router.GET(PREFIX+"/transformer/options", rs.GetTransformerOptions())
	router.GET(PREFIX+"/transformer/sampleconfigs", rs.GetTransformerSampleConfigs())
	router.POST(PREFIX+"/transformer/transform", rs.PostTransform())
	router.POST(PREFIX+"/transformer/check", rs.PostTransformerCheck())

	//sender API
	router.GET(PREFIX+"/sender/usages", rs.GetSenderUsages())
	router.GET(PREFIX+"/sender/options", rs.GetSenderKeyOptions())
	router.POST(PREFIX+"/sender/send", rs.PostSend())
	router.POST(PREFIX+"/sender/check", rs.PostSenderCheck())
	router.GET(PREFIX+"/sender/router/usage", rs.GetSenderRouterUsage())
	router.GET(PREFIX+"/sender/router/option", rs.GetSenderRouterOption())

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
	router.GET(PREFIX+"/cluster/configs/:name", rs.GetClusterConfig())
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
	if mgr.DisableWeb {
		log.Warn("logkit web service was disabled")
		return rs
	}
	for {
		if port > 10000 {
			log.Fatal("bind port failed too many times, exit...")
		}
		if mgr.DisableWeb {
			break
		}

		address = ":" + strconv.Itoa(port)
		if mgr.BindHost != "" {
			address, httpschema = RemoveHttpProtocal(mgr.BindHost)
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
	if !mgr.DisableWeb {
		err = generateStatsShell(address, PREFIX)
		if err != nil {
			log.Warn(err)
		}
	}
	rs.address = address
	if rs.cluster.Enable {
		if rs.cluster.Address == "" {
			rs.cluster.Address, err = GetMySlaveUrl(address, httpschema)
			if err != nil {
				log.Fatalf("get slave url bindaddress[%v] error %v", address, err)
			}
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
		host, err = utilsos.GetLocalIP()
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
	err = os.Chmod(StatsShell, DefaultDirPerm)
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
		"code": ErrNothing,
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
				v.Tag = rs.cluster.Tag
				v.Url = rs.cluster.Address
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
		rs.mgr.lock.RLock()
		for _, conf := range rs.mgr.runnerConfig {
			runnerNameList = append(runnerNameList, conf.RunnerName)
		}
		rs.mgr.lock.RUnlock()
		return RespSuccess(c, runnerNameList)
	}
}

// get /logkit/configs
func (rs *RestService) GetConfigs() echo.HandlerFunc {
	return func(c echo.Context) error {
		rss := rs.mgr.Configs()
		return RespSuccess(c, rss)
	}
}

// get /logkit/configs/:name
func (rs *RestService) GetConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		_, runnerConfig, _, err := rs.checkNameAndConfig(c)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrConfigName, err.Error())
		}
		return RespSuccess(c, runnerConfig)
	}
}

func convertWebTransformerConfig(conf map[string]interface{}) map[string]interface{} {
	if conf == nil {
		return conf
	}
	//TODO do some pre process
	return conf
}

func (rs *RestService) checkNameAndConfig(c echo.Context) (name string, conf RunnerConfig, file string, err error) {
	if name = c.Param("name"); name == "" {
		err = errors.New("config name is empty")
		return
	}
	var exist bool
	var tmpConf RunnerConfig
	rs.mgr.lock.RLock()
	defer rs.mgr.lock.RUnlock()
	file = filepath.Join(rs.mgr.RestDir, name+".conf")
	if tmpConf, exist = rs.mgr.runnerConfig[file]; !exist {
		err = errors.New("config " + name + " is not found")
		return
	}
	deepCopyByJson(&conf, &tmpConf)
	return
}

// post /logkit/configs/<name>
func (rs *RestService) PostConfig() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var name string
		if name = c.Param("name"); name == "" {
			errMsg := "runner name is empty"
			return RespError(c, http.StatusBadRequest, ErrRunnerAdd, errMsg)
		}
		var nconf RunnerConfig
		if err = c.Bind(&nconf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerAdd, err.Error())
		}
		nconf.IsInWebFolder = true
		nconf.ParserConf = parser.ConvertWebParserConfig(nconf.ParserConf)
		if err = rs.mgr.AddRunner(name, nconf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerAdd, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// put /logkit/configs/<name>
func (rs *RestService) PutConfig() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var name string
		if name = c.Param("name"); name == "" {
			errMsg := "config name is empty"
			return RespError(c, http.StatusBadRequest, ErrRunnerUpdate, errMsg)
		}
		var nconf RunnerConfig
		if err = c.Bind(&nconf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerUpdate, err.Error())
		}
		nconf.IsInWebFolder = true
		nconf.ParserConf = parser.ConvertWebParserConfig(nconf.ParserConf)
		if err = rs.mgr.UpdateRunner(name, nconf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerUpdate, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/configs/<name>/reset
func (rs *RestService) PostConfigReset() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var name string
		if name = c.Param("name"); name == "" {
			errMsg := "config name is empty"
			return RespError(c, http.StatusBadRequest, ErrRunnerReset, errMsg)
		}
		if err = rs.mgr.ResetRunner(name); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerReset, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/configs/<name>/start
func (rs *RestService) PostConfigStart() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var name string
		if name = c.Param("name"); name == "" {
			errMsg := "config name is empty"
			return RespError(c, http.StatusBadRequest, ErrRunnerStart, errMsg)
		}
		if err = rs.mgr.StartRunner(name); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerStart, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// POST /logkit/configs/<name>/stop
func (rs *RestService) PostConfigStop() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var name string
		if name = c.Param("name"); name == "" {
			errMsg := "config name is empty"
			return RespError(c, http.StatusBadRequest, ErrRunnerStop, errMsg)
		}
		if err = rs.mgr.StopRunner(name); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerStop, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// delete /logkit/configs/<name>
func (rs *RestService) DeleteConfig() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		var name string
		if name = c.Param("name"); name == "" {
			errMsg := "config name is empty"
			return RespError(c, http.StatusBadRequest, ErrRunnerDelete, errMsg)
		}
		if err = rs.mgr.DeleteRunner(name); err != nil {
			return RespError(c, http.StatusBadRequest, ErrRunnerDelete, err.Error())
		}
		return RespSuccess(c, nil)
	}
}

// get /logkit/errorcode
func (rs *RestService) GetErrorCodeHumanize() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, ErrorCodeHumanize)
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
	if rs.l != nil {
		rs.l.Close()
	}
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
