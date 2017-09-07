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

	"github.com/labstack/echo"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
)

var DEFAULT_PORT = 4000

const (
	StatsShell = "stats"
	PREFIX     = "/logkit"
)

type cmdArgs struct {
	CmdArgs []string
}

type RestService struct {
	mgr     *Manager
	l       net.Listener
	address string
}

func NewRestService(mgr *Manager, router *echo.Echo) *RestService {

	rs := &RestService{
		mgr: mgr,
	}
	router.GET(PREFIX+"/status", rs.Status())
	router.GET(PREFIX+"/configs", rs.GetConfigs())
	router.GET(PREFIX+"/configs/:name", rs.GetConfig())
	router.POST(PREFIX+"/configs/:name", rs.PostConfig())
	router.DELETE(PREFIX+"/configs/:name", rs.DeleteConfig())

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

	//version
	router.GET(PREFIX+"/version", rs.GetVersion())

	var (
		port     = DEFAULT_PORT
		address  string
		listener net.Listener
		err      error
	)

	for {
		if port > 10000 {
			log.Fatal("bind port failed too many times, exit...")
		}
		address = ":" + strconv.Itoa(port)
		if mgr.BindHost != "" {
			address = mgr.BindHost
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
	return rs
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

// get /logkit/status
func (rs *RestService) Status() echo.HandlerFunc {
	return func(c echo.Context) error {
		rss := rs.mgr.Status()
		return c.JSON(http.StatusOK, rss)
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
		return c.JSON(http.StatusOK, rss)
	}
}

// get /logkit/configs/:name
func (rs *RestService) GetConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		name := c.Param("name")
		filename := rs.mgr.RestDir + "/" + name + ".conf"
		rs.mgr.lock.RLock()
		defer rs.mgr.lock.RUnlock()
		rss, ok := rs.mgr.runnerConfig[filename]
		if name == "" || !ok {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("config name is empty or file %v not exist", filename))
		}
		return c.JSON(http.StatusOK, rss)
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

// post /logkit/configs/<name>
func (rs *RestService) PostConfig() echo.HandlerFunc {
	return func(c echo.Context) (err error) {
		name := c.Param("name")
		if name == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "config name is empty")
		}

		var nconf RunnerConfig
		if err = c.Bind(&nconf); err != nil {
			return err
		}
		filename := rs.mgr.RestDir + "/" + nconf.RunnerName + ".conf"
		if rs.mgr.isRunning(filename) {
			return echo.NewHTTPError(http.StatusBadRequest, "file "+filename+" runner is running")
		}
		nconf.ParserConf = convertWebParserConfig(nconf.ParserConf)
		nconf.IsInWebFolder = true
		err = rs.mgr.ForkRunner(filename, nconf, true)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		confBytes, err := json.MarshalIndent(nconf, "", "    ")
		err = ioutil.WriteFile(filename, confBytes, 0644)
		return
	}
}

// delete /logkit/configs/<name>
func (rs *RestService) DeleteConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		name := c.Param("name")
		if name == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "config name is empty")
		}
		filename := rs.mgr.RestDir + "/" + name + ".conf"
		err := rs.mgr.Remove(filename)
		if err != nil {
			return err
		}
		return os.Remove(filename)
	}
}

func (rs *RestService) GetVersion() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.String(http.StatusOK, rs.mgr.Version)
	}
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
