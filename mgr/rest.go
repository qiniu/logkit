package mgr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
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

type ErrorResponse struct {
	Error error `json:"error"`
}

func NewErrorResponse(err error) *ErrorResponse {
	return &ErrorResponse{Error: err}
}

func NewRestService(mgr *Manager, router *httprouter.Router) *RestService {

	rs := &RestService{
		mgr: mgr,
	}
	router.GET(PREFIX+"/status", rs.Status)
	router.GET(PREFIX+"/configs", rs.GetConfigs)
	router.GET(PREFIX+"/configs/:name", rs.GetConfig)
	router.POST(PREFIX+"/configs/:name", rs.PostConfig)
	router.DELETE(PREFIX+"/configs/:name", rs.DeleteConfig)

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
func (rs *RestService) Status(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	rss := rs.mgr.Status()
	br, _ := json.Marshal(rss)
	rw.Write(br)
	rw.Header().Set("Content-Type", "application/json")
	return
}

// get /logkit/configs
func (rs *RestService) GetConfigs(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	rss := rs.mgr.runnerConfig
	br, _ := json.Marshal(rss)
	rw.Write(br)
	rw.Header().Set("Content-Type", "application/json")
	return
}

// get /logkit/configs/:name
func (rs *RestService) GetConfig(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	var err error
	defer func() {
		if err != nil {
			ret, errX := json.Marshal(NewErrorResponse(err))
			if errX != nil {
				log.Error(errX)
			}
			rw.Write(ret)
		}
	}()
	filename := rs.mgr.RestDir + "/" + name + ".conf"
	rss, ok := rs.mgr.runnerConfig[filename]
	if name == "" || !ok {
		rw.WriteHeader(400)
		err = fmt.Errorf("config name is empty or file %v not exist", filename)
		return
	}
	br, _ := json.Marshal(rss)
	rw.Write(br)
	rw.Header().Set("Content-Type", "application/json")
	return
}

// post /logkit/configs/<name>
func (rs *RestService) PostConfig(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	rw.Header().Set("Content-Type", "application/json")
	var err error
	defer func() {
		if err != nil {
			ret, errX := json.Marshal(NewErrorResponse(err))
			if errX != nil {
				log.Error(errX)
			}
			rw.Write(ret)
		} else {
			rw.Write([]byte("{}"))
		}
	}()
	if name == "" {
		rw.WriteHeader(400)
		err = fmt.Errorf("config name is empty")
		return
	}
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		rw.WriteHeader(400)
		return
	}
	var nconf RunnerConfig
	err = conf.LoadData(&nconf, content)
	if err != nil {
		rw.WriteHeader(400)
		return
	}
	filename := rs.mgr.RestDir + "/" + nconf.RunnerName + ".conf"
	if rs.mgr.isRunning(filename) {
		rw.WriteHeader(400)
		err = fmt.Errorf("file %v runner is running", filename)
		return
	}
	err = rs.mgr.ForkRunner(filename, nconf, true)
	return
}

// delete /logkit/configs/<name>
func (rs *RestService) DeleteConfig(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	name := params.ByName("name")
	rw.Header().Set("Content-Type", "application/json")
	var err error
	defer func() {
		if err != nil {
			ret, errX := json.Marshal(NewErrorResponse(err))
			if errX != nil {
				log.Error(errX)
			}
			rw.Write(ret)
		} else {
			rw.Write([]byte("{}"))
		}
	}()
	if name == "" {
		rw.WriteHeader(400)
		err = fmt.Errorf("config name is empty")
		return
	}
	filename := rs.mgr.RestDir + "/" + name + ".conf"
	err = rs.mgr.Remove(filename)
	return
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
