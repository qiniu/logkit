package logdb

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/request"
)

var builder LogdbErrBuilder

type Logdb struct {
	Config     *config.Config
	HTTPClient *http.Client
}

func NewConfig() *config.Config {
	return config.NewConfig()
}

func New(c *config.Config) (LogdbAPI, error) {
	return NewClient(c)
}

func NewClient(c *config.Config) (p *Logdb, err error) {
	if c.LogdbEndpoint == "" {
		c.LogdbEndpoint = c.Endpoint
	}
	if c.LogdbEndpoint == "" {
		c.LogdbEndpoint = config.DefaultLogDBEndpoint
	}
	c.ConfigType = config.TypeLOGDB
	if err = base.CheckEndPoint(c.LogdbEndpoint); err != nil {
		return
	}

	var t = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   c.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: c.ResponseTimeout,
	}

	p = &Logdb{
		Config:     c,
		HTTPClient: &http.Client{Transport: t},
	}

	return
}

func (c *Logdb) newRequest(op *request.Operation, token string, v interface{}) *request.Request {
	req := request.New(c.Config, c.HTTPClient, op, token, builder, v)
	req.Data = v
	return req
}

func (c *Logdb) NewOperation(opName string, args ...interface{}) *request.Operation {
	var method, urlTmpl string
	switch opName {
	case base.OpCreateRepo:
		method, urlTmpl = base.MethodPost, "/v5/repos/%s"
	case base.OpUpdateRepo:
		method, urlTmpl = base.MethodPut, "/v5/repos/%s"
	case base.OpListRepos:
		method, urlTmpl = base.MethodGet, "/v5/repos"
	case base.OpGetRepo:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s"
	case base.OpDeleteRepo:
		method, urlTmpl = base.MethodDelete, "/v5/repos/%s"
	case base.OpSendLog:
		method, urlTmpl = base.MethodPost, "/v5/repos/%s/data?omitInvalidLog=%t"
	case base.OpQueryLog:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s/search?q=%s&sort=%s&from=%d&size=%d&scroll=%s&highlight=%t"
	case base.OpQueryScroll:
		method, urlTmpl = base.MethodPost, "/v5/repos/%s/scroll"
	case base.OpQueryHistogramLog:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s/histogram?q=%s&from=%d&to=%d&field=%s"
	case base.OpPutRepoConfig:
		method, urlTmpl = base.MethodPut, "/v5/repos/%s/config"
	case base.OpGetRepoConfig:
		method, urlTmpl = base.MethodGet, "/v5/repos/%s/config"
	case base.OpPartialQuery:
		method, urlTmpl = base.MethodPost, "/v5/repos/%s/s"
	case base.OpSchemaRef:
		method, urlTmpl = base.MethodPost, "/v5/schema/derivation"
	default:
		c.Config.Logger.Errorf("unmatched operation name: %s", opName)
		return nil
	}

	return &request.Operation{
		Name:   opName,
		Method: method,
		Path:   fmt.Sprintf(urlTmpl, args...),
	}
}
