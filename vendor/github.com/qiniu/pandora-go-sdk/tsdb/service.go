package tsdb

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/base/request"
)

var builder errBuilder

type Tsdb struct {
	Config     *config.Config
	HTTPClient *http.Client
}

func NewConfig() *config.Config {
	return config.NewConfig()
}

func New(c *config.Config) (TsdbAPI, error) {
	return newClient(c)
}

func newClient(c *config.Config) (p *Tsdb, err error) {
	if c.TsdbEndpoint == "" {
		c.TsdbEndpoint = c.Endpoint
	}
	if c.TsdbEndpoint == "" {
		c.TsdbEndpoint = config.DefaultTSDBEndpoint
	}
	c.ConfigType = config.TypeTSDB
	if err = base.CheckEndPoint(c.TsdbEndpoint); err != nil {
		return
	}

	var t = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   c.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: c.ResponseTimeout,
	}

	p = &Tsdb{
		Config:     c,
		HTTPClient: &http.Client{Transport: t},
	}

	return
}

func (c *Tsdb) newRequest(op *request.Operation, token string, v interface{}) *request.Request {
	req := request.New(c.Config, c.HTTPClient, op, token, builder, v)
	req.Data = v
	return req
}

func (c *Tsdb) newOperation(opName string, args ...interface{}) *request.Operation {
	var method, urlTmpl string
	switch opName {
	case base.OpCreateRepo:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s"
	case base.OpListRepos:
		method, urlTmpl = base.MethodGet, "/v4/repos"
	case base.OpGetRepo:
		method, urlTmpl = base.MethodGet, "/v4/repos/%s"
	case base.OpDeleteRepo:
		method, urlTmpl = base.MethodDelete, "/v4/repos/%s"
	case base.OpUpdateRepoMetadata:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s/meta"
	case base.OpDeleteRepoMetadata:
		method, urlTmpl = base.MethodDelete, "/v4/repos/%s/meta"
	case base.OpCreateSeries:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s/series/%s"
	case base.OpUpdateSeriesMetadata:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s/series/%s/meta"
	case base.OpDeleteSeriesMetadata:
		method, urlTmpl = base.MethodDelete, "/v4/repos/%s/series/%s/meta"
	case base.OpListSeries:
		method, urlTmpl = base.MethodGet, "/v4/repos/%s/series"
	case base.OpDeleteSeries:
		method, urlTmpl = base.MethodDelete, "/v4/repos/%s/series/%s"
	case base.OpCreateView:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s/views/%s"
	case base.OpListView:
		method, urlTmpl = base.MethodGet, "/v4/repos/%s/views"
	case base.OpDeleteView:
		method, urlTmpl = base.MethodDelete, "/v4/repos/%s/views/%s"
	case base.OpGetView:
		method, urlTmpl = base.MethodGet, "/v4/repos/%s/views/%s"
	case base.OpQueryPoints:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s/query"
	case base.OpWritePoints:
		method, urlTmpl = base.MethodPost, "/v4/repos/%s/points"
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
