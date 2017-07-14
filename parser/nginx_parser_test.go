package parser

import (
	"fmt"
	"testing"

	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

var accLog2 = []string{`110.110.101.101 - - [21/Mar/2017:18:14:17 +0800] "GET /files/yyyysx HTTP/1.1" 206 607 1 "-" "Apache-HttpClient/4.4.1 (Java/1.7.0_80)" "-" "122.121.111.222, 122.121.111.333, 192.168.90.61" "192.168.42.54:5000" www.qiniu.com llEAAFgmnoIa3q0U "0.040" 0.040 760 "-" "-" - - QCloud`}
var accLog1 = []string{`111.111.111.101 - - [30/Aug/2016:14:03:37 +0800] "GET /s5/M00/CE/91/xaxsxsxsxs HTTP/1.1" 200 4962 4259 "http://www.abc.cn" "Mozilla/5.0 (Windows NT 6.1; WOW64)" "-" "123.123.123.123" 192.168.41.58:5000 mirror.qiniu.com WEQAAM8htpudgG8U 0.204 0.204 938 - -  -`}
var accErrLog = []string{`can't work'`}
var timeformat1 string
var accLog1Entry sender.Data

func init() {
	timelocal1, _ := time.Parse(time.RFC3339, "2016-08-30T14:03:37+08:00")
	timeformat1 = timelocal1.Format(time.RFC3339)
	accLog1Entry = sender.Data{"request": `GET /s5/M00/CE/91/xaxsxsxsxs HTTP/1.1`,
		"sent_http_x_reqid": "WEQAAM8htpudgG8U", "request_length": int64(938),
		"http_x_from_cdn": "-", "time_local": timeformat1, "status": int64(200), "upstream_addr": "192.168.41.58:5000", "host": "mirror.qiniu.com", "http_x_estat": `-`, "bytes_sent": int64(4962), "http_user_agent": `Mozilla/5.0 (Windows NT 6.1; WOW64)`,
		"http_x_forwarded_for": "123.123.123.123", "http_x_stat": "-", "http_transfer_encoding": "-", "upstream_response_time": "0.204", "request_time": 0.204, "remote_addr": "111.111.111.101",
		"remote_user": "-", "body_bytes_sent": int64(4259), "http_referer": `http://www.abc.cn`}
}

var cfg = conf.MapConf{NginxConfPath: "nginx_test_data/nginx.conf", NginxLogFormat: "main", NginxAccSchema: "remote_addr:string, remote_user:string, time_local:date, request:string, status:long, bytes_sent:long, body_bytes_sent:long, http_referer:string, http_user_agent:string, http_transfer_encoding:string, http_x_forwarded_for:string, upstream_addr:string, host:string, sent_http_x_reqid:string, upstream_response_time:string, request_time:float, request_length:long, upstream_http_x_tag:string, upstream_http_x_uid:string, http_x_stat:string, http_x_estat:string, http_x_from_cdn:string"}
var cfg2 = conf.MapConf{NginxConfPath: "nginx_test_data/nginx.conf", NginxLogFormat: "logkit", NginxAccSchema: "remote_addr:string, remote_user:string, time_local:date, request:string, status:long, bytes_sent:long, body_bytes_sent:long, http_referer:string, http_user_agent:string, http_transfer_encoding:string, http_x_forwarded_for:string, upstream_addr:string, host:string, sent_http_x_reqid:string, upstream_response_time:string, request_time:float, request_length:long, upstream_http_x_tag:string, upstream_http_x_uid:string, http_x_stat:string, http_x_estat:string, http_x_from_cdn:string"}

func TestNewNginxParser(t *testing.T) {
	p, err := NewNginxAccParser("nginx", cfg)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, p.Name(), "nginx", "nginx parser name not equal")
	entry1S, err := p.Parse(accLog1)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	entry1 := entry1S[0]
	for k, v := range entry1 {
		assert.Equal(t, accLog1Entry[k], v, "parser "+k+" not match")
	}
	errFormat := fmt.Errorf("access log line '%v' does not match given format '%v'", accErrLog[0], p.regexp)
	_, err = p.Parse(accErrLog)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Equal(t, err, errFormat, "it should be err format")

	p2, err := NewNginxAccParser("nginx", cfg2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p2.Parse(accLog2)
	if c, ok := err.(*utils.StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
}
