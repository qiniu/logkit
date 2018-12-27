package http_response

import (
	"errors"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/http_response"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/metric/telegraf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

const MetricName = "http_response"

var (
	ConfigAddress             = "address"
	ConfigHttpProxy           = "http_proxy"
	ConfigMethod              = "method"
	ConfigFollowRedirects     = "follow_redirects"
	ConfigBody                = "body"
	ConfigResponseStringMatch = "response_string_match"
	ConfigHeaders             = "headers"
	ConfigInsecureSkipVerify  = "insecure_skip_verify"
	ConfigTLSCA               = "tls_ca"
	ConfigTLSCert             = "tls_cert"
	ConfigTLSKey              = "tls_key"
)

func init() {
	telegraf.AddUsage(MetricName, "http请求返回内容(http_response)")
	telegraf.AddConfig(MetricName, map[string]interface{}{
		metric.OptionString: []Option{
			{
				KeyName:      ConfigAddress,
				Default:      `http://localhost`,
				DefaultNoUse: true,
				Description:  "服务器连接地址",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigHttpProxy,
				Default:      "",
				Placeholder:  "http://localhost:8888",
				DefaultNoUse: false,
				Description:  "http代理地址",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigMethod,
				Default:      "GET",
				DefaultNoUse: false,
				Description:  "请求方法",
			},
			{
				KeyName:       ConfigFollowRedirects,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       "false",
				DefaultNoUse:  false,
				Description:   "访问重定向地址",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:      ConfigBody,
				Default:      "",
				DefaultNoUse: false,
				Description:  "请求体",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigResponseStringMatch,
				Default:      "",
				DefaultNoUse: false,
				Description:  "请求结果匹配",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigHeaders,
				Default:      "",
				DefaultNoUse: false,
				Description:  "请求头，键值对用空格，多组用逗号",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:       ConfigInsecureSkipVerify,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       true,
				DefaultNoUse:  false,
				Description:   "跳过校验SSL证书",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:            ConfigTLSCA,
				ChooseOnly:         false,
				Default:            "",
				Required:           false,
				Placeholder:        "证书授权的地址.ca",
				DefaultNoUse:       true,
				AdvanceDepend:      "insecure_skip_verify",
				AdvanceDependValue: false,
				Description:        "证书授权地址(tls_ca)",
				ToolTip:            `证书授权地址`,
			},
			{
				KeyName:            ConfigTLSCert,
				ChooseOnly:         false,
				Default:            "",
				Required:           false,
				Placeholder:        "证书的地址.cert",
				DefaultNoUse:       true,
				AdvanceDepend:      "insecure_skip_verify",
				AdvanceDependValue: false,
				Description:        "证书地址(tls_cert)",
				ToolTip:            `证书地址`,
			},
			{
				KeyName:            ConfigTLSKey,
				ChooseOnly:         false,
				Default:            "",
				Required:           false,
				Placeholder:        "秘钥文件的地址.key",
				DefaultNoUse:       true,
				AdvanceDepend:      "insecure_skip_verify",
				AdvanceDependValue: false,
				Description:        "私钥文件地址(tls_key)",
				ToolTip:            `私钥文件地址`,
			},
		},
		metric.AttributesString: KeyValueSlice{},
	})
}

type collector struct {
	*telegraf.Collector
}

func (c *collector) SyncConfig(data map[string]interface{}, meta *reader.Meta) error {
	hr, ok := c.Input.(*http_response.HTTPResponse)
	if !ok {
		return errors.New("unexpected elasticsearch type, want '*elasticsearch.Elasticsearch'")
	}
	server, ok := data[ConfigAddress].(string)
	if !ok {
		return fmt.Errorf("request address is not valid string, but %T", data[ConfigAddress])
	}
	hr.Address = strings.TrimSpace(server)

	httpProxy, ok := data[ConfigHttpProxy].(string)
	if ok {
		hr.HTTPProxy = strings.TrimSpace(httpProxy)
	}

	method, ok := data[ConfigMethod].(string)
	if ok {
		hr.Method = strings.TrimSpace(method)
	}
	redirects, ok := data[ConfigFollowRedirects].(bool)
	if ok {
		hr.FollowRedirects = redirects
	}
	body, ok := data[ConfigBody].(string)
	if ok {
		hr.Body = body
	}

	responseMatch, ok := data[ConfigResponseStringMatch].(string)
	if ok {
		hr.ResponseStringMatch = strings.TrimSpace(responseMatch)
	}
	headers, ok := data[ConfigHeaders].(string)
	if ok {
		nheaders := getheaders(headers)
		if len(nheaders) > 0 {
			hr.Headers = nheaders
		}
	}
	InsecureSkipVerify, ok := data[ConfigInsecureSkipVerify].(bool)
	if ok {
		hr.InsecureSkipVerify = InsecureSkipVerify
	}
	TLSCA, ok := data[ConfigTLSCA].(string)
	if ok {
		hr.TLSCA = strings.TrimSpace(TLSCA)
	}
	TLSCert, ok := data[ConfigTLSCert].(string)
	if ok {
		hr.TLSCert = strings.TrimSpace(TLSCert)
	}
	TLSKey, ok := data[ConfigTLSKey].(string)
	if ok {
		hr.TLSKey = strings.TrimSpace(TLSKey)
	}
	return nil
}

func getheaders(header string) map[string]string {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil
	}
	hds := strings.Split(header, ",")
	var headers = make(map[string]string)
	for _, v := range hds {
		kv := strings.Fields(v)
		if len(kv) < 2 {
			continue
		}
		headers[kv[0]] = kv[1]
	}
	return headers
}

// NewCollector creates a new Elasticsearch collector.
func NewCollector() metric.Collector {
	input := inputs.Inputs[MetricName]()
	if _, err := toml.Decode(input.SampleConfig(), input); err != nil {
		log.Warnf("metric: failed to decode sample config of http_response: %v", err)
	}
	return &collector{telegraf.NewCollector(MetricName, input)}
}

func init() {
	metric.Add(MetricName, NewCollector)
}
