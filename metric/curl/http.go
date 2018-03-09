package curl

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/logkit/metric"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
)

const (
	TypeMetricHttp   = "http"
	MetricHttpUsages = "HTTP(http)"

	HttpStatusCode = "http_status_code"
	HttpRespHead   = "http_resp_head"
	HttpData       = "http_data"
	HttpTimeCost   = "http_time_cost"
	HttpTarget     = "http_target"
	HttpOrder      = "http_order"

	HttpErrState = "http_err_state"
	HttpErrMsg   = "http_err_msg"

	// Config 中的字段
	ConfigHttpDatas = "http_datas"

	DefaultTimeOut = 1 * time.Minute
	StateSuccess   = "success"
	StateFail      = "fail"
)

// KeyHttpUsages TypeMetricHttp 中的字段名称
var KeyHttpUsages = []KeyValue{
	{HttpStatusCode, "http响应状态码"},
	{HttpRespHead, "http响应头部信息"},
	{HttpData, "http响应内容"},
	{HttpTimeCost, "http响应用时"},
	{HttpTarget, "http请求地址"},
	{HttpOrder, "http请求顺序"},
	{HttpErrState, "http响应状态"},
	{HttpErrMsg, "http响应错误信息"},
}

// ConfigHttpUsages TypeMetricHttp config 中的字段描述
var ConfigHttpUsages = []KeyValue{
	{ConfigHttpDatas, "填写(" + ConfigHttpDatas + ")"},
}

type HttpStats struct {
	HttpDatas string `json:"http_datas"`
}

var httpdatas string

func (_ *HttpStats) Name() string {
	return TypeMetricHttp
}

func (_ *HttpStats) Usages() string {
	return MetricHttpUsages
}

func (_ *HttpStats) Tags() []string {
	return []string{HttpStatusCode, HttpRespHead, HttpData, HttpTimeCost, HttpTarget, HttpErrState, HttpErrMsg, HttpOrder}
}

func (_ *HttpStats) Config() map[string]interface{} {
	configOptions := make([]Option, 0)
	for _, val := range ConfigHttpUsages {
		option := Option{
			KeyName:      val.Key,
			ChooseOnly:   false,
			Default:      `[{"method":"GET", "url":"https://www.qiniu.com", "expect_code":200}]`,
			DefaultNoUse: true,
			Description:  val.Value,
			Type:         metric.ConsifTypeString,
		}
		configOptions = append(configOptions, option)
	}
	config := map[string]interface{}{
		metric.OptionString:     configOptions,
		metric.AttributesString: KeyHttpUsages,
	}
	return config
}

type HttpDataReq struct {
	Method     string            `json:"method"`
	Url        string            `json:"url"`
	Header     map[string]string `json:"header"`
	Body       string            `json:"body"`
	ExpectCode int               `json:"expect_code"`
	ExpectData []byte            `json:"expect_data"`
}

func (s *HttpStats) Collect() (datas []map[string]interface{}, err error) {
	var httpDataArr []HttpDataReq
	if httpdatas != s.HttpDatas {
		err = jsoniter.Unmarshal([]byte(s.HttpDatas), &httpDataArr)
		if err != nil {
			return nil, fmt.Errorf("metric %v unmarshal config error %v", TypeMetricHttp, err)
		}
		httpdatas = s.HttpDatas
	}

	steps := make(map[string]interface{})
	stepsCreateTime := time.Now()
	for idx, httpData := range httpDataArr {
		var step = make(map[string]interface{})
		request, err := http.NewRequest(httpData.Method, httpData.Url, bytes.NewBuffer([]byte(httpData.Body)))
		if err != nil {
			return nil, fmt.Errorf("erro new http request: %s", err)
		}
		//给一个key设定为响应的value.
		for k, v := range httpData.Header {
			request.Header.Set(k, v) //必须设定该参数,POST参数才能正常提交
		}

		createTime := time.Now()
		http.DefaultClient.Transport = &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   DefaultTimeOut,
				Deadline:  time.Now().Add(DefaultTimeOut),
				KeepAlive: DefaultTimeOut,
			}).Dial,
		}
		resp, err := http.DefaultClient.Do(request) //发送请求
		completeTime := time.Now()
		if err != nil {
			step, _ = setStep(resp, httpData, err, createTime, completeTime, idx+1)
			steps[HttpErrState] = step[HttpErrState]
			steps[HttpErrMsg] = step[HttpErrMsg]
			steps[HttpTimeCost] = completeTime.Sub(stepsCreateTime).Nanoseconds() / int64(time.Millisecond)
			break
		}

		step, err = setStep(resp, httpData, err, createTime, completeTime, idx+1)
		if err != nil {
			return nil, fmt.Errorf("error getting response body: %s", err)
		}
		steps[strconv.Itoa(idx+1)] = step
	}
	_, ok := steps[HttpErrState]
	if !ok {
		steps[HttpErrState] = StateSuccess
		steps[HttpErrMsg] = ""
		steps[HttpTimeCost] = time.Now().Sub(stepsCreateTime).Nanoseconds() / int64(time.Millisecond)
	}
	datas = append(datas, steps)

	return
}

func setStep(resp *http.Response, httpData HttpDataReq, err error,
	createTime, completeTime time.Time, order int) (map[string]interface{}, error) {
	if err != nil {
		resp = &http.Response{
			StatusCode: -1,
			Header:     nil,
		}
		return setStepValue(resp, httpData, createTime, completeTime, err.Error(), order), nil
	}

	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return setStepValue(resp, httpData, createTime, completeTime, string(content), order), nil
}

func setStepValue(resp *http.Response, httpData HttpDataReq,
	createTime, completeTime time.Time, content string, order int) map[string]interface{} {
	step := map[string]interface{}{
		HttpData:     content,
		HttpTimeCost: completeTime.Sub(createTime).Nanoseconds() / int64(time.Millisecond),
		HttpTarget:   httpData.Url,
		HttpOrder:    order,
	}
	if resp != nil {
		step[HttpStatusCode] = resp.StatusCode
		step[HttpRespHead] = resp.Header
		if !compareExpectResult(httpData.ExpectCode, resp.StatusCode,
			string(httpData.ExpectData), content) {
			step[HttpErrState] = StateFail
			step[HttpErrMsg] = content
		} else {
			step[HttpErrState] = StateSuccess
			step[HttpErrMsg] = ""
		}
	}
	return step
}

func compareExpectResult(expectCode, realCode int, expectData, realData string) bool {
	if expectCode != realCode {
		return false
	}
	if expectData != "" && !strings.Contains(realData, expectData) {
		return false
	}
	return true
}

func init() {
	metric.Add(TypeMetricHttp, func() metric.Collector {
		return &HttpStats{}
	})
}
