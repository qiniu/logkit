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

	HttpErrState = "http_err_state"
	HttpErrMsg   = "http_err_msg"

	HttpTimeCostTotal = "http_time_cost_total"
	HttpErrStateTotal = "http_err_state_total"
	HttpErrMsgTotal   = "http_err_msg_total"

	// Config 中的字段
	ConfigHttpDatas = "http_datas"

	DefaultTimeOut = 1 * time.Minute
	StateSuccess   = float64(1)
	StateFail      = float64(0)
)

// KeyHttpUsages TypeMetricHttp 中的字段名称
var KeyHttpUsages = KeyValueSlice{
	{HttpStatusCode, "http响应状态码", ""},
	{HttpRespHead, "http响应头部信息", ""},
	{HttpData, "http响应内容", ""},
	{HttpTimeCost, "http响应用时", ""},
	{HttpTarget, "http请求地址", ""},
	{HttpErrState, "http响应状态", ""},
	{HttpErrMsg, "http响应错误信息", ""},
}

// ConfigHttpUsages TypeMetricHttp config 中的字段描述
var ConfigHttpUsages = KeyValueSlice{
	{ConfigHttpDatas, "填写(" + ConfigHttpDatas + ")", ""},
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
	return []string{HttpStatusCode, HttpRespHead, HttpData, HttpTimeCost, HttpTarget, HttpErrState, HttpErrMsg}
}

func (_ *HttpStats) Config() map[string]interface{} {
	configOptions := make([]Option, 0)
	for _, val := range ConfigHttpUsages {
		option := Option{
			KeyName:      val.Key,
			ChooseOnly:   false,
			Default:      `[{"method":"GET", "url":"https://www.qiniu.com", "expect_code":200, "expect_data":"七牛云"},{"method":"GET", "url":"https://www.qiniu.com/products/pandora", "expect_code":200, "expect_data":"七牛云"}]`,
			DefaultNoUse: true,
			Description:  val.Value,
			Type:         metric.ConfigTypeString,
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
	ExpectData string            `json:"expect_data"`
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

	data := make(map[string]interface{})
	totalCreateTime := time.Now()
	for idx, httpData := range httpDataArr {
		request, err := http.NewRequest(httpData.Method, httpData.Url, bytes.NewBuffer([]byte(httpData.Body)))
		if err != nil {
			return nil, fmt.Errorf("erro new http request: %s", err)
		}
		//给一个key设定为响应的value
		for k, v := range httpData.Header {
			request.Header.Set(k, v) //必须设定该参数,POST参数才能正常提交
		}

		createTime := time.Now()
		client := &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   DefaultTimeOut,
					Deadline:  time.Now().Add(DefaultTimeOut),
					KeepAlive: DefaultTimeOut,
				}).Dial,
			},
		}
		resp, err := client.Do(request) //发送请求
		completeTime := time.Now()
		if err != nil {
			data, _, _ = setData(data, resp, httpData, err, createTime, completeTime, idx+1)
			data[HttpErrStateTotal] = StateFail
			data[HttpErrMsgTotal] = data[HttpErrMsg+"_"+strconv.Itoa(idx+1)]
			data[HttpTimeCostTotal] =
				completeTime.Sub(totalCreateTime).Nanoseconds() / int64(time.Millisecond)
			break
		}

		failReason := ""
		data, failReason, err = setData(data, resp, httpData, err, createTime, completeTime, idx+1)
		if err != nil {
			return nil, fmt.Errorf("error getting response body: %s", err)
		}
		if data[HttpErrState+"_"+strconv.Itoa(idx+1)] == StateFail {
			data[HttpErrStateTotal] = StateFail
			data[HttpErrMsgTotal] = failReason
			data[HttpTimeCostTotal] =
				completeTime.Sub(totalCreateTime).Nanoseconds() / int64(time.Millisecond)
			break
		}
	}
	_, ok := data[HttpErrStateTotal]
	if !ok {
		data[HttpErrStateTotal] = StateSuccess
		data[HttpErrMsgTotal] = ""
		data[HttpTimeCostTotal] =
			time.Now().Sub(totalCreateTime).Nanoseconds() / int64(time.Millisecond)
	}
	datas = append(datas, data)

	return
}

func setData(data map[string]interface{}, resp *http.Response, httpData HttpDataReq, err error,
	createTime, completeTime time.Time, idx int) (map[string]interface{}, string, error) {
	if err != nil {
		resp = &http.Response{
			StatusCode: -1,
			Header:     nil,
		}
		data, failReason := setDataValue(data, resp, httpData, createTime, completeTime, err.Error(), idx)
		return data, failReason, nil
	}

	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	data, failReason := setDataValue(data, resp, httpData, createTime, completeTime, string(content), idx)
	return data, failReason, nil
}

func setDataValue(data map[string]interface{}, resp *http.Response, httpData HttpDataReq,
	createTime, completeTime time.Time, content string, idx int) (map[string]interface{}, string) {
	failReason := ""
	// 加上_idx后缀
	httpDataIdx, httpTimeCostIdx, httpTargetIdx, httpStatusCodeIdx,
		httpRespHeadIdx, httpErrStateIdx, httpErrMsgIdx := joinIdx(strconv.Itoa(idx))
	data[httpDataIdx] = content
	data[httpTimeCostIdx] = completeTime.Sub(createTime).Nanoseconds() / int64(time.Millisecond)
	data[httpTargetIdx] = httpData.Url
	if resp != nil {
		data[httpStatusCodeIdx] = resp.StatusCode
		data[httpRespHeadIdx] = resp.Header
		if err, ok := compareExpectResult(httpData.ExpectCode, resp.StatusCode,
			httpData.ExpectData, content); !ok {
			data[httpErrStateIdx] = StateFail
			data[httpErrMsgIdx] = content
			failReason = err.Error()
		} else {
			data[httpErrStateIdx] = StateSuccess
			data[httpErrMsgIdx] = ""
		}
	}
	return data, failReason
}

func compareExpectResult(expectCode, realCode int, expectData, realData string) (error, bool) {
	if expectCode != realCode {
		return fmt.Errorf("return status code is: %d, expect: %d", realCode, expectCode), false
	}
	if expectData != "" && !strings.Contains(realData, expectData) {
		return fmt.Errorf("don't contain: %s", expectData), false
	}
	return nil, true
}

func joinIdx(idx string) (string, string, string, string, string, string, string) {
	httpDataIdx := HttpData + "_" + idx
	httpTimeCostIdx := HttpTimeCost + "_" + idx
	httpTargetIdx := HttpTarget + "_" + idx
	httpStatusCodeIdx := HttpStatusCode + "_" + idx
	httpRespHeadIdx := HttpRespHead + "_" + idx
	httpErrStateIdx := HttpErrState + "_" + idx
	httpErrMsgIdx := HttpErrMsg + "_" + idx

	return httpDataIdx, httpTimeCostIdx, httpTargetIdx,
		httpStatusCodeIdx, httpRespHeadIdx, httpErrStateIdx, httpErrMsgIdx
}

func init() {
	metric.Add(TypeMetricHttp, func() metric.Collector {
		return &HttpStats{}
	})
}
