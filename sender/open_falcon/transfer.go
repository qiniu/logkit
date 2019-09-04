package open_falcon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/sender/config"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

type TransferSender struct {
	host       string
	url        string
	path       string
	client     *http.Client
	step       int
	tags       string
	extraInfo  map[string]string
	runnerName string
	prefix     string
	tagKeys    map[string]bool
	separator  string
}

type TransferData struct {
	Metric      string  `json:"metric"`
	EndPoint    string  `json:"endpoint"`
	Tags        string  `json:"tags"`
	Value       float64 `json:"value"`
	Step        int     `json:"step"`        // 采集频率，秒
	CounterType string  `json:"counterType"` // 默认使用 GAUGE，原值
	TimeStamp   int64   `json:"timestamp"`   // 秒
}

type RespData struct {
	Message string `json:"message"`
	Total   int    `json:"total"`
	Invalid int    `json:"invalid"`
	Latency int    `json:"latency"`
}

type TransferResponse struct {
	Msg  string   `json:"msg"`
	Data RespData `json:"data"`
}

const (
	CounterTypeGauge string = "GAUGE"
	Success          string = "success"
)

func init() {
	sender.RegisterConstructor(TypeOpenFalconTransfer, NewSender)
}

func NewSender(c conf.MapConf) (sender.Sender, error) {
	transferHost, err := c.GetString(KeyOpenFalconTransferHost)
	if err != nil {
		return nil, err
	}
	transferHost = strings.TrimSuffix(transferHost, "/")
	transferUrl, err := c.GetStringOr(KeyOpenFalconTransferURL, "/api/push")
	if err != nil {
		return nil, err
	}
	separator, _ := c.GetStringOr(KeySeparator, "_")
	transferUrl = strings.TrimPrefix(transferUrl, "/")
	timeout, _ := c.GetStringOr(KeyHttpTimeout, "30s")
	step, err := c.GetInt(KeyCollectInterval)
	if err != nil {
		return nil, err
	}
	tags, err := c.GetStringOr(KeyTags, "")
	if err != nil {
		return nil, err
	}
	prefix, err := c.GetStringOr(KeyOpenFalconTransferPrefix, "")
	dur, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, errors.New("timeout configure " + timeout + " is invalid")
	}

	keyStr, _ := c.GetStringOr(KeyOpenFalconTransferTagKeys, "")
	var keys []string
	if keyStr != "" {
		keys = strings.Split(strings.TrimSpace(keyStr), ",")
	}
	tagKeys := make(map[string]bool)
	for _, k := range keys {
		tagKeys[strings.TrimSpace(k)] = true
	}

	name, _ := c.GetStringOr(KeyName, "")
	transferSender := &TransferSender{
		host:       transferHost,
		url:        transferUrl,
		path:       transferHost + "/" + transferUrl,
		step:       step,
		tags:       strings.TrimSpace(tags),
		extraInfo:  utilsos.GetExtraInfo(),
		client:     &http.Client{Timeout: dur},
		runnerName: name,
		prefix:     prefix,
		tagKeys:    tagKeys,
		separator:  separator,
	}
	return transferSender, nil
}

func (ts *TransferSender) Name() string {
	return "open_falcon_transfer_" + ts.path + "_"
}

func (ts *TransferSender) Send(datas []Data) error {
	var (
		transferDatas = make([]TransferData, 0)

		ok         bool
		vfields    map[string]interface{}
		vtags      map[string]string
		endpoint   = ts.extraInfo[KeyHostName]
		prefixName string
	)

	ste := &StatsError{
		StatsInfo: StatsInfo{
			Success: 0,
			Errors:  int64(len(datas)),
		},
	}
	timeStamp := time.Now().Unix()
	for _, d := range datas {
		tags := ts.tags
		transferTmpDatas := make([]TransferData, 0)
		if name, ok := d["name"]; ok {
			prefixName = fmt.Sprintf("%v", name)
		}
		for k, v := range d {
			switch k {
			case "fields":
				if vfields, ok = v.(map[string]interface{}); ok {
					for ik, iv := range vfields {
						if tmpData, success := ts.converToTransferData(ik, iv, timeStamp); success {
							transferTmpDatas = append(transferTmpDatas, tmpData)
						} else {
							log.Warnf("ik: %s, iv: %v type: %T cannot convert to float, discard it", ik, iv, iv)
						}
					}
				}
			case "tags":
				if vtags, ok = v.(map[string]string); ok {
					endpoint = getEndpoint(prefixName, ts.extraInfo[KeyHostName], ts.separator, vtags)
					for ik, iv := range vtags {
						tags = setTags(tags, ts.tagKeys, ik, iv)
					}
				}
			default:
				tags = setTags(tags, ts.tagKeys, k, v)
			}
		}

		log.Debugf("test fields endpoint: %v, prefixName: %v, tags: %v", endpoint, prefixName, tags)
		if prefixName != "" {
			prefixName += "_"
		}
		// tags 赋值
		for idx := range transferTmpDatas {
			transferTmpDatas[idx].Metric = ts.prefix + prefixName + transferTmpDatas[idx].Metric
			transferTmpDatas[idx].Tags = tags
			transferTmpDatas[idx].EndPoint = endpoint
			log.Debugf("test fields metric: %v, value: %v", transferTmpDatas[idx].Metric, transferTmpDatas[idx].Value)
		}
		transferDatas = append(transferDatas, transferTmpDatas...)
	}
	if len(transferDatas) == 0 {
		log.Warnf("Runner[%v] Sender[%v] send no data", ts.runnerName, ts.Name())
		ste.LastError = "no valid data to send"
		ste.SendError = reqerr.NewSendError("no valid data to send", sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return ste
	}
	byteData, err := json.Marshal(transferDatas)
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v] marshal transferDatas %+v failed: %v", ts.runnerName, ts.Name(), transferDatas, err)
		ste.LastError = err.Error()
		ste.SendError = reqerr.NewSendError(err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return ste
	}
	req, err := http.NewRequest(http.MethodPost, ts.path, bytes.NewReader(byteData))
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v] construct request failed, %v", ts.runnerName, ts.Name(), err)
		ste.LastError = err.Error()
		ste.SendError = reqerr.NewSendError(err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return err
	}
	resp, err := ts.client.Do(req)
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v] post datas error %v\n", ts.runnerName, ts.Name(), err)
		ste.LastError = err.Error()
		ste.SendError = reqerr.NewSendError(err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return ste
	}
	defer resp.Body.Close()
	var respBody []byte
	if resp.StatusCode != http.StatusOK {
		respBody, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] read response body error %v\n", ts.runnerName, ts.Name(), err)
			ste.LastError = err.Error()
			ste.SendError = reqerr.NewSendError(err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
			return ste
		}
		log.Errorf("Runner[%v] Sender[%v] response code is %v, response body is %v\n", ts.runnerName, ts.Name(), resp.StatusCode, string(respBody))
		ste.LastError = string(respBody)
		ste.SendError = reqerr.NewSendError(string(respBody), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return ste
	}
	respBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v] read response body error %v\n", ts.runnerName, ts.Name(), err)
		ste.LastError = string(respBody)
		ste.SendError = reqerr.NewSendError(string(respBody), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return err
	}
	if string(respBody) == Success {
		log.Infof("sender to open-falcon success: len(transferDatas) = %d", len(transferDatas))
		return nil
	}
	var respData TransferResponse
	if err = json.Unmarshal(respBody, &respData); err != nil {
		log.Errorf("Runner[%v] Sender[%v] unmarshal response body (%s) error %v\n", ts.runnerName, ts.Name(), string(respBody), err)
		ste.LastError = err.Error()
		ste.SendError = reqerr.NewSendError(err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return ste
	}
	if respData.Msg != Success || respData.Data.Invalid != 0 {
		log.Warnf("Runner[%v] Sender[%v] send to transfer failed %v\n", ts.runnerName, ts.Name(), respData)
		if respData.Msg == Success {
			ste.Errors = int64(respData.Data.Invalid)
			ste.Success = int64(respData.Data.Total) - ste.Errors
		}
		ste.SendError = reqerr.NewSendError(string(respBody), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
		return ste
	}
	log.Infof("sender to open-falcon success: %+v", respData)
	return nil
}

func setTags(tags string, tagKeys map[string]bool, key string, val interface{}) string {
	if val == nil {
		return tags
	}

	if len(tagKeys) == 0 || tagKeys[key] {
		if tags != "" {
			tags += ","
		}
		return tags + key + "=" + fmt.Sprintf("%v", val)
	}

	return tags
}

func (ts *TransferSender) Close() (err error) {
	return nil
}

func (ts *TransferSender) converToTransferData(key string, value interface{}, timeStamp int64) (TransferData, bool) {
	var (
		valuef   float64
		valuef32 float32

		valuei   int
		valuei32 int32
		valuei64 int64

		valueui   uint
		valueui32 uint32
		valueui64 uint64

		values string
		valuej json.Number

		ok  bool
		err error
	)
	result := TransferData{
		Metric:      key,
		Step:        ts.step,
		CounterType: CounterTypeGauge,
		TimeStamp:   timeStamp,
	}
	if valuef32, ok = value.(float32); ok {
		result.Value = float64(valuef32)
		return result, true
	}
	if valuef, ok = value.(float64); ok {
		result.Value = valuef
		return result, true
	}

	if valuei, ok = value.(int); ok {
		result.Value = float64(valuei)
		return result, true
	}
	if valuei32, ok = value.(int32); ok {
		result.Value = float64(valuei32)
		return result, true
	}
	if valuei64, ok = value.(int64); ok {
		result.Value = float64(valuei64)
		return result, true
	}

	if valueui, ok = value.(uint); ok {
		result.Value = float64(valueui)
		return result, true
	}
	if valueui32, ok = value.(uint32); ok {
		result.Value = float64(valueui32)
		return result, true
	}
	if valueui64, ok = value.(uint64); ok {
		result.Value = float64(valueui64)
		return result, true
	}

	if valuej, ok = value.(json.Number); ok {
		if valuef, err = valuej.Float64(); err == nil {
			result.Value = valuef
			return result, true
		}
	}
	if values, ok = value.(string); ok {
		if valuef, err := strconv.ParseFloat(values, 64); err == nil {
			result.Value = valuef
			return result, true
		}
	}
	return TransferData{}, false
}

func getEndpoint(name, defaultVal, separator string, tagsVal map[string]string) string {
	if endpoint, ok := tagsVal["endpoint"]; ok {
		return endpoint
	}

	if strings.HasPrefix(name, "vsphere"+separator+"vm"+separator) {
		return tagsVal["vmname"]
	}

	if strings.HasPrefix(name, "vsphere"+separator+"host"+separator) {
		return tagsVal["esxhostname"]
	}

	if strings.HasPrefix(name, "vsphere"+separator+"datastore"+separator) {
		return tagsVal["dsname"]
	}

	if strings.HasPrefix(name, "vsphere"+separator+"cluster"+separator) {
		return tagsVal["clustername"]
	}

	if strings.HasPrefix(name, "vsphere"+separator+"datacenter"+separator) {
		return tagsVal["dcname"]
	}

	return defaultVal
}
