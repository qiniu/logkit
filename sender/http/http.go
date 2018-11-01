package http

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/json-iterator/go"
	"github.com/sven0726/fasttemplate"

	"github.com/qiniu/pandora-go-sdk/pipeline"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/sender/config"
	. "github.com/qiniu/logkit/utils/models"
)

var _ sender.SkipDeepCopySender = &Sender{}

type Sender struct {
	url      string
	gZip     bool
	csvHead  bool
	protocol string
	csvSplit string
	template string

	client         *http.Client
	templateRender *fasttemplate.Template
	runnerName     string
}

func init() {
	sender.RegisterConstructor(TypeHttp, NewSender)
}

// http sender
func NewSender(c conf.MapConf) (sender.Sender, error) {
	url, err := c.GetString(KeyHttpSenderUrl)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	gZip, _ := c.GetBoolOr(KeyHttpSenderGzip, false)
	templateStr, _ := c.GetStringOr(KeyHttpSenderTemplate, "")
	csvHead, _ := c.GetBoolOr(KeyHttpSenderCsvHead, true)
	csvSplit, _ := c.GetStringOr(KeyHttpSenderCsvSplit, "\t")
	protocol, _ := c.GetStringOr(KeyHttpSenderProtocol, SendProtocolJson)
	runnerName, _ := c.GetStringOr(KeyRunnerName, UnderfinedRunnerName)
	timeout, _ := c.GetStringOr(KeyHttpTimeout, "30s")
	dur, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, errors.New("timeout configure " + timeout + " is invalid")
	}
	switch protocol {
	case SendProtocolCSV:
		if csvSplit == "" {
			csvSplit = "\t"
		}
	case SendProtocolJson, SendProtocolWholeJson, SendProtocolRaw:
	default:
		return nil, fmt.Errorf("runner[%v] create sender error, protocol %v is not support", runnerName, protocol)
	}
	var templateRender *fasttemplate.Template
	_temp := strings.TrimSpace(templateStr)
	if _temp != "" {
		templateRender = fasttemplate.New(_temp, "{{", "}}")
	}

	httpSender := &Sender{
		url:            url,
		gZip:           gZip,
		template:       _temp,
		csvHead:        csvHead,
		protocol:       protocol,
		csvSplit:       csvSplit,
		runnerName:     runnerName,
		templateRender: templateRender,
		client:         &http.Client{Timeout: dur},
	}
	return httpSender, nil
}

func (h *Sender) Name() string {
	return "httpSender_" + h.url + "_"
}

func (h *Sender) Send(data []Data) (err error) {
	var sendBytes []byte
	switch h.protocol {
	case SendProtocolJson:
		if sendBytes, err = h.convertToJsonBytes(data); err != nil {
			return err
		}
	case SendProtocolCSV:
		if sendBytes, err = h.convertToCsvBytes(data); err != nil {
			return err
		}
	case SendProtocolWholeJson:
		if sendBytes, err = h.convertToBodyJsonBytes(data); err != nil {
			return err
		}
	case SendProtocolRaw:
		if sendBytes, err = h.convertToRawBytes(data); err != nil {
			return err
		}
	default:
		return fmt.Errorf("runner[%v] Sender[%v] send data error, protocol %v is not support", h.runnerName, h.Name(), h.protocol)
	}
	return h.sendData(sendBytes)
}

func (h *Sender) Close() error {
	return nil
}

func (h *Sender) renderTemplate(data Data) (string, error) {
	if h.template != "" && h.templateRender != nil {
		return h.templateRender.ExecuteString(data), nil
	} else {
		db, err := jsoniter.Marshal(data)
		if err != nil {
			return "", err
		}
		return string(db), nil
	}
}

func (h *Sender) convertToRawBytes(datas []Data) ([]byte, error) {
	var buf bytes.Buffer
	for _, data := range datas {
		if data["raw"] == nil {
			continue
		}
		switch newVal := data["raw"].(type) {
		case string:
			buf.WriteString(newVal)
			if !strings.HasSuffix(newVal, "\n") {
				buf.WriteString("\n")
			}
		case []byte:
			buf.Write(newVal)
			if !strings.HasSuffix(string(newVal), "\n") {
				buf.WriteString("\n")
			}
		default:
			return nil, fmt.Errorf("value[%v] expect as string or []byte,actual get %T", data["raw"], data["raw"])
		}
	}
	return buf.Bytes(), nil
}

func (h *Sender) convertToJsonBytes(datas []Data) (byteData []byte, err error) {
	dataArray := make([]string, len(datas))
	for i, data := range datas {
		dataArray[i], err = h.renderTemplate(data)
		if err != nil {
			return byteData, err
		}
	}
	byteData = []byte(strings.Join(dataArray, "\n"))
	return byteData, nil
}

func (h *Sender) convertToBodyJsonBytes(datas []Data) (byteData []byte, err error) {
	dataArray := make([]string, len(datas))
	for i, data := range datas {
		dataArray[i], err = h.renderTemplate(data)
		if err != nil {
			return nil, err
		}
	}
	byteData = []byte("[" + strings.Join(dataArray, ",") + "]")
	return byteData, nil
}

func (h *Sender) convertToCsvBytes(datas []Data) (byteData []byte, err error) {
	keySet := NewHashSet()
	for _, data := range datas {
		for k := range data {
			keySet.Add(k)
		}
	}
	keys := keySet.Elements()
	keyNum := keySet.Len()
	keyArray := make([]string, keyNum)
	for i, val := range keys {
		keyArray[i] = val.(string)
	}

	curIndex := 0
	dataArray := make([]string, len(datas)+1)
	if h.csvHead {
		dataArray[curIndex] = strings.Join(keyArray, h.csvSplit)
		curIndex++
	}

	for _, data := range datas {
		tmpArray := make([]interface{}, keyNum)
		for i, key := range keyArray {
			val, ok := data[key]
			if ok {
				tmpArray[i] = val
			} else {
				tmpArray[i] = ""
			}
		}
		if dataArray[curIndex], err = h.interfaceJoin(tmpArray, h.csvSplit); err != nil {
			return
		}
		curIndex++
	}
	byteData = []byte(strings.Join(dataArray, "\n"))
	return
}

func (h *Sender) sendData(byteData []byte) (err error) {
	if h.gZip {
		if byteData, err = gzipData(byteData); err != nil {
			log.Errorf("Runner[%v] Sender[%v] write gzip error %v\n", h.runnerName, h.Name(), err)
			return err
		}
	}
	req, err := http.NewRequest(http.MethodPost, h.url, bytes.NewReader(byteData))
	if err != nil {
		return err
	}
	switch h.protocol {
	case SendProtocolJson, SendProtocolWholeJson:
		req.Header.Set(ContentTypeHeader, ApplicationJson)
	case SendProtocolCSV, SendProtocolRaw:
		req.Header.Set(ContentTypeHeader, TextPlain)
	default:
	}
	if h.gZip {
		req.Header.Set(ContentEncodingHeader, "gzip")
	}
	resp, err := h.client.Do(req)
	if err != nil {
		log.Errorf("Runner[%v] Sender[%v] post data error %v\n", h.runnerName, h.Name(), err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] read response body error %v\n", h.runnerName, h.Name(), err)
			return err
		}
		log.Errorf("Runner[%v] Sender[%v] response code is %v, response body is %v\n", h.runnerName, h.Name(), resp.StatusCode, string(body))
		return fmt.Errorf(string(body))
	}
	return nil
}

func gzipData(datas []byte) (byteData []byte, err error) {
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	if _, err = g.Write(datas); err != nil {
		return
	}
	g.Close()
	byteData = buf.Bytes()
	return
}

func (h *Sender) interfaceJoin(dataArray []interface{}, sep string) (string, error) {
	strData := make([]string, len(dataArray))
	for i, data := range dataArray {
		str, err := pipeline.DataConvert(data, pipeline.RepoSchemaEntry{
			ValueType: pipeline.PandoraTypeString,
		})
		if err != nil {
			log.Errorf("Runner[%v] Sender[%v] convert %v to string error %v, ignored it\n", h.runnerName, h.Name(), data, err)
			continue
		}
		if val, ok := str.(string); ok {
			strData[i] = val
		} else {
			log.Errorf("Runner[%v] Sender[%v] convert %v to string failed, ignored it\n", h.runnerName, h.Name(), data)
		}
	}
	return strings.Join(strData, sep), nil
}

func (_ *Sender) SkipDeepCopy() bool { return true }
