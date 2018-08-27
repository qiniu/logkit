package mgr

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/parser/grok"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/router"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	DefaultTryTimes = 3
	MetaTmp         = "meta_tmp/"
)

// RawData 从 reader 模块中根据 type 获取字符串形式的样例日志
func RawData(readerConfig conf.MapConf) (string, error) {
	if readerConfig == nil {
		return "", fmt.Errorf("reader config cannot be empty")
	}

	runnerName, _ := readerConfig.GetString(GlobalKeyName)
	configMetaPath := runnerName + "_" + Hash(strconv.FormatInt(time.Now().Unix(), 10))
	metaPath := filepath.Join(MetaTmp, configMetaPath)
	log.Debugf("Runner[%v] Using %s as default metaPath", runnerName, metaPath)
	readerConfig[reader.KeyMetaPath] = metaPath

	rd, err := reader.NewReader(readerConfig, true)
	if err != nil {
		return "", err
	}
	defer func() {
		rd.Close()
		os.RemoveAll(metaPath)
	}()

	if dr, ok := rd.(reader.DaemonReader); ok {
		if err := dr.Start(); err != nil {
			return "", err
		}
	}

	var timeoutStatus int32
	type dataErr struct {
		data string
		err  error
	}
	// Note: 添加一位缓冲保证 goroutine 在 runner 已经超时的情况下能够正常退出，避免资源泄露
	readChan := make(chan dataErr, 1)
	go func() {
		if dr, ok := rd.(reader.DataReader); ok {
			data, _, err := dr.ReadData()
			if err != nil && err != io.EOF {
				readChan <- dataErr{"", err}
				return
			}

			p, err := jsoniter.MarshalIndent(data, "", "  ")
			readChan <- dataErr{string(p), err}
			return
		}

		// ReadLine 是可能读到空值的，在接收器宣布超时或读取到数据之前需要不停循环读取
		for atomic.LoadInt32(&timeoutStatus) == 0 {
			str, err := rd.ReadLine()
			if err != nil && err != io.EOF {
				readChan <- dataErr{"", err}
				return
			}
			if err == io.EOF || len(str) > 0 {
				readChan <- dataErr{str, nil}
				return
			}
		}
	}()

	var rawData string
	timeout := time.NewTimer(time.Minute)
	select {
	case de := <-readChan:
		rawData, err = de.data, de.err
		if err != nil {
			return "", fmt.Errorf("reader %q - error: %v", rd.Name(), err)
		}

	case <-timeout.C:
		atomic.StoreInt32(&timeoutStatus, 1)
		return "", fmt.Errorf("reader %q read timeout, no data received", rd.Name())
	}

	if len(rawData) >= DefaultMaxBatchSize {
		err = errors.New("data size large than 2M and will be discard")
		return "", fmt.Errorf("reader %q - error: %v", rd.Name(), err)
	}
	return rawData, nil
}

//parse模块中各种type的日志都能获取解析后的数据
func ParseData(parserConfig conf.MapConf) (parsedData []Data, err error) {
	parserConfig = parser.ConvertWebParserConfig(parserConfig)
	if parserConfig == nil {
		err = fmt.Errorf("parser config was empty after web config convet")
		return
	}

	sampleData, err := getSampleData(parserConfig)
	if err != nil {
		return nil, err
	}
	parserRegistry := parser.NewRegistry()
	logParser, err := parserRegistry.NewLogParser(parserConfig)
	if err != nil {
		return nil, err
	}
	sampleData, err = checkSampleData(sampleData, logParser)
	if err != nil {
		return nil, err
	}

	parsedData, err = logParser.Parse(sampleData)
	if err != nil {
		return parsedData, CheckErr(err)
	}

	return parsedData, nil
}

func TransformData(transformerConfig map[string]interface{}) ([]Data, error) {
	if transformerConfig == nil {
		err := fmt.Errorf("transformer config cannot be empty")
		return nil, err
	}

	create, err := getTransformerCreator(transformerConfig)
	if err != nil {
		return nil, err
	}

	data, err := getDataFromTransformConfig(transformerConfig)
	if err != nil {
		return nil, err
	}

	transformer, err := getTransformer(transformerConfig, create)
	if err != nil {
		return nil, err
	}

	// Transform data
	transformedData, transErr := transformer.Transform(data)
	se, ok := transErr.(*StatsError)
	if ok {
		transErr = se.ErrorDetail
	}
	if transErr != nil {
		se, ok := transErr.(*StatsError)
		if ok {
			transErr = se.ErrorDetail
		}
		err := fmt.Errorf("transform processing error %v", transErr)
		return nil, err
	}
	return transformedData, nil
}

func SendData(senderConfig map[string]interface{}) error {
	if senderConfig == nil {
		return fmt.Errorf("sender config cannot be empty")
	}

	sendersConf, err := getSendersConfig(senderConfig)
	if err != nil {
		return err
	}

	senders, err := getSenders(sendersConf)
	defer os.RemoveAll(filepath.Join(MetaTmp, reader.FtSaveLogPath))
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range senders {
			s.Close()
		}
	}()

	router, err := getRouter(senderConfig, len(senders))
	if err != nil {
		return err
	}

	datas, err := getDataFromSenderConfig(senderConfig)
	if err != nil {
		return err
	}

	senderDataList := classifySenderData(senders, datas, router)
	for index, s := range senders {
		if err := trySend(s, senderDataList[index], DefaultTryTimes); err != nil {
			return err
		}
	}
	return nil
}

func getSendersConfig(senderConfig map[string]interface{}) ([]conf.MapConf, error) {
	config := senderConfig[KeySendConfig]
	byteSendersConfig, err := jsoniter.Marshal(config)
	if err != nil {
		return nil, err
	}
	var sendersConf []conf.MapConf
	if jsonErr := jsoniter.Unmarshal(byteSendersConfig, &sendersConf); jsonErr != nil {
		return nil, jsonErr
	}
	if sendersConf == nil {
		return nil, fmt.Errorf("sender config cannot be empty")
	}
	return sendersConf, nil
}

func getDataFromSenderConfig(senderConfig map[string]interface{}) ([]Data, error) {
	var datas = []Data{}
	rawData := senderConfig[KeySampleLog]
	rawDataStr, ok := rawData.(string)
	if !ok {
		err := fmt.Errorf("missing param %s", KeySampleLog)
		return nil, err
	}
	if rawDataStr == "" {
		err := fmt.Errorf("sender fetched empty sample log")
		return nil, err
	}

	if jsonErr := jsoniter.Unmarshal([]byte(rawDataStr), &datas); jsonErr != nil {
		return nil, jsonErr
	}
	return datas, nil
}

func getSenders(sendersConf []conf.MapConf) ([]sender.Sender, error) {
	senders := make([]sender.Sender, 0)
	sr := sender.NewRegistry()
	ftSaveLogPath := filepath.Join(MetaTmp, reader.FtSaveLogPath)
	for i, senderConfig := range sendersConf {
		senderConfig[sender.KeyFaultTolerant] = "false"
		senderConfig[sender.KeyFtSaveLogPath] = ftSaveLogPath
		s, err := sr.NewSender(senderConfig, "")
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
		delete(sendersConf[i], sender.InnerUserAgent)
	}
	return senders, nil
}

func getRouter(senderConfig map[string]interface{}, senderCnt int) (*router.Router, error) {
	routerConf, err := getRouterConfig(senderConfig)
	if err != nil {
		return nil, err
	}

	router, err := router.NewSenderRouter(routerConf, senderCnt)
	if err != nil {
		return nil, fmt.Errorf("sender router error, %v", err)
	}
	return router, nil
}

func getRouterConfig(senderConfig map[string]interface{}) (router.RouterConfig, error) {
	config := senderConfig[KeyRouterConfig]
	byteRouterConfig, err := jsoniter.Marshal(config)
	if err != nil {
		return router.RouterConfig{}, err
	}

	var routerConfig router.RouterConfig
	if jsonErr := jsoniter.Unmarshal(byteRouterConfig, &routerConfig); jsonErr != nil {
		return router.RouterConfig{}, jsonErr
	}
	return routerConfig, nil
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func trySend(s sender.Sender, datas []Data, times int) (err error) {
	if times <= 0 {
		times = DefaultTryTimes
	}
	if len(datas) <= 0 {
		return nil
	}
	cnt := 0
	for {
		if cnt >= times {
			break
		}
		err = s.Send(datas)
		if se, ok := err.(*StatsError); ok {
			err = se.ErrorDetail
		}

		if err != nil {
			cnt++
			se, ok := err.(*reqerr.SendError)
			if ok {
				datas = sender.ConvertDatas(se.GetFailDatas())
				continue
			}
			if cnt < times {
				continue
			}
			err = fmt.Errorf("retry send %v times, but still error %v, discard datas %v ... total %v lines", cnt, err, datas, len(datas))
			return err
		}
		break
	}
	return
}

func getSampleData(parserConfig conf.MapConf) ([]string, error) {
	parserType, _ := parserConfig.GetString(parser.KeyParserType)
	rawData, _ := parserConfig.GetStringOr(KeySampleLog, "")
	var sampleData []string

	switch parserType {
	case parser.TypeCSV, parser.TypeJSON, parser.TypeRaw, parser.TypeNginx, parser.TypeEmpty, parser.TypeKafkaRest, parser.TypeLogv1:
		sampleData = append(sampleData, rawData)
	case parser.TypeSyslog:
		sampleData = strings.Split(rawData, "\n")
		sampleData = append(sampleData, parser.PandoraParseFlushSignal)
	case parser.TypeMySQL:
		sampleData = strings.Split(rawData, "\n")
		sampleData = append(sampleData, parser.PandoraParseFlushSignal)
	case parser.TypeGrok:
		grokMode, _ := parserConfig.GetString(parser.KeyGrokMode)
		if grokMode != grok.ModeMulti {
			sampleData = append(sampleData, rawData)
		} else {
			sampleData = []string{rawData}
		}
	default:
		sampleData = append(sampleData, rawData)
	}

	return sampleData, nil
}

func checkSampleData(sampleData []string, logParser parser.Parser) ([]string, error) {
	if len(sampleData) <= 0 {
		_, ok := logParser.(parser.Flushable)
		if ok {
			sampleData = []string{parser.PandoraParseFlushSignal}
		} else {
			err := fmt.Errorf("parser [%v] fetched 0 lines", logParser.Name())
			return nil, err
		}
	}
	return sampleData, nil
}

func getTransformerCreator(transformerConfig map[string]interface{}) (transforms.Creator, error) {
	transformKeyType, ok := transformerConfig[KeyType]
	if !ok {
		err := fmt.Errorf("missing param %s", KeyType)
		return nil, err
	}
	transformKeyTypeStr, ok := transformKeyType.(string)
	if !ok {
		err := fmt.Errorf("param %s must be of type string", KeyType)
		return nil, err
	}

	create, ok := transforms.Transformers[transformKeyTypeStr]
	if !ok {
		err := fmt.Errorf("transformer of type %v not exist", transformKeyTypeStr)
		return nil, err
	}
	return create, nil
}

func getDataFromTransformConfig(transformerConfig map[string]interface{}) ([]Data, error) {
	rawData, ok := transformerConfig[KeySampleLog]
	if !ok {
		err := fmt.Errorf("missing param %s", KeySampleLog)
		return nil, err
	}
	rawDataStr, ok := rawData.(string)
	if !ok {
		err := fmt.Errorf("missing param %s", KeySampleLog)
		return nil, err
	}
	if rawDataStr == "" {
		err := fmt.Errorf("transformer fetched empty sample log")
		return nil, err
	}

	var data = []Data{}
	var singleData Data
	if jsonErr := jsoniter.Unmarshal([]byte(rawDataStr), &singleData); jsonErr != nil {
		if jsonErr = jsoniter.Unmarshal([]byte(rawDataStr), &data); jsonErr != nil {
			return nil, jsonErr
		}
	} else {
		// is single log, and method transformer.transform(data []sender.Data) accept a param of slice type
		data = append(data, singleData)
	}
	return data, nil
}

func getTransformer(transConfig map[string]interface{}, create transforms.Creator) (transforms.Transformer, error) {
	trans := create()
	transformerConfig := convertWebTransformerConfig(transConfig)
	delete(transformerConfig, KeySampleLog)
	bts, jsonErr := jsoniter.Marshal(transformerConfig)
	if jsonErr != nil {
		return nil, jsonErr
	}
	if jsonErr = jsoniter.Unmarshal(bts, trans); jsonErr != nil {
		return nil, jsonErr
	}

	if trans, ok := trans.(transforms.Initializer); ok {
		if err := trans.Init(); err != nil {
			return nil, err
		}
	}
	return trans, nil
}
