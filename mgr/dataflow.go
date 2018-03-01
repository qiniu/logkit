package mgr

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/json-iterator/go"
)

const DefaultTryTimes = 3

//reader模块中各种type的日志都能获取raw_data
func RawData(readerConfig conf.MapConf) (rawData string, err error) {
	if readerConfig == nil {
		err = fmt.Errorf("reader config cannot be empty")
		return
	}

	var rd reader.Reader
	rd, err = reader.NewFileBufReader(readerConfig, true)
	if err != nil {
		return
	}

	tryCount := DefaultTryTimes
	for {
		if tryCount <= 0 {
			err = fmt.Errorf("get raw data time out, raw data is empty")
			return
		}
		tryCount--
		rawData, err = rd.ReadLine()
		if err != nil && err != io.EOF {
			return "", fmt.Errorf("reader %s - error: %v", rd.Name(), err)
		}
		if rawData == "" || len(rawData) > defaultMaxBatchSize {
			continue
		}
		return
	}

	return
}

//parse模块中各种type的日志都能获取解析后的数据
func ParseData(parserConfig conf.MapConf) (parsedData []Data, err error) {
	parserConfig = convertWebParserConfig(parserConfig)
	if parserConfig == nil {
		err = fmt.Errorf("parser config was empty after web config convet")
		return
	}

	sampleData, err := getSampleData(parserConfig)
	if err != nil {
		return nil, err
	}
	parserRegistry := parser.NewParserRegistry()
	logParser, err := parserRegistry.NewLogParser(parserConfig)
	if err != nil {
		return nil, err
	}
	sampleData, err = checkSampleData(sampleData, logParser)
	if err != nil {
		return nil, err
	}

	parsedData, err = logParser.Parse(sampleData)
	err = checkErr(err, logParser.Name())
	if len(parsedData) <= 0 {
		err = fmt.Errorf("received parsed data length = 0, err : %v", err)
		return nil, err
	}

	return
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
	se, ok := transErr.(*utils.StatsError)
	if ok {
		transErr = se.ErrorDetail
	}
	if transErr != nil {
		se, ok := transErr.(*utils.StatsError)
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
	if err != nil {
		return err
	}

	senderCnt := len(senders)
	router, err := getRouter(senderConfig, senderCnt)
	if err != nil {
		return err
	}

	datas, err := getDataFromSenderConfig(senderConfig)
	if err != nil {
		return err
	}

	senderDataList := classifySenderData(datas, router, len(senders))
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
	sr := sender.NewSenderRegistry()
	for i, senderConfig := range sendersConf {
		senderConfig[sender.KeyFaultTolerant] = "false"
		s, err := sr.NewSender(senderConfig, "")
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
		delete(sendersConf[i], sender.InnerUserAgent)
	}
	return senders, nil
}

func getRouter(senderConfig map[string]interface{}, senderCnt int) (*sender.Router, error) {
	routerConf, err := getRouterConfig(senderConfig)
	if err != nil {
		return nil, err
	}

	router, err := sender.NewSenderRouter(routerConf, senderCnt)
	if err != nil {
		return nil, fmt.Errorf("sender router error, %v", err)
	}
	return router, nil
}

func getRouterConfig(senderConfig map[string]interface{}) (sender.RouterConfig, error) {
	config := senderConfig[KeyRouterConfig]
	byteRouterConfig, err := jsoniter.Marshal(config)
	if err != nil {
		return sender.RouterConfig{}, err
	}

	var routerConfig sender.RouterConfig
	if jsonErr := jsoniter.Unmarshal(byteRouterConfig, &routerConfig); jsonErr != nil {
		return sender.RouterConfig{}, jsonErr
	}
	return routerConfig, nil
}

// trySend 尝试发送数据，如果此时runner退出返回false，其他情况无论是达到最大重试次数还是发送成功，都返回true
func trySend(s sender.Sender, datas []Data, times int) error {
	if len(datas) <= 0 {
		return nil
	}
	cnt := 1
	for {
		err := s.Send(datas)
		if se, ok := err.(*utils.StatsError); ok {
			err = se.ErrorDetail
		}

		if err != nil {
			se, ok := err.(*reqerr.SendError)
			if ok {
				datas = sender.ConvertDatas(se.GetFailDatas())
				cnt++
				continue
			}
			if times <= 0 || cnt < times {
				cnt++
				continue
			}
			err = fmt.Errorf("retry send %v times, but still error %v, discard datas %v ... total %v lines", cnt, err, datas, len(datas))
			return err
		}
		break
	}

	return nil
}

func getSampleData(parserConfig conf.MapConf) ([]string, error) {
	parserType, _ := parserConfig.GetString(parser.KeyParserType)
	rawData, _ := parserConfig.GetStringOr(KeySampleLog, "")
	var sampleData []string

	switch parserType {
	case parser.TypeCSV, parser.TypeJson, parser.TypeRaw, parser.TypeNginx, parser.TypeEmpty, parser.TypeKafkaRest, parser.TypeLogv1:
		sampleData = strings.Split(rawData, "\n")
	case parser.TypeSyslog:
		sampleData = strings.Split(rawData, "\n")
		sampleData = append(sampleData, parser.SyslogEofLine)
	case parser.TypeGrok:
		grokMode, _ := parserConfig.GetString(parser.KeyGrokMode)
		if grokMode != parser.ModeMulti {
			sampleData = strings.Split(rawData, "\n")
		} else {
			sampleData = []string{rawData}
		}
	default:
		errMsg := fmt.Sprintf("parser type <%v> is not supported yet", parserType)
		return nil, errors.New(errMsg)
	}

	return sampleData, nil
}

func checkSampleData(sampleData []string, logParser parser.LogParser) ([]string, error) {
	if len(sampleData) <= 0 {
		pt, ok := logParser.(parser.ParserType)
		if ok && pt.Type() == parser.TypeSyslog {
			sampleData = []string{parser.SyslogEofLine}
		} else {
			err := fmt.Errorf("parser [%v] fetched 0 lines", logParser.Name())
			return nil, err
		}
	}
	return sampleData, nil
}

func checkErr(err error, parserName string) error {
	se, ok := err.(*utils.StatsError)
	var errorCnt int64
	if ok {
		errorCnt = se.Errors
		err = se.ErrorDetail
	} else if err != nil {
		errorCnt = 1
	}
	if err != nil {
		errMsg := fmt.Sprintf("parser %s, error %v ", parserName, err.Error())
		err = fmt.Errorf("%v parse line errors occured, same as %v", errorCnt, errors.New(errMsg))
	}

	return err
}

func getTransformerCreator(transformerConfig map[string]interface{}) (transforms.Creator, error) {
	transformKeyType, ok := transformerConfig[transforms.KeyType]
	if !ok {
		err := fmt.Errorf("missing param %s", transforms.KeyType)
		return nil, err
	}
	transformKeyTypeStr, ok := transformKeyType.(string)
	if !ok {
		err := fmt.Errorf("param %s must be of type string", transforms.KeyType)
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

	if trans, ok := trans.(transforms.Initialize); ok {
		if err := trans.Init(); err != nil {
			return nil, err
		}
	}
	return trans, nil
}
