package mgr

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/grok"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/router"
	"github.com/qiniu/logkit/sender"
	senderConf "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	DefaultTryTimes = 3
	MetaTmp         = "meta_tmp/"

	DefaultRawDataBatchLen = 10
	RawDataMaxBatchLines   = 100
	DefaultRawDataSize     = 16 * 1024
	DefaultRawDataTimeout  = 30
)

// RawData 从 reader 模块中根据 type 获取多条字符串形式的样例日志
func RawData(readerConfig conf.MapConf) ([]string, error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("recover when exec RawData\npanic: %v\nstack: %s", rec, debug.Stack())
		}
	}()
	if readerConfig == nil {
		return nil, errors.New("reader config cannot be empty")
	}

	runnerName, _ := readerConfig.GetString(GlobalKeyName)
	rawDataTimeOut, _ := readerConfig.GetIntOr(config.KeyRawDataTimeout, DefaultRawDataTimeout)
	if rawDataTimeOut < 10 || rawDataTimeOut > 300 {
		rawDataTimeOut = DefaultRawDataTimeout
	}
	configMetaPath := runnerName + "_" + Hash(strconv.FormatInt(time.Now().Unix(), 10))
	metaPath := filepath.Join(MetaTmp, configMetaPath)
	log.Debugf("Runner[%v] Using %s as default metaPath", runnerName, metaPath)
	readerConfig[config.KeyMetaPath] = metaPath
	size, _ := readerConfig.GetIntOr("raw_data_lines", DefaultRawDataBatchLen)
	// 控制最大条数为 100
	if size > RawDataMaxBatchLines {
		size = RawDataMaxBatchLines
	}

	rd, err := reader.NewReader(readerConfig, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		rd.Close()
		os.RemoveAll(metaPath)
	}()

	if dr, ok := rd.(reader.DaemonReader); ok {
		if err := dr.Start(); err != nil {
			return nil, err
		}
	}

	var timeoutStatus int32
	// Note: 添加一位缓冲保证 goroutine 在 runner 已经超时的情况下能够正常退出，避免资源泄露
	readChan := make(chan dataResult, 1)
	go func() {
		if dr, ok := rd.(reader.DataReader); ok {
			for atomic.LoadInt32(&timeoutStatus) == 0 {
				res := readDatas(dr, size, &timeoutStatus)
				if len(res.data) == 0 && res.lastErr == nil {
					continue
				}
				readChan <- res
				return
			}
		}

		// ReadLine 是可能读到空值的，在接收器宣布超时或读取到数据之前需要不停循环读取
		var lastErr error
		for atomic.LoadInt32(&timeoutStatus) == 0 {
			res := readLines(rd, size, &timeoutStatus)
			if (len(res.data) == 0 && res.lastErr == nil) || os.IsNotExist(res.lastErr) {
				if res.lastErr != nil {
					lastErr = res.lastErr
				}
				continue
			}
			readChan <- res
			return
		}
		if atomic.LoadInt32(&timeoutStatus) == 1 {
			if lastErr != nil {
				readChan <- dataResult{lastErr: lastErr}
				return
			}
		}

	}()

	var rawData []string
	timeout := time.NewTimer(time.Duration(rawDataTimeOut) * time.Second)
	defer timeout.Stop()
	select {
	case de := <-readChan:
		rawData, err = de.data, de.lastErr
		if err != nil {
			return nil, fmt.Errorf("reader %q - error: %v", rd.Name(), err)
		}

	case <-timeout.C:
		atomic.StoreInt32(&timeoutStatus, 1)
		return readRawData(readChan, rd.Name())
	}

	if len(rawData) >= DefaultMaxBatchSize {
		err = errors.New("data size large than 2M and will be discard")
		return nil, fmt.Errorf("reader %q - error: %v", rd.Name(), err)
	}
	return rawData, nil
}

//parse模块中各种type的日志都能获取解析后的数据
func ParseData(parserConfig conf.MapConf) (parsedData []Data, err error) {
	parserConfig = parser.ConvertWebParserConfig(parserConfig)
	if parserConfig == nil {
		return nil, errors.New("parser config was empty after web config convet")
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
		return nil, errors.New("transformer config cannot be empty")
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

	if transformer.Type() == "script" {
		return nil, errors.New("该transformer暂不支持页面上预览")
	}

	// Transform data
	transformedData, transErr := transformer.Transform(data)
	se, ok := transErr.(*StatsError)
	if ok {
		transErr = errors.New(se.LastError)
	}
	if transErr != nil {
		se, ok := transErr.(*StatsError)
		if ok {
			transErr = errors.New(se.LastError)
		}
		return nil, fmt.Errorf("transform processing error %v", transErr)
	}
	return transformedData, nil
}

func SendData(senderConfig map[string]interface{}) error {
	if senderConfig == nil {
		return errors.New("sender config cannot be empty")
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
		return nil, errors.New("sender config cannot be empty")
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
		return nil, errors.New("sender fetched empty sample log")
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
		senderConfig[senderConf.KeyFaultTolerant] = "false"
		senderConfig[senderConf.KeyFtSaveLogPath] = ftSaveLogPath
		senderConfig[senderConf.KeySenderTest] = "true"
		s, err := sr.NewSender(senderConfig, "")
		if err != nil {
			return nil, err
		}
		senders = append(senders, s)
		delete(sendersConf[i], senderConf.InnerUserAgent)
		delete(sendersConf[i], senderConf.KeySenderTest)
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
		err = s.Send(datas)
		if err == nil {
			break
		}

		if se, ok := err.(*StatsError); ok {
			if se.Errors == 0 {
				break
			}
			if se.SendError != nil {
				err = se.SendError
			}
		}

		if se, ok := err.(*reqerr.SendError); ok {
			datas = sender.ConvertDatas(se.GetFailDatas())
		}

		cnt++
		if cnt < times {
			continue
		}
		err = fmt.Errorf("retry send %v times, but still error %v, discard datas... total %v lines", cnt, err, len(datas))
		return err
	}
	return nil
}

func getSampleData(parserConfig conf.MapConf) ([]string, error) {
	parserType, err := parserConfig.GetString(KeyParserType)
	if err != nil {
		return []string{}, err
	}
	rawData, _ := parserConfig.GetStringOr(KeySampleLog, "")
	var sampleData []string

	switch parserType {
	case TypeCSV, TypeJSON, TypeRaw, TypeNginx, TypeEmpty, TypeKafkaRest, TypeLogv1:
		sampleData = append(sampleData, rawData)
	case TypeSyslog:
		sampleData = strings.Split(rawData, "\n")
		sampleData = append(sampleData, PandoraParseFlushSignal)
	case TypeMySQL:
		sampleData = strings.Split(rawData, "\n")
		sampleData = append(sampleData, PandoraParseFlushSignal)
	case TypeGrok:
		grokMode, _ := parserConfig.GetString(KeyGrokMode)
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
			sampleData = []string{PandoraParseFlushSignal}
		} else {
			return nil, errors.New("parser [" + logParser.Name() + "] fetched 0 lines")
		}
	}
	return sampleData, nil
}

func getTransformerCreator(transformerConfig map[string]interface{}) (transforms.Creator, error) {
	transformKeyType, ok := transformerConfig[KeyType]
	if !ok {
		return nil, fmt.Errorf("missing param %s", KeyType)
	}
	transformKeyTypeStr, ok := transformKeyType.(string)
	if !ok {
		return nil, fmt.Errorf("param %s must be of type string", KeyType)
	}

	create, ok := transforms.Transformers[transformKeyTypeStr]
	if !ok {
		return nil, fmt.Errorf("transformer of type %v not exist", transformKeyTypeStr)
	}
	return create, nil
}

func getDataFromTransformConfig(transformerConfig map[string]interface{}) ([]Data, error) {
	rawData, ok := transformerConfig[KeySampleLog]
	if !ok {
		return nil, errors.New("missing param " + KeySampleLog)
	}
	rawDataStr, ok := rawData.(string)
	if !ok {
		return nil, fmt.Errorf("expect %s string, but got %T", KeySampleLog, rawData)
	}
	if rawDataStr == "" {
		return nil, errors.New("transformer fetched empty sample log")
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

type dataResult struct {
	data    []string
	lastErr error
}

func readDatas(dr reader.DataReader, size int, timeoutStatus *int32) dataResult {
	var (
		batchLen, batchSize int
		datas               []string
		err                 error
		bytes               int64
		data                Data
		jsonData            []byte
	)
	datas = make([]string, 0, size)
	for !batchFullOrTimeout(batchLen, size, timeoutStatus) {
		data, _, err = dr.ReadData()
		if err != nil && err != io.EOF {
			return dataResult{datas, err}
		}
		if len(data) < 1 {
			log.Debugf("data read got empty data")
			time.Sleep(time.Second)
			continue
		}

		jsonData, err = jsoniter.MarshalIndent(data, "", "  ")
		if err != nil {
			break
		}
		datas = append(datas, string(jsonData))
		batchLen++
		batchSize += int(bytes)
	}

	return dataResult{datas, err}
}

func readLines(rd reader.Reader, size int, timeoutStatus *int32) dataResult {
	var (
		batchLen, batchSize int
		line                string
		err                 error
	)
	lines := make([]string, 0, size)
	for !batchFullOrTimeout(batchLen, size, timeoutStatus) {
		line, err = rd.ReadLine()
		if os.IsNotExist(err) {
			log.Debugf("data read error: %v", err)
			time.Sleep(3 * time.Second)
			break
		}
		if err != nil && err != io.EOF {
			return dataResult{lines, err}
		}
		if err == io.EOF {
			err = nil
		}

		if len(line) < 1 {
			log.Debugf("no more content fetched sleep 1 second...")
			time.Sleep(time.Second)
			continue
		}

		line = checkLineSize(line, DefaultRawDataSize)
		lines = append(lines, line)
		batchLen++
		batchSize += len(line)
	}

	return dataResult{lines, err}
}

func batchFullOrTimeout(batchLen, size int, timeoutStatus *int32) bool {
	// 达到最大行数
	if DefaultRawDataBatchLen > 0 && batchLen >= size {
		log.Debugf("meet the max batch length %v", size)
		return true
	}

	if atomic.LoadInt32(timeoutStatus) == 1 {
		return true
	}

	return false
}

func checkLineSize(rawData string, size int) string {
	if len(rawData) <= size {
		return rawData
	}

	return rawData[:size] +
		fmt.Sprintf(" ......(only show %d bytes, remain %d bytes)",
			size, size)
}

// readChan 也是通过 timeoutStatus 判断是否超时，超时之后返回的值需要进一步获取，因此等待一秒之后获取 readChan 的值
func readRawData(readChan chan dataResult, name string) ([]string, error) {
	time.Sleep(time.Second)
	select {
	case rc := <-readChan:
		rawData, err := rc.data, rc.lastErr
		if err != nil {
			return nil, fmt.Errorf("reader %q - error: %v", name, err)
		}
		return rawData, err
	default:
		return nil, fmt.Errorf("reader %q read timeout, no data received", name)
	}
}
