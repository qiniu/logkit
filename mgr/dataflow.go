package mgr

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

//reader模块中各种type的日志都能获取raw_data
func GetRawData(readerConfig conf.MapConf) (rawData string, err error) {
	if readerConfig == nil {
		err = fmt.Errorf("reader config cannot be empty")
		return
	}

	var rd reader.Reader
	rd, err = reader.NewFileBufReader(readerConfig, true)
	if err != nil {
		return
	}

	tryCount := 3
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
		if len(rawData) <= 0 {
			continue
		}
		return
	}

	return
}

//parse模块中各种type的日志都能获取解析后的数据
func GetParsedData(parserConfig conf.MapConf) (parsedData []Data, err error) {
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
