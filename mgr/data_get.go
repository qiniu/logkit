package mgr

import (
	"errors"
	"fmt"
	"io"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
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
			log.Errorf("reader %s - error: %v", rd.Name(), err)
			break
		}
		if len(rawData) <= 0 {
			log.Debugf("reader %s no more content fetched sleep 1 second...", rd.Name())
			continue
		}
		return
	}

	return
}

//parse模块中各种type的日志都能获取解析后的数据
func GetParseData(parserConfig conf.MapConf, sampleData []string) (parseData []sender.Data, err error) {
	if parserConfig == nil {
		err = fmt.Errorf("parser config cannot be empty")
		return
	}

	pr := parser.NewParserRegistry()
	ps, err := pr.NewLogParser(parserConfig)
	if err != nil {
		return nil, err
	}

	if len(sampleData) <= 0 {
		log.Debugf("parser [%v] fetched 0 lines", ps.Name())
		pt, ok := ps.(parser.ParserType)
		if ok && pt.Type() == parser.TypeSyslog {
			sampleData = []string{parser.SyslogEofLine}
		} else {
			err = fmt.Errorf("parser [%v] fetched 0 lines", ps.Name())
			return nil, err
		}
	}

	parseData, err = ps.Parse(sampleData)
	se, ok := err.(*utils.StatsError)
	var errorCnt int64
	if ok {
		errorCnt = se.Errors
		err = se.ErrorDetail
	} else if err != nil {
		errorCnt = 1
	}
	if err != nil {
		errMsg := fmt.Sprintf("parser %s error : %v ", ps.Name(), err.Error())
		err = errors.New(errMsg)
		log.Errorf("%v parse line errors occured, same as %v", errorCnt, err)
	}

	if len(parseData) <= 0 {
		err = fmt.Errorf("received parsed data length = 0")
		return nil, err
	}

	return
}
