package mgr

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

type Service struct {
	Prefix string
}

var KeySampleLog = "sampleLog"

// PostParseRet 返回值
type PostParseRet struct {
	SamplePoints []Data `json:"SamplePoints"`
}

// post /logkit/parser/parse 接受解析请求
func (rs *RestService) PostParse() echo.HandlerFunc {
	return func(c echo.Context) error {
		reqConf := conf.MapConf{}
		if err := c.Bind(&reqConf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrParseParse, err.Error())
		}
		reqConf = convertWebParserConfig(reqConf)
		nparser, err := parser.NewParserRegistry().NewLogParser(reqConf)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrParseParse, err.Error())
		}
		ptp, _ := reqConf.GetString(parser.KeyParserType)
		rawlogs, _ := reqConf.GetStringOr(KeySampleLog, "")
		var logs []string
		switch ptp {
		case parser.TypeCSV, parser.TypeJson, parser.TypeRaw, parser.TypeNginx, parser.TypeEmpty, parser.TypeKafkaRest, parser.TypeLogv1:
			logs = strings.Split(rawlogs, "\n")
		case parser.TypeSyslog:
			logs = strings.Split(rawlogs, "\n")
			logs = append(logs, parser.SyslogEofLine)
		case parser.TypeGrok:
			gm, _ := reqConf.GetString(parser.KeyGrokMode)
			if gm != parser.ModeMulti {
				logs = strings.Split(rawlogs, "\n")
			} else {
				logs = []string{rawlogs}
			}
		default:
			errMsg := fmt.Sprintf("parser type <%v> is not supported yet", ptp)
			return RespError(c, http.StatusBadRequest, ErrParseParse, errMsg)
		}
		datas, err := nparser.Parse(logs)
		se, ok := err.(*utils.StatsError)
		if ok {
			err = se.ErrorDetail
		}
		if err != nil {
			errMsg := fmt.Sprintf("parser error %v", err)
			return RespError(c, http.StatusBadRequest, ErrParseParse, errMsg)
		}
		return RespSuccess(c, PostParseRet{SamplePoints: datas})
	}
}

// get /logkit/parser/usages 获得解析用途说明
func (rs *RestService) GetParserUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, parser.ModeUsages)
	}
}

// get /logkit/parser/options 获取解析选项
func (rs *RestService) GetParserKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, parser.ModeKeyOptions)
	}
}

// get /logkit/parser/samplelogs 获取样例日志
func (rs *RestService) GetParserSampleLogs() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, parser.SampleLogs)
	}
}

// POST /logkit/parser/check
func (rs *RestService) PostParserCheck() echo.HandlerFunc {
	return func(c echo.Context) error {
		reqConf := conf.MapConf{}
		if err := c.Bind(&reqConf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrParseParse, err.Error())
		}
		_, err := parser.NewParserRegistry().NewLogParser(reqConf)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrParseParse, err.Error())
		}
		return RespSuccess(c, nil)
	}
}
