package mgr

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
)

type Service struct {
	Prefix string
}

var KeySampleLog = "sampleLog"

// PostParseRet 返回值
type PostParseRet struct {
	SamplePoints []sender.Data `json:"SamplePoints"`
}

// post /logkit/parser/parse 接受解析请求
func (rs *RestService) PostParse() echo.HandlerFunc {
	return func(c echo.Context) error {
		reqConf := conf.MapConf{}
		if err := c.Bind(&reqConf); err != nil {
			return err
		}
		reqConf = convertWebParserConfig(reqConf)
		nparser, err := parser.NewParserRegistry().NewLogParser(reqConf)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		ptp, _ := reqConf.GetString(parser.KeyParserType)
		rawlogs, _ := reqConf.GetStringOr(KeySampleLog, "")
		var logs []string
		switch ptp {
		case parser.TypeCSV, parser.TypeJson, parser.TypeRaw, parser.TypeNginx, parser.TypeEmpty, parser.TypeKafkaRest, parser.TypeLogv1:
			logs = strings.Split(rawlogs, "\n")
		case parser.TypeGrok:
			gm, _ := reqConf.GetString(parser.KeyGrokMode)
			if gm != parser.ModeMulti {
				logs = strings.Split(rawlogs, "\n")
			} else {
				logs = []string{rawlogs}
			}
		default:
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("parser type <%v> is not supported yet", ptp))
		}
		datas, err := nparser.Parse(logs)
		se, ok := err.(*utils.StatsError)
		if ok {
			err = se.ErrorDetail
		}
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("parser type error %v", err))
		}
		return c.JSON(http.StatusOK, PostParseRet{SamplePoints: datas})
	}
}

// get /logkit/parser/usages 接受解析请求
func (rs *RestService) GetParserUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, parser.ModeUsages)
	}
}

// get /logkit/parser/options 接受解析请求
func (rs *RestService) GetParserKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, parser.ModeKeyOptions)
	}
}

// get /logkit/parser/samplelogs 接受解析请求
func (rs *RestService) GetParserSampleLogs() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, parser.SampleLogs)
	}
}
