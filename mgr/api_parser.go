package mgr

import (
	"fmt"
	"net/http"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/labstack/echo"
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
		parserConfig := conf.MapConf{}
		if err := c.Bind(&parserConfig); err != nil {
			return RespError(c, http.StatusBadRequest, ErrParseParse, err.Error())
		}

		parseData, err := ParseData(parserConfig)
		se, ok := err.(*StatsError)
		if ok {
			err = se.ErrorDetail
		}
		if err != nil {
			errMsg := fmt.Sprintf("parser error %v", err)
			return RespError(c, http.StatusBadRequest, ErrParseParse, errMsg)
		}

		return RespSuccess(c, PostParseRet{SamplePoints: parseData})
	}
}

// get /logkit/parser/usages 获得解析用途说明
func (rs *RestService) GetParserUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, parser.ModeUsages)
	}
}

// get /logkit/parser/tooltips 获取解析用途提示
func (rs *RestService) GetParserTooltips() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, parser.ModeToolTips)
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
		_, err := parser.NewRegistry().NewLogParser(reqConf)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrParseParse, err.Error())
		}
		return RespSuccess(c, nil)
	}
}
