package mgr

import (
	"net/http"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/labstack/echo"
)

// get /logkit/reader/usages 获取Reader用途
func (rs *RestService) GetReaderUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, reader.ModeUsages)
	}
}

// get /logkit/reader/options 获取Reader参数配置
func (rs *RestService) GetReaderKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, reader.ModeKeyOptions)
	}
}

// POST /logkit/reader/read 请求校验reader配置
func (rs *RestService) PostRead() echo.HandlerFunc {
	return func(c echo.Context) error {
		var readerConf conf.MapConf // request body params in map format
		if err := c.Bind(&readerConf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrReadRead, err.Error())
		}
		rawData, err := RawData(readerConf)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrReadRead, err.Error())
		}

		return RespSuccess(c, rawData)
	}
}

// POST /logkit/reader/check 请求校验reader配置
func (rs *RestService) PostReaderCheck() echo.HandlerFunc {
	return func(c echo.Context) error {
		var readerConf conf.MapConf // request body params in map format
		if err := c.Bind(&readerConf); err != nil {
			return RespError(c, http.StatusBadRequest, ErrReadRead, err.Error())
		}
		_, err := reader.NewFileBufReader(readerConf, true)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrReadRead, err.Error())
		}

		return RespSuccess(c, nil)
	}
}
