package mgr

import (
	"github.com/labstack/echo"
	"github.com/qiniu/logkit/reader"
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

// POST /logkit/reader/check 请求校验reader配置
func (rs *RestService) PostReaderCheck() echo.HandlerFunc {
	return func(c echo.Context) error {

		//TODO check reader

		return RespSuccess(c, nil)
	}
}
