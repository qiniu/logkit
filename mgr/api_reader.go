package mgr

import (
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/reader"
)

// get /logkit/readerusages 接受解析请求
func (rs *RestService) GetReaderUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, reader.ModeUsages)
	}
}

// get /logkit/readeroptions 接受解析请求
func (rs *RestService) GetReaderKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, reader.ModeKeyOptions)
	}
}
