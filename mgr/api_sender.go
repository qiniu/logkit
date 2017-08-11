package mgr

import (
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/sender"
)

// get /logkit/sender/usages 接受解析请求
func (rs *RestService) GetSenderUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, sender.ModeUsages)
	}
}

// get /logkit/sender/options 接受解析请求
func (rs *RestService) GetSenderKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, sender.ModeKeyOptions)
	}
}
