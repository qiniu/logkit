package mgr

import (
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/metric"
)

//GET /logkit/metric/keys
func (rs *RestService) GetMetricKeys() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, metric.GetMetricTypeKey())
	}
}

//GET /logkit/metric/usages
func (rs *RestService) GetMetricUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, metric.GetMetricUsages())
	}
}

//GET /logkit/metric/options
func (rs *RestService) GetMetricOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, metric.GetMetricOptions())
	}
}
