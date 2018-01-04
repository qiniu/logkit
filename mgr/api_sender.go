package mgr

import (
	"github.com/labstack/echo"
	"github.com/qiniu/logkit/sender"
)

// get /logkit/sender/usages 获取sender用途说明
func (rs *RestService) GetSenderUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, sender.ModeUsages)
	}
}

// get /logkit/sender/options 获取sender配置参数
func (rs *RestService) GetSenderKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, sender.ModeKeyOptions)
	}
}

// get /logkit/sender/router/option 获取所有sender router的配置项
func (rs *RestService) GetSenderRouterOption() echo.HandlerFunc {
	return func(c echo.Context) error {
		routerOption := sender.GetRouterOption()
		return RespSuccess(c, routerOption)
	}
}

// get /logkit/sender/router/usage 获取所有sender router匹配方式的名字和作用
func (rs *RestService) GetSenderRouterUsage() echo.HandlerFunc {
	return func(c echo.Context) error {
		routerUsage := sender.GetRouterMatchTypeUsage()
		return RespSuccess(c, routerUsage)
	}
}

// POST /logkit/sender/check 请求校验sender配置
func (rs *RestService) PostSenderCheck() echo.HandlerFunc {
	return func(c echo.Context) error {
		//TODO check sender
		return RespSuccess(c, nil)
	}
}
