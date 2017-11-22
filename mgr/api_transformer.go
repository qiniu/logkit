package mgr

import (
	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	"github.com/qiniu/logkit/conf"
	"fmt"
	"encoding/json"
	"github.com/qiniu/logkit/sender"
)

// GET /logkit/transformer/usages
func (rs *RestService) GetTransformerUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		var ModeUsages []utils.KeyValue
		for _, v := range transforms.Transformers {
			cr := v()
			ModeUsages = append(ModeUsages, utils.KeyValue{
				Key:   cr.Type(),
				Value: cr.Description(),
			})
		}
		return c.JSON(http.StatusOK, ModeUsages)
	}
}

//GET /logkit/transformer/options
func (rs *RestService) GetTransformerOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		ModeKeyOptions := make(map[string][]utils.Option)
		for _, v := range transforms.Transformers {
			cr := v()
			ModeKeyOptions[cr.Type()] = cr.ConfigOptions()
		}
		return c.JSON(http.StatusOK, ModeKeyOptions)
	}
}

//GET /logkit/transformer/sampleconfigs
func (rs *RestService) GetTransformerSampleConfigs() echo.HandlerFunc {
	return func(c echo.Context) error {
		SampleConfigs := make(map[string]string)
		for _, v := range transforms.Transformers {
			cr := v()
			SampleConfigs[cr.Type()] = cr.SampleConfig()
		}
		return c.JSON(http.StatusOK, SampleConfigs)
	}
}

// POST /logkit/transformer/transform
// Transform (multiple logs/single log) in (json array/json object) format with registered transformers
// Return result string in json array format
func (rs *RestService) PostTransform() echo.HandlerFunc {
	return func(c echo.Context) error {
		var paramErr error // error caused by invalid param
		var transErr error // error caused by incorrect transform process
		var tp string // transformer type string
		var trans transforms.Transformer // transformer itself
		var rawLogs string // sample logs picked from request in json format
		var data = []sender.Data{} // multiple sample logs in map format
		var singleData sender.Data // single sample log in map format
		var bts []byte
		reqConf := conf.MapConf{} // request body params in map format

		// bind request context onto map[string]string
		if err := c.Bind(&reqConf); err != nil {
			return err
		}

		// Get params from request & Valid Params & Initialize transformer using valid params
		// param 1: transformer type
		tp, paramErr = reqConf.GetString(transforms.KeyType)
		if paramErr != nil {
			// param absence
			return echo.NewHTTPError(http.StatusBadRequest, paramErr.Error())
		}
		create, ok := transforms.Transformers[tp]
		if !ok {
			// no such type transformer
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("type %v of transformer not exist", tp))
		}
		// param 2: sample logs
		rawLogs, paramErr = reqConf.GetString(KeySampleLog)
		if paramErr != nil {
			// param absence
			return echo.NewHTTPError(http.StatusBadRequest, paramErr.Error())
		}
		if paramErr = json.Unmarshal([]byte(rawLogs), &singleData); paramErr != nil {
			// may be multiple sample logs
			if paramErr = json.Unmarshal([]byte(rawLogs), &data); paramErr != nil {
				// invalid JSON, neither multiple sample logs nor single sample log
				return echo.NewHTTPError(http.StatusBadRequest, paramErr.Error())
			}
		} else {
			// is single log, and method transformer.transform(data []sender.Data) accept a param of slice type
			data = append(data, singleData)
		}
		// initialize transformer
		trans = create()
		reqConf = convertWebTransformerConfig(reqConf)
		delete(reqConf, KeySampleLog)
		if bts, paramErr = json.Marshal(reqConf); paramErr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, paramErr.Error())
		}
		if paramErr = json.Unmarshal(bts, trans); paramErr != nil {
			return echo.NewHTTPError(http.StatusBadRequest, paramErr.Error())
		}
		if trans, ok := trans.(transforms.Initialize); ok {
			if paramErr = trans.Init(); paramErr != nil {
				return echo.NewHTTPError(http.StatusBadRequest, paramErr.Error())
			}
		}

		// Act Transform
		data, transErr = trans.Transform(data)
		if transErr != nil {
			se, ok := transErr.(*utils.StatsError)
			if ok {
				transErr = se.ErrorDetail
			}
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("transformer type error %v", transErr))
		}

		// Transform Success
		return c.JSON(http.StatusOK, data)
	}
}