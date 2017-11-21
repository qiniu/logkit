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
// transform logs in json array format with registered transformers
func (rs *RestService) PostTransform() echo.HandlerFunc {
	return func(c echo.Context) error {
		var err error
		var tp string
		var trans transforms.Transformer
		var rawLogs string
		var data = []sender.Data{}
		var singleData sender.Data
		reqConf := conf.MapConf{}
		err = c.Bind(&reqConf)

		// Valid Params & Initialize Transformer
		if err == nil {
			tp,err = reqConf.GetString(transforms.KeyType)
			if err == nil {
				create, ok := transforms.Transformers[tp]
				if !ok {
					err = fmt.Errorf("type %v of transformer not exist", tp)
				}
				rawLogs, err = reqConf.GetString(KeySampleLog)
				if err == nil {
					// single sample log
					err = json.Unmarshal([]byte(rawLogs), &singleData)
					// multi sample log
					if err != nil {
						err = json.Unmarshal([]byte(rawLogs), &data)
					} else {
						data = append(data, singleData)
					}
					if err == nil {
						trans = create()
						var bts []byte
						reqConf = convertWebTransformerConfig(reqConf)
						delete(reqConf, KeySampleLog)
						bts, err = json.Marshal(reqConf)
						if err == nil {
							err = json.Unmarshal(bts, trans)
							if err == nil {
								if trans, ok := trans.(transforms.Initialize); ok {
									err = trans.Init()
								}
							}
						}
					}
				}
			}
		}

		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		// Act Transform
		data, err = trans.Transform(data)
		se, ok := err.(*utils.StatsError)
		if ok {
			err = se.ErrorDetail
		}
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("transformer type error %v", err))
		}

		// Transform Success
		return c.JSON(http.StatusOK, data)
	}
}