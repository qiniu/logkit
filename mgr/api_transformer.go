package mgr

import (
	"github.com/labstack/echo"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
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
		return RespSuccess(c, ModeUsages)
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
		return RespSuccess(c, ModeKeyOptions)
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
		return RespSuccess(c, SampleConfigs)
	}
}
