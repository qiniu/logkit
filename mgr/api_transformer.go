package mgr

import (
	"net/http"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/labstack/echo"
)

// GET /logkit/transformer/usages
func (rs *RestService) GetTransformerUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, transforms.GetTransformerUsages())
	}
}

//GET /logkit/transformer/options
func (rs *RestService) GetTransformerOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, transforms.GetTransformerOptions())
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

// POST /logkit/transformer/transform
// Transform (multiple logs/single log) in (json array/json object) format with registered transformers
// Return result string in json array format
func (rs *RestService) PostTransform() echo.HandlerFunc {
	return func(c echo.Context) error {
		var transformerConfig map[string]interface{} // request body params in map format
		// bind request context onto map[string]string
		if err := c.Bind(&transformerConfig); err != nil {
			return err
		}

		transformData, err := GetTransformedData(transformerConfig)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrTransformTransform, err.Error())
		}

		// Transform Success
		return RespSuccess(c, transformData)
	}
}
