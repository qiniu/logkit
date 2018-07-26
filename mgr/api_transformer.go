package mgr

import (
	"net/http"
	"sort"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/labstack/echo"
)

// GET /logkit/transformer/usages
func (rs *RestService) GetTransformerUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		usages := transforms.GetTransformerUsages()
		sort.Stable(usages)
		return RespSuccess(c, usages)
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
			return RespError(c, http.StatusBadRequest, ErrTransformTransform, err.Error())
		}

		transformData, err := TransformData(transformerConfig)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrTransformTransform, err.Error())
		}

		// Transform Success
		return RespSuccess(c, transformData)
	}
}

// POST /logkit/transformer/check
func (rs *RestService) PostTransformerCheck() echo.HandlerFunc {
	return func(c echo.Context) error {
		var transformerConfig map[string]interface{} // request body params in map format
		// bind request context onto map[string]string
		if err := c.Bind(&transformerConfig); err != nil {
			return RespError(c, http.StatusBadRequest, ErrTransformTransform, err.Error())
		}

		create, err := getTransformerCreator(transformerConfig)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrTransformTransform, err.Error())
		}
		_, err = getTransformer(transformerConfig, create)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrTransformTransform, err.Error())

		}

		// Check Success
		return RespSuccess(c, nil)
	}
}
