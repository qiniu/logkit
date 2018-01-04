package mgr

import (
	"fmt"
	"net/http"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"
	"github.com/qiniu/logkit/sender"
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

// POST /logkit/transformer/transform
// Transform (multiple logs/single log) in (json array/json object) format with registered transformers
// Return result string in json array format
func (rs *RestService) PostTransform() echo.HandlerFunc {
	return func(c echo.Context) error {
		var ok bool                      // value exists in a map / assertion is successful
		var jsonErr error                // error caused by json marshal or unmarshal
		var transErr error               // error caused by incorrect transform process
		var tp string                    // transformer type string
		var trans transforms.Transformer // transformer itself
		var rawLogs string               // sample logs picked from request in json format
		var data = []sender.Data{}       // multiple sample logs in map format
		var singleData sender.Data       // single sample log in map format
		var bts []byte
		var reqConf map[string]interface{} // request body params in map format

		// bind request context onto map[string]string
		if err := c.Bind(&reqConf); err != nil {
			return err
		}

		// Get params from request & Valid Params & Initialize transformer using valid params
		// param 1: transformer type
		if _, ok = reqConf[transforms.KeyType]; !ok {
			// param absence
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, fmt.Sprintf("missing param %s", transforms.KeyType))
		}
		tp, ok = (reqConf[transforms.KeyType]).(string)
		if !ok {
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, fmt.Sprintf("param %s must be of type string", transforms.KeyType))
		}
		create, ok := transforms.Transformers[tp]
		if !ok {
			// no such type transformer
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, fmt.Sprintf("transformer of type %v not exist", tp))
		}
		// param 2: sample logs
		if _, ok = reqConf[KeySampleLog]; !ok {
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, fmt.Sprintf("missing param %s", KeySampleLog))
		}
		if rawLogs, ok = (reqConf[KeySampleLog]).(string); !ok {
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, fmt.Sprintf("missing param %s", KeySampleLog))
		}
		if jsonErr = jsoniter.Unmarshal([]byte(rawLogs), &singleData); jsonErr != nil {
			// may be multiple sample logs
			if jsonErr = jsoniter.Unmarshal([]byte(rawLogs), &data); jsonErr != nil {
				// invalid JSON, neither multiple sample logs nor single sample log
				return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, jsonErr.Error())
			}
		} else {
			// is single log, and method transformer.transform(data []sender.Data) accept a param of slice type
			data = append(data, singleData)
		}
		// initialize transformer
		trans = create()
		reqConf = convertWebTransformerConfig(reqConf)
		delete(reqConf, KeySampleLog)
		if bts, jsonErr = jsoniter.Marshal(reqConf); jsonErr != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, jsonErr.Error())
		}
		if jsonErr = jsoniter.Unmarshal(bts, trans); jsonErr != nil {
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, jsonErr.Error())
		}
		if trans, ok := trans.(transforms.Initialize); ok {
			if err := trans.Init(); err != nil {
				return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, err.Error())
			}
		}

		// Act Transform
		data, transErr = trans.Transform(data)
		if transErr != nil {
			se, ok := transErr.(*utils.StatsError)
			if ok {
				transErr = se.ErrorDetail
			}
			return RespError(c, http.StatusBadRequest, utils.ErrTransformTransform, fmt.Sprintf("transform processing error %v", transErr))
		}

		// Transform Success
		return RespSuccess(c, data)
	}
}
