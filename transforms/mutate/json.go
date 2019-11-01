package mutate

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Json{}
	_ transforms.Transformer      = &Json{}
	_ transforms.Initializer      = &Json{}
)

type Json struct {
	Key      string `json:"key"`
	New      string `json:"new"`
	Extract  bool   `json:"extract"`
	stats    StatsInfo
	jsonTool jsoniter.API

	keys []string
	news []string

	numRoutine int
}

func (g *Json) Init() error {
	g.Key = strings.TrimSpace(g.Key)
	g.keys = GetKeys(g.Key)
	g.New = strings.TrimSpace(g.New)
	g.news = GetKeys(g.New)
	if len(g.news) == 0 {
		g.news = g.keys
	}

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Json) Transform(datas []Data) ([]Data, error) {
	if len(g.keys) == 0 {
		g.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = g.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go g.transform(dataPipeline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipeline <- transforms.TransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipeline)
	}()

	var transformResultSlice = make(transforms.TransformResultSlice, dataLen)
	for resultInfo := range resultChan {
		transformResultSlice[resultInfo.Index] = resultInfo
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
	return datas, fmtErr
}

func (g *Json) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("json transformer not support rawTransform")
}

func parseJson(jsonTool jsoniter.API, jsonStr string) (data interface{}, err error) {
	err = jsonTool.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		err = errors.New("parse json str error " + err.Error() + ", jsonStr is: " + jsonStr)
		log.Debug(err)
	}
	return
}

func (g *Json) Description() string {
	//return "parse jsonStr to json"
	return `解析json并加入到数据中，如json为 {"a":123}，加入后变为{"myNewKey":{"a":123}}`
}

func (g *Json) Type() string {
	return "json"
}

func (g *Json) SampleConfig() string {
	return `{
		"type":"json",
		"key":"myParseKey",
		"new":"myNewKey",
		"extract":false
    }`
}

func (g *Json) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		{
			KeyName:       "extract",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "提取json中的字段（只提取第一层，请确保字段名不重名）",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
	}
}

func (g *Json) Stage() string {
	return transforms.StageAfterParser
}

func (g *Json) Stats() StatsInfo {
	return g.stats
}

func (g *Json) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("json", func() transforms.Transformer {
		return &Json{
			jsonTool: jsoniter.Config{
				EscapeHTML: true,
				UseNumber:  true,
			}.Froze(),
		}
	})
}

func (g *Json) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, g.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + g.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}
		strVal = strings.TrimSpace(strVal)
		if strVal == "" {
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
			}
			continue
		}
		jsonVal, parseErr := parseJson(g.jsonTool, strVal)
		if parseErr != nil {
			errNum, err = transforms.SetError(errNum, parseErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}

		if g.Extract {
			jsonMap, ok := jsonVal.(map[string]interface{})
			if !ok {
				getErr = fmt.Errorf("expect map[string]interface{}, got %T", jsonVal)
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.New)
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					ErrNum:  errNum,
					Err:     err,
				}
				continue
			}

			for k, v := range jsonMap {
				setErr := SetExtractMapValue(transformInfo.CurData, v, false, k, g.news...)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
				}
			}
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}

		setErr := SetMapValue(transformInfo.CurData, jsonVal, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
		}
		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			ErrNum:  errNum,
			Err:     err,
		}
	}
	wg.Done()
}
