package mutate

import (
	"errors"
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Spliter{}
	_ transforms.Transformer      = &Spliter{}
	_ transforms.Initializer      = &Spliter{}
)

type Spliter struct {
	Key          string `json:"key"`
	SeparateKey  string `json:"sep"`
	ArrayName    string `json:"new"`
	ArrayNameNew string `json:"newfield"` // 兼容老版本

	stats      StatsInfo
	keys       []string
	numRoutine int
}

func (g *Spliter) Init() error {
	if g.ArrayName == "" {
		g.ArrayName = g.ArrayNameNew
	}
	g.keys = GetKeys(g.Key)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Spliter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("split transformer not support rawTransform")
}

func (g *Spliter) Transform(datas []Data) ([]Data, error) {
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
	if g.ArrayName == "" {
		err = errors.New("array name is empty string,can't use as array field key name")
		errNum = dataLen
		g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
		return datas, fmtErr
	}
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

func (g *Spliter) Description() string {
	//return "split one field into array from data"
	return `将指定的字段切割成数组, 例如 "a,b,c" 切割为 ["a","b","c"]`
}

func (g *Spliter) Type() string {
	return "split"
}

func (g *Spliter) SampleConfig() string {
	return `{
		"type":"split",
		"key":"SplitFieldKey",
		"sep":",",
		"new":"name"
	}`
}

func (g *Spliter) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "new",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "new_field_keyname",
			DefaultNoUse: true,
			Description:  "解析后数据的字段名(new)",
			CheckRegex:   CheckPattern,
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "sep",
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  ",",
			Required:     true,
			DefaultNoUse: true,
			Description:  "分隔符(sep)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Spliter) Stage() string {
	return transforms.StageAfterParser
}

func (g *Spliter) Stats() StatsInfo {
	return g.stats
}

func (g *Spliter) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("split", func() transforms.Transformer {
		return &Spliter{}
	})
}

func (g *Spliter) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	newKeys := make([]string, len(g.keys))
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		copy(newKeys, g.keys)
		val, getErr := GetMapValue(transformInfo.CurData, newKeys...)
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
		newKeys[len(newKeys)-1] = g.ArrayName
		setErr := SetMapValue(transformInfo.CurData, strings.Split(strVal, g.SeparateKey), false, newKeys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, strings.Join(newKeys, "."))
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
