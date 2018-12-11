package mutate

import (
	"errors"
	"net/url"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	ModeDecode = "decode"
	ModeEncode = "encode"
)

var (
	_ transforms.StatsTransformer = &URL{}
	_ transforms.Transformer      = &URL{}
	_ transforms.Initializer      = &URL{}

	OptionURLMode = Option{
		KeyName:       KeyMode,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{ModeDecode, ModeEncode},
		Default:       ModeDecode,
		Required:      true,
		DefaultNoUse:  false,
		Description:   "url decode或encode",
	}
)

type URL struct {
	Mode   string `json:"mode"`
	Key    string `json:"key"`
	CStage string `json:"stage"`

	stats      StatsInfo
	keys       []string
	numRoutine int
}

func (u *URL) Init() error {
	u.keys = GetKeys(u.Key)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	u.numRoutine = numRoutine
	return nil
}

func (u *URL) Description() string {
	return `对于日志数据中的每条记录，进行url decode/encode。`
}

func (u *URL) SampleConfig() string {
	return `{
       "type":"url",
       "mode":"decode",
       "key":"myParseKey",
    }`
}

func (u *URL) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		OptionURLMode,
		transforms.KeyStage,
	}
}

func (u *URL) Type() string {
	return "url"
}

func (u *URL) RawTransform(datas []string) ([]string, error) {
	if len(u.keys) == 0 {
		u.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = u.numRoutine
		dataPipeline = make(chan transforms.RawTransformInfo)
		resultChan   = make(chan transforms.RawTransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go u.rawtransform(dataPipeline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipeline <- transforms.RawTransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipeline)
	}()

	var transformResultSlice = make(transforms.RawTransformResultSlice, dataLen)
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

	u.stats, fmtErr = transforms.SetStatsInfo(err, u.stats, int64(errNum), int64(dataLen), u.Type())
	return datas, fmtErr
}

func (u *URL) Stage() string {
	if u.CStage == "" {
		return transforms.StageAfterParser
	}
	return u.CStage
}

func (u *URL) Stats() StatsInfo {
	return u.stats
}

func (u *URL) SetStats(err string) StatsInfo {
	u.stats.LastError = err
	return u.stats
}

func (u *URL) Transform(datas []Data) ([]Data, error) {
	if len(u.keys) == 0 {
		u.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = u.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)
	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go u.transform(dataPipeline, resultChan, wg)
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

	u.stats, fmtErr = transforms.SetStatsInfo(err, u.stats, int64(errNum), int64(dataLen), u.Type())
	return datas, fmtErr
}

func init() {
	transforms.Add("url", func() transforms.Transformer {
		return &URL{}
	})
}

func (u *URL) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, u.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, u.Key)
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + u.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		var newVal string
		switch u.Mode {
		case ModeDecode:
			newVal, err = url.QueryUnescape(strVal)
			if err != nil {
				errNum, err = transforms.SetError(errNum, err, transforms.General, "")
			}
		case ModeEncode:
			newVal = url.QueryEscape(strVal)
		default:
			errNum, err = transforms.SetError(errNum, errors.New("url transformer not support this mode["+u.Mode+"]"), transforms.General, "")
		}
		setErr := SetMapValue(transformInfo.CurData, newVal, false, u.keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, u.Key)
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			Err:     err,
			ErrNum:  errNum,
		}
	}
	wg.Done()
}

func (u *URL) rawtransform(dataPipeline <-chan transforms.RawTransformInfo, resultChan chan transforms.RawTransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		var newVal string
		switch u.Mode {
		case ModeDecode:
			newVal, err = url.QueryUnescape(transformInfo.CurData)
			if err != nil {
				errNum, err = transforms.SetError(errNum, err, transforms.General, "")
			}
		case ModeEncode:
			newVal = url.QueryEscape(transformInfo.CurData)
		default:
			errNum, err = transforms.SetError(errNum, errors.New("url transformer not support this mode["+u.Mode+"]"), transforms.General, "")
		}
		transformInfo.CurData = newVal

		resultChan <- transforms.RawTransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			Err:     err,
			ErrNum:  errNum,
		}
	}
	wg.Done()
}
