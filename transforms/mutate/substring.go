package mutate

import (
	"errors"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	KeySubStart = "start"
	KeySubEnd   = "end"
)

var (
	_ transforms.StatsTransformer = &Sub{}
	_ transforms.Transformer      = &Sub{}
	_ transforms.Initializer      = &Sub{}

	OptionSubStart = Option{
		KeyName:      KeySubStart,
		ChooseOnly:   false,
		Required:     false,
		Default:      0,
		Placeholder:  "0",
		Element:      InputNumber,
		DefaultNoUse: true,
		Description:  "字段提取的起始位置(start)",
		ToolTip:      "指定需要提取字段起始元素的位置（包含）默认为0。不支持负数",
		Type:         transforms.TransformTypeLong,
	}
	OptionSubEnd = Option{
		KeyName:      KeySubEnd,
		ChooseOnly:   false,
		Required:     false,
		Default:      1,
		Placeholder:  "1",
		Element:      InputNumber,
		DefaultNoUse: true,
		Description:  "字段提取的结束位置(end)",
		ToolTip:      "指定需要提取字段结束元素的位置（不包含）默认为1。不支持负数",
		Type:         transforms.TransformTypeLong,
	}
)

type Sub struct {
	Key    string `json:"key"`
	New    string `json:"new"`
	Start  int    `json:"start"`
	End    int    `json:"end"`
	CStage string `json:"stage"`
	stats  StatsInfo

	oldKeys []string
	newKeys []string

	numRoutine int
}

func (s *Sub) Init() error {
	if s.Start < 0 || s.End < 0 || s.Start >= s.End {
		return errors.New("transform[substring] invalid start or end index, please make sure start>=0, end>=0, start<end")
	}
	s.oldKeys = GetKeys(s.Key)
	if s.New == "" {
		s.newKeys = GetKeys(s.Key)
	} else {
		s.newKeys = GetKeys(s.New)
	}
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	s.numRoutine = numRoutine
	return nil
}

func (s *Sub) Description() string {
	return `对于日志数据中的每条记录，对指定的键值进行定长字段提取。`
}

func (s *Sub) SampleConfig() string {
	return `{
       "type":"substring",
       "key":"my_field_keyname",
       "start":"0",
       "end":"10",
    }`
}

func (s *Sub) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		OptionSubStart,
		OptionSubEnd,
		transforms.KeyStage,
	}
}

func (s *Sub) Type() string {
	return "substring"
}

func (s *Sub) RawTransform(datas []string) ([]string, error) {
	if len(s.oldKeys) == 0 {
		if err := s.Init(); err != nil {
			return nil, err
		}
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = s.numRoutine
		dataPipeline = make(chan transforms.RawTransformInfo)
		resultChan   = make(chan transforms.RawTransformResult)
		wg           = new(sync.WaitGroup)
	)
	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go s.rawtransform(dataPipeline, resultChan, wg)
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

	s.stats, fmtErr = transforms.SetStatsInfo(err, s.stats, int64(errNum), int64(dataLen), s.Type())
	return datas, fmtErr
}

func (s *Sub) Stage() string {
	if s.CStage == "" {
		return transforms.StageAfterParser
	}
	return s.CStage
}

func (s *Sub) Stats() StatsInfo {
	return s.stats
}

func (s *Sub) SetStats(err string) StatsInfo {
	s.stats.LastError = err
	return s.stats
}

func (s *Sub) Transform(datas []Data) ([]Data, error) {
	if len(s.oldKeys) == 0 {
		if err := s.Init(); err != nil {
			return nil, err
		}
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = s.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)
	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go s.transform(dataPipeline, resultChan, wg)
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

	s.stats, fmtErr = transforms.SetStatsInfo(err, s.stats, int64(errNum), int64(dataLen), s.Type())
	return datas, fmtErr
}

func init() {
	transforms.Add("substring", func() transforms.Transformer {
		return &Sub{}
	})
}

func (s *Sub) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
		sLen   int
		newVal string
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0
		val, getErr := GetMapValue(transformInfo.CurData, s.oldKeys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, s.Key)
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
			typeErr := errors.New("transform key " + s.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		sLen = len(strVal)
		if s.Start >= sLen {
			newVal = ""
		} else if s.End >= sLen {
			newVal = strVal[s.Start:]
		} else {
			newVal = strVal[s.Start:s.End]
		}
		setErr := SetMapValue(transformInfo.CurData, newVal, false, s.newKeys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, s.New)
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

func (s *Sub) rawtransform(dataPipeline <-chan transforms.RawTransformInfo, resultChan chan transforms.RawTransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
		strVal string
		sLen   int
		newVal string
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0
		strVal = transformInfo.CurData
		sLen = len(strVal)
		if s.Start >= sLen {
			newVal = strVal
		} else if s.End >= sLen {
			newVal = strVal[s.Start:]
		} else {
			newVal = strVal[s.Start:s.End]
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
