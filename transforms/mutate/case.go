package mutate

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	ModeUpper = "upper"
	ModeLower = "lower"

	KeyCase = "key"
	KeyMode = "mode"
)

var (
	_ transforms.StatsTransformer = &Case{}
	_ transforms.Transformer      = &Case{}
	_ transforms.Initializer      = &Case{}

	OptionCaseKey = Option{
		KeyName:      KeyCase,
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "需要进行大小写转换的键(" + KeyCase + ")",
		ToolTip:      "对该字段的值进行大小写转换",
		Type:         transforms.TransformTypeString,
	}
	OptionCaseMode = Option{
		KeyName:       KeyMode,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{ModeUpper, ModeLower},
		Default:       ModeLower,
		Required:      true,
		DefaultNoUse:  false,
		Description:   "转换模式(" + KeyMode + ")",
	}
)

type Case struct {
	Mode   string `json:"mode"`
	Key    string `json:"key"`
	CStage string `json:"stage"`
	stats  StatsInfo

	keys []string

	numRoutine int
}

func (c *Case) Init() error {
	c.keys = GetKeys(c.Key)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	c.numRoutine = numRoutine
	return nil
}

func (c *Case) Description() string {
	return `对于日志数据中的每条记录，进行大小写转换。`
}

func (c *Case) SampleConfig() string {
	return `{
       "type":"case",
       "mode":"upper",
       "key":"myParseKey",
    }`
}

func (c *Case) ConfigOptions() []Option {
	return []Option{
		OptionCaseKey,
		OptionCaseMode,
		transforms.KeyStage,
	}
}

func (c *Case) Type() string {
	return "case"
}

func (c *Case) RawTransform(datas []string) ([]string, error) {
	if len(c.keys) == 0 {
		c.Init()
	}

	var (
		err, fmtErr error
		errNum      int
	)
	numRoutine := c.numRoutine
	if len(datas) < numRoutine {
		numRoutine = len(datas)
	}
	dataPipline := make(chan transforms.RawTransformInfo)
	resultChan := make(chan transforms.RawTransformResult)

	wg := new(sync.WaitGroup)
	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go c.rawtransform(dataPipline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipline <- transforms.RawTransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipline)
	}()

	var transformResultSlice = make(transforms.RawTransformResultSlice, 0, len(datas))
	for resultInfo := range resultChan {
		transformResultSlice = append(transformResultSlice, resultInfo)
	}
	if numRoutine > 1 {
		sort.Stable(transformResultSlice)
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	c.stats, fmtErr = transforms.SetStatsInfo(err, c.stats, int64(errNum), int64(len(datas)), c.Type())
	return datas, fmtErr
}

func (c *Case) Stage() string {
	return transforms.StageAfterParser
}

func (c *Case) Stats() StatsInfo {
	return c.stats
}

func (c *Case) SetStats(err string) StatsInfo {
	c.stats.LastError = err
	return c.stats
}

func (c *Case) Transform(datas []Data) ([]Data, error) {
	if len(c.keys) == 0 {
		c.Init()
	}

	var (
		err, fmtErr error
		errNum      int
	)
	numRoutine := c.numRoutine
	if len(datas) < numRoutine {
		numRoutine = len(datas)
	}
	dataPipline := make(chan transforms.TransformInfo)
	resultChan := make(chan transforms.TransformResult)

	wg := new(sync.WaitGroup)
	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go c.transform(dataPipline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipline <- transforms.TransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipline)
	}()

	var transformResultSlice = make(transforms.TransformResultSlice, 0, len(datas))
	for resultInfo := range resultChan {
		transformResultSlice = append(transformResultSlice, resultInfo)
	}
	if numRoutine > 1 {
		sort.Stable(transformResultSlice)
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	c.stats, fmtErr = transforms.SetStatsInfo(err, c.stats, int64(errNum), int64(len(datas)), c.Type())
	return datas, fmtErr
}

func init() {
	transforms.Add("case", func() transforms.Transformer {
		return &Case{}
	})
}

func (c *Case) transform(dataPipline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, c.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, c.Key)
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
			typeErr := fmt.Errorf("transform key %v data type is not string", c.Key)
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
		switch c.Mode {
		case ModeUpper:
			newVal = strings.ToUpper(strVal)
		case ModeLower:
			newVal = strings.ToLower(strVal)
		default:
			newVal = strVal
			errNum, err = transforms.SetError(errNum, fmt.Errorf("case transformer not support this mode[%s]", c.Mode), transforms.General, "")
		}
		setErr := SetMapValue(transformInfo.CurData, newVal, false, c.keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, c.Key)
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

func (c *Case) rawtransform(dataPipline <-chan transforms.RawTransformInfo, resultChan chan transforms.RawTransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipline {
		err = nil
		errNum = 0
		strVal := transformInfo.CurData
		var newVal string
		switch c.Mode {
		case ModeUpper:
			newVal = strings.ToUpper(strVal)
		case ModeLower:
			newVal = strings.ToLower(strVal)
		default:
			newVal = strVal
			errNum, err = transforms.SetError(errNum, fmt.Errorf("case transformer not support this mode[%s]", c.Mode), transforms.General, "")
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
