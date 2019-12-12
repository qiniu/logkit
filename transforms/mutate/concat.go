package mutate

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Concat{}
	_ transforms.Transformer      = &Concat{}
	_ transforms.Initializer      = &Concat{}
)

type Concat struct {
	Key    string `json:"key"`
	New    string `json:"new"`
	joiner string `json:"joiner"`

	keys  [][]string
	news  []string
	stats StatsInfo

	numRoutine int
}

func (c *Concat) Init() error {
	keys := strings.Split(c.Key, ",")
	c.keys = make([][]string, len(keys))
	for i := range c.keys {
		c.keys[i] = GetKeys(keys[i])
	}
	c.news = GetKeys(c.New)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	c.numRoutine = numRoutine
	return nil
}

func (c *Concat) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("concat transformer not support rawTransform")
}

func (c *Concat) Transform(datas []Data) ([]Data, error) {
	if len(c.keys) == 0 {
		c.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = c.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go c.transform(dataPipeline, resultChan, wg)
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

	c.stats, fmtErr = transforms.SetStatsInfo(err, c.stats, int64(errNum), int64(dataLen), c.Type())
	return datas, fmtErr
}

func (c *Concat) Description() string {
	//return "concat can concat a field"
	return `连接值, 如连接两个标签{key1:a,key2:b}的值, 则数据中加入 {"new":"ab"}`
}

func (c *Concat) Type() string {
	return "concat"
}

func (c *Concat) SampleConfig() string {
	return `{
		"type":"concat",
		"key":"my_field_keyname",
		"new":"my_field_newname",
		"override":false
	}`
}

func (c *Concat) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
		{
			KeyName:      "joiner",
			ChooseOnly:   false,
			Default:      "",
			Placeholder:  "",
			DefaultNoUse: false,
			Advance:      true,
			Description:  "连接符(joiner)",
			CheckRegex:   CheckPatternKey,
			ToolTip:      "连接符，默认为空",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (c *Concat) Stage() string {
	return transforms.StageAfterParser
}

func (c *Concat) Stats() StatsInfo {
	return c.stats
}

func (c *Concat) SetStats(err string) StatsInfo {
	c.stats.LastError = err
	return c.stats
}

func init() {
	transforms.Add("concat", func() transforms.Transformer {
		return &Concat{}
	})
}

func (c *Concat) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err       error
		errNum    int
		concatStr string
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0
		concatStr = ""

		for _, keys := range c.keys {
			val, getErr := GetMapValue(transformInfo.CurData, keys...)
			if getErr != nil {
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, strings.Join(keys, "."))
				continue
			}
			valStr, ok := val.(string)
			if !ok {
				typeErr := fmt.Errorf("keys: %s type is: %T, only support concat string", strings.Join(keys, "."), val)
				errNum, err = transforms.SetError(errNum, typeErr, transforms.GetErr, strings.Join(keys, "."))
				continue
			}
			if c.joiner != "" && concatStr != "" {
				concatStr += c.joiner
			}
			concatStr += valStr
		}
		if err != nil {
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}

		setErr := SetMapValue(transformInfo.CurData, concatStr, false, c.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, c.New)
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
