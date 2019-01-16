package mutate

import (
	"errors"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Copy{}
	_ transforms.Transformer      = &Copy{}
	_ transforms.Initializer      = &Copy{}
)

type Copy struct {
	Key      string `json:"key"`
	New      string `json:"new"`
	Override bool   `json:"override"`

	keys  []string
	news  []string
	stats StatsInfo

	numRoutine int
}

func (c *Copy) Init() error {
	c.keys = GetKeys(c.Key)
	c.news = GetKeys(c.New)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	c.numRoutine = numRoutine
	return nil
}

func (c *Copy) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("copy transformer not support rawTransform")
}

func (c *Copy) Transform(datas []Data) ([]Data, error) {
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

func (c *Copy) Description() string {
	//return "copy can copy a field"
	return `拷贝键的值, 如拷贝标签{key:a,new:b}, 则数据中加入 {"b":"<value of a>"}`
}

func (c *Copy) Type() string {
	return "copy"
}

func (c *Copy) SampleConfig() string {
	return `{
		"type":"copy",
		"key":"my_field_keyname",
		"new":"my_field_newname",
		"override":false
	}`
}

func (c *Copy) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
		transforms.KeyOverride,
	}
}

func (c *Copy) Stage() string {
	return transforms.StageAfterParser
}

func (c *Copy) Stats() StatsInfo {
	return c.stats
}

func (c *Copy) SetStats(err string) StatsInfo {
	c.stats.LastError = err
	return c.stats
}

func init() {
	transforms.Add("copy", func() transforms.Transformer {
		return &Copy{}
	})
}

func (c *Copy) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, c.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}

		_, getErr = GetMapValue(transformInfo.CurData, c.news...)
		if getErr == nil && !c.Override {
			existErr := errors.New("the key " + c.New + " already exists")
			errNum, err = transforms.SetError(errNum, existErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}

		setErr := SetMapValue(transformInfo.CurData, val, true, c.news...)
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
