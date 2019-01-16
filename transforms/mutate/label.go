package mutate

import (
	"errors"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Label{}
	_ transforms.Transformer      = &Label{}
	_ transforms.Initializer      = &Label{}
)

type Label struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Override bool   `json:"override"`

	keys  []string
	stats StatsInfo

	numRoutine int
}

func (g *Label) Init() error {
	g.keys = GetKeys(g.Key)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Label) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("label transformer not support rawTransform")
}

func (g *Label) Transform(datas []Data) ([]Data, error) {
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

func (g *Label) Description() string {
	//return "label can add a field"
	return `增加标签, 如设置标签{key:a,value:b}, 则数据中加入 {"a":"b"}`
}

func (g *Label) Type() string {
	return "label"
}

func (g *Label) SampleConfig() string {
	return `{
		"type":"label",
		"key":"my_field_keyname",
		"value":"my_field_value",
		"override":false
	}`
}

func (g *Label) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "value",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "要添加的数据值[仅限string类型](value)",
			Type:         transforms.TransformTypeString,
		},
		transforms.KeyOverride,
	}
}

func (g *Label) Stage() string {
	return transforms.StageAfterParser
}

func (g *Label) Stats() StatsInfo {
	return g.stats
}

func (g *Label) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("label", func() transforms.Transformer {
		return &Label{}
	})
}

func (g *Label) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		_, getErr := GetMapValue(transformInfo.CurData, g.keys...)
		if getErr == nil && !g.Override {
			existErr := errors.New("the key " + g.Key + " already exists")
			errNum, err = transforms.SetError(errNum, existErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		setErr := SetMapValue(transformInfo.CurData, g.Value, false, g.keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.Key)
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
