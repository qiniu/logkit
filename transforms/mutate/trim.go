package mutate

import (
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	Prefix = "prefix"
	Suffix = "suffix"
	Both   = "both"
)

var (
	_ transforms.StatsTransformer = &Trim{}
	_ transforms.Transformer      = &Trim{}
	_ transforms.Initializer      = &Trim{}
)

type Trim struct {
	Key        string `json:"key"`
	Characters string `json:"characters"`
	Place      string `json:"place"`

	stats StatsInfo
	keys  []string

	numRoutine int
}

func (g *Trim) Init() error {
	g.keys = GetKeys(g.Key)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Trim) Transform(datas []Data) ([]Data, error) {
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

func (g *Trim) RawTransform(datas []string) ([]string, error) {
	return datas, nil
}

func (g *Trim) Description() string {
	return "去掉字符串前后多余的字符，如 abc123, 设置trim的字符为abc，变化后为 123"
}

func (g *Trim) Type() string {
	return "trim"
}

func (g *Trim) SampleConfig() string {
	return `{
		"type":"trim",
		"key":"ToBeTrimedFieldKey",
		"characters":"trimCh",
		"place":"left"
	}`
}

func (g *Trim) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "characters",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "ToBeTrimedFieldKey",
			DefaultNoUse: true,
			Description:  "要修整掉的字符内容(characters)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:       "place",
			ChooseOnly:    true,
			ChooseOptions: []interface{}{Both, Prefix, Suffix},
			Default:       Both,
			DefaultNoUse:  false,
			Description:   "修整位置(place)",
			Type:          transforms.TransformTypeBoolean,
			ToolTip:       "前缀后缀都去掉(both)，只去掉前缀(prefix)或者后缀(suffix)",
		},
	}
}

func (g *Trim) Stage() string {
	return transforms.StageAfterParser
}

func (g *Trim) Stats() StatsInfo {
	return g.stats
}

func (g *Trim) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("trim", func() transforms.Transformer {
		return &Trim{}
	})
}

func (g *Trim) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
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
		switch g.Place {
		case Prefix:
			strVal = strings.TrimPrefix(strVal, g.Characters)
		case Suffix:
			strVal = strings.TrimSuffix(strVal, g.Characters)
		default:
			strVal = strings.Trim(strVal, g.Characters)
		}
		setErr := SetMapValue(transformInfo.CurData, strVal, false, g.keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.Key)
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
