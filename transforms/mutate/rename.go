package mutate

import (
	"errors"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Rename{}
	_ transforms.Transformer      = &Rename{}
	_ transforms.Initializer      = &Rename{}
)

type Rename struct {
	Key        string `json:"key"`
	NewKeyName string `json:"new_key_name"`
	NewKey     string `json:"new"`
	stats      StatsInfo

	keys       []string
	news       []string
	numRoutine int
}

func (g *Rename) Init() error {
	g.keys = GetKeys(g.Key)
	if g.NewKey == "" {
		g.NewKey = g.NewKeyName
	}
	g.news = GetKeys(g.NewKey)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}
func (g *Rename) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("rename transformer not support rawTransform")
}

func (g *Rename) Transform(datas []Data) ([]Data, error) {
	if g.keys == nil {
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

func (g *Rename) Description() string {
	//return "rename can mutate old field name to new field name"
	return "用新的字段重命名旧的字段, 如 {a:123} 改为 {b:123}"
}

func (g *Rename) Type() string {
	return "rename"
}

func (g *Rename) SampleConfig() string {
	return `{
		"type":"rename",
		"key":"old_key_name"
	    "new_key_name":"new_key_name"
	}`
}

func (g *Rename) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
	}
}

func (g *Rename) Stage() string {
	return transforms.StageAfterParser
}

func (g *Rename) Stats() StatsInfo {
	return g.stats
}

func (g *Rename) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("rename", func() transforms.Transformer {
		return &Rename{}
	})
}

func (g *Rename) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
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
		DeleteMapValue(transformInfo.CurData, g.keys...)
		setErr := SetMapValue(transformInfo.CurData, val, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.NewKeyName)
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
