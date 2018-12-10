package mutate

import (
	"sort"
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Discarder{}
	_ transforms.Transformer      = &Discarder{}
	_ transforms.Initializer      = &Discarder{}
)

type Discarder struct {
	Key       string `json:"key"`
	StageTime string `json:"stage"`
	stats     StatsInfo

	discardKeys [][]string

	numRoutine int
}

func (g *Discarder) Init() error {
	discardKeys := strings.Split(g.Key, ",")
	g.discardKeys = make([][]string, len(discardKeys))
	for i := range g.discardKeys {
		g.discardKeys[i] = GetKeys(discardKeys[i])
	}

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Discarder) RawTransform(datas []string) ([]string, error) {
	if len(g.discardKeys) == 0 {
		g.Init()
	}

	var (
		dataLine   = len(datas)
		ret        = make([]string, dataLine)
		err        error
		errNum     int
		numRoutine = g.numRoutine

		dataPipeline = make(chan transforms.RawTransformInfo)
		resultChan   = make(chan transforms.RawTransformResult)
		wg           = new(sync.WaitGroup)
	)
	if dataLine < numRoutine {
		numRoutine = dataLine
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go g.rawtransform(dataPipeline, resultChan, wg)
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

	var transformResultSlice = make(transforms.RawTransformResultSlice, dataLine)
	for resultInfo := range resultChan {
		transformResultSlice[resultInfo.Index] = resultInfo
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
			continue
		}
		ret[transformResult.Index] = transformResult.CurData
	}

	var (
		result      = make([]string, dataLine)
		resultIndex = 0
	)
	for _, retEntry := range ret {
		if retEntry == "" {
			continue
		}
		result[resultIndex] = retEntry
		resultIndex++
	}

	g.stats, _ = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLine), g.Type())
	return result[:resultIndex], nil
}

func (g *Discarder) Transform(datas []Data) ([]Data, error) {
	if g.discardKeys == nil {
		g.Init()
	}

	var (
		err    error
		errNum int
	)
	numRoutine := g.numRoutine
	if len(datas) < numRoutine {
		numRoutine = len(datas)
	}
	dataPipeline := make(chan transforms.TransformInfo)
	resultChan := make(chan transforms.TransformResult)

	wg := new(sync.WaitGroup)
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

	g.stats, _ = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, nil
}

func (g *Discarder) Description() string {
	//return "discard onefield from data"
	return `删除指定的数据字段, 如数据{"a":123,"b":"xx"}, 指定删除a，变为{"b":"xx"}, 可写多个用逗号分隔 a,b 数据变为空 {}`
}

func (g *Discarder) Type() string {
	return "discard"
}

func (g *Discarder) SampleConfig() string {
	return `{
		"type":"discard",
		"key":"DiscardFieldKey1,DiscardFieldKey2",
		"stage":"after_parser"
	}`
}

func (g *Discarder) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
	}
}

func (g *Discarder) Stage() string {
	if g.StageTime == "" {
		return transforms.StageAfterParser
	}
	return g.StageTime
}

func (g *Discarder) Stats() StatsInfo {
	return g.stats
}

func (g *Discarder) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("discard", func() transforms.Transformer {
		return &Discarder{}
	})
}

func (g *Discarder) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	for transformInfo := range dataPipeline {
		for _, keys := range g.discardKeys {
			DeleteMapValue(transformInfo.CurData, keys...)
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
		}
	}
	wg.Done()
}

func (g *Discarder) rawtransform(dataPipeline <-chan transforms.RawTransformInfo, resultChan chan transforms.RawTransformResult, wg *sync.WaitGroup) {
	for transformInfo := range dataPipeline {
		if strings.Contains(transformInfo.CurData, g.Key) {
			continue
		}

		resultChan <- transforms.RawTransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
		}
	}
	wg.Done()
}
