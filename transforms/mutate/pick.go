package mutate

import (
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const TYPE = "pick"

var (
	_ transforms.StatsTransformer = &Pick{}
	_ transforms.Transformer      = &Pick{}
	_ transforms.Initializer      = &Pick{}
)

type Pick struct {
	Key       string `json:"key"`
	StageTime string `json:"stage"`
	stats     StatsInfo

	keys       []string
	numRoutine int
}

func (g *Pick) Init() error {
	g.keys = strings.Split(g.Key, ",")
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Pick) RawTransform(datas []string) ([]string, error) {
	var ret []string
	for i := range datas {
		if !strings.Contains(datas[i], g.Key) {
			continue
		}
		ret = append(ret, datas[i])
	}
	g.stats.Success += int64(len(datas))
	return ret, nil
}

func (g *Pick) Transform(datas []Data) ([]Data, error) {
	if len(g.keys) == 0 {
		g.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int
		retDatas    = make([]Data, len(datas))
		result      = make([]Data, 0, len(datas))

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
		retDatas[transformResult.Index] = transformResult.CurData
	}

	for _, data := range retDatas {
		if len(data) == 0 {
			continue
		}
		result = append(result, data)
	}
	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
	return result, fmtErr
}

func (g *Pick) Description() string {
	//return "pick onefield from data"
	return `保留指定的数据字段, 如数据{"a":123,"b":"xx"}, 指定a，变为{"a":"123"}, 可写多个用逗号分隔 a,b 数据变为空 {"a":123,"b":"xx"}`
}

func (g *Pick) Type() string {
	return TYPE
}

func (g *Pick) SampleConfig() string {
	return `{
		"type":"pick",
		"key":"PickFieldKey1,PickFieldKey2",
		"stage":"after_parser"
	}`
}

func (g *Pick) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
	}
}

func (g *Pick) Stage() string {
	if g.StageTime == "" {
		return transforms.StageAfterParser
	}
	return g.StageTime
}

func (g *Pick) Stats() StatsInfo {
	return g.stats
}

func (g *Pick) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add(TYPE, func() transforms.Transformer {
		return &Pick{}
	})
}

func (g *Pick) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	for transformInfo := range dataPipeline {
		data := Data{}
		for _, v := range g.keys {
			keys := GetKeys(v)
			PickMapValue(transformInfo.CurData, data, keys...)
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: data,
		}
	}
	wg.Done()
}
