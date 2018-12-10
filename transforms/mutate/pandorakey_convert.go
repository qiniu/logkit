package mutate

import (
	"errors"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &PandoraKeyConvert{}
	_ transforms.Transformer      = &PandoraKeyConvert{}
)

type PandoraKeyConvert struct {
	stats StatsInfo
	cache map[string]KeyInfo

	numRoutine int
}

func (g *PandoraKeyConvert) Init() error {
	g.cache = make(map[string]KeyInfo)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}
func (g *PandoraKeyConvert) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("pandora_key_convert transformer not support rawTransform")
}

func (g *PandoraKeyConvert) Transform(datas []Data) ([]Data, error) {
	if g.cache == nil {
		g.Init()
	}

	var (
		dataLen = len(datas)
		err     error
		errNum  int

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

	g.stats, _ = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
	return datas, nil
}

func (g *PandoraKeyConvert) Description() string {
	//return "pandora_key_convert can convert data key name to valid pandora key"
	return "将数据中的key名称中不合Pandora字段名规则的字符转为下划线, 如 a.b/c 改为 a_b_c"
}

func (g *PandoraKeyConvert) Type() string {
	return "pandora_key_convert"
}

func (g *PandoraKeyConvert) SampleConfig() string {
	return `{
		"type":"pandora_key_convert"
	}`
}

func (g *PandoraKeyConvert) ConfigOptions() []Option {
	return []Option{}
}

func (g *PandoraKeyConvert) Stage() string {
	return transforms.StageAfterParser
}

func (g *PandoraKeyConvert) Stats() StatsInfo {
	return g.stats
}

func (g *PandoraKeyConvert) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("pandora_key_convert", func() transforms.Transformer {
		return &PandoraKeyConvert{cache: make(map[string]KeyInfo)}
	})
}

func (g *PandoraKeyConvert) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	for transformInfo := range dataPipeline {
		transformInfo.CurData = DeepConvertKeyWithCache(transformInfo.CurData, g.cache)
		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
		}
	}
	wg.Done()
}
