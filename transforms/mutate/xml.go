package mutate

import (
	"errors"
	"strings"
	"sync"

	"github.com/clbanning/mxj"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Xml{}
	_ transforms.Transformer      = &Xml{}
	_ transforms.Initializer      = &Xml{}
)

type Xml struct {
	Key   string `json:"key"`
	New   string `json:"new"`
	Keep  bool   `json:"keep"`
	stats StatsInfo

	keys       []string
	news       []string
	numRoutine int
}

func (g *Xml) Init() error {
	g.keys = GetKeys(g.Key)
	g.news = GetKeys(g.New)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Xml) Transform(datas []Data) ([]Data, error) {
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

func (g *Xml) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("xml transformer not support rawTransform")
}

func parseXml(xmlStr string, keep bool) (data map[string]interface{}, err error) {
	return mxj.NewMapXml([]byte(xmlStr), !keep)
}

func (g *Xml) Description() string {
	//return "parse xmlString to xml data"
	return "解析xml, 将xml格式转变为json结构"
}

func (g *Xml) Type() string {
	return "xml"
}

func (g *Xml) SampleConfig() string {
	return `{
       "type":"xml",
       "key":"myParseKey",
       "new":"myNewKey",
       "keep": "keepString"
    }`
}

func (g *Xml) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
		transforms.KeyKeepString,
	}
}

func (g *Xml) Stage() string {
	return transforms.StageAfterParser
}

func (g *Xml) Stats() StatsInfo {
	return g.stats
}

func (g *Xml) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("xml", func() transforms.Transformer {
		return &Xml{}
	})
}

func (g *Xml) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
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
		strVal = strings.TrimSpace(strVal)
		if len(strVal) < 1 {
			continue
		}
		xmlVal, perr := parseXml(strVal, g.Keep)
		if perr != nil {
			errNum, err = transforms.SetError(errNum, perr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}
		if len(g.news) == 0 {
			DeleteMapValue(transformInfo.CurData, g.keys...)
			g.news = g.keys
		}
		setErr := SetMapValue(transformInfo.CurData, xmlVal, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
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
