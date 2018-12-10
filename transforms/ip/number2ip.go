package ip

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Number2Ip{}
	_ transforms.Transformer      = &Number2Ip{}
	_ transforms.Initializer      = &Number2Ip{}
)

type Number2Ip struct {
	Key string `json:"key"`
	New string `json:"new"`

	keys  []string
	news  []string
	stats StatsInfo

	numRoutine int
}

func (g *Number2Ip) Init() error {
	g.keys = GetKeys(g.Key)
	g.news = GetKeys(g.New)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Number2Ip) Transform(datas []Data) ([]Data, error) {
	if len(g.keys) == 0 {
		g.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int
		numRoutine  = g.numRoutine

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

func (g *Number2Ip) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("json transformer not support rawTransform")
}

func convertIp(ipInt int64) (ip string) {
	// need to do two bit shifting and “0xff” masking
	b0 := strconv.FormatInt((ipInt>>24)&0xff, 10)
	b1 := strconv.FormatInt((ipInt>>16)&0xff, 10)
	b2 := strconv.FormatInt((ipInt>>8)&0xff, 10)
	b3 := strconv.FormatInt(ipInt&0xff, 10)
	return b0 + "." + b1 + "." + b2 + "." + b3
}

func (g *Number2Ip) Description() string {
	return `将整型数据/字符串转换为ip, 我们认为该数字为32位2进制数的十进制表示法，如整型数据为为 223556667，转换后变为 13.83.52.59`
}

func (g *Number2Ip) Type() string {
	return "number2ip"
}

func (g *Number2Ip) SampleConfig() string {
	return `{
       "type":"number2ip",
       "key":"myParseKey",
       "new":"myNewKey"
    }`
}

func (g *Number2Ip) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
	}
}

func (g *Number2Ip) Stage() string {
	return transforms.StageAfterParser
}

func (g *Number2Ip) Stats() StatsInfo {
	return g.stats
}

func (g *Number2Ip) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("number2ip", func() transforms.Transformer {
		return &Number2Ip{}
	})
}

func (g *Number2Ip) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err, convertErr error
		errNum          int
	)
	news := g.news
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, g.keys...)
		if getErr != nil {
			errNum++
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     errors.New("transform key " + g.Key + " not exist in data"),
				ErrNum:  errNum,
			}
			continue
		}
		var number int64
		switch newVal := val.(type) {
		case int64:
			number = newVal
		case int:
			number = int64(newVal)
		case int32:
			number = int64(newVal)
		case int16:
			number = int64(newVal)
		case uint64:
			number = int64(newVal)
		case uint32:
			number = int64(newVal)
		case json.Number:
			number, convertErr = newVal.Int64()
			if convertErr != nil {
				errNum++
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					Err:     errors.New("transform key " + g.Key + " data type is not int64"),
					ErrNum:  errNum,
				}
				continue
			}
		case string:
			number, convertErr = strconv.ParseInt(newVal, 10, 64)
			if convertErr != nil {
				errNum, err = transforms.SetError(errNum, convertErr, transforms.General, "")
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					Err:     err,
					ErrNum:  errNum,
				}
				continue
			}
		default:
			typeErr := errors.New("transform key " + g.Key + " data type is not correct")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}

		ipVal := convertIp(number)
		if len(news) == 0 {
			DeleteMapValue(transformInfo.CurData, g.keys...)
			news = g.keys
		}
		setErr := SetMapValue(transformInfo.CurData, ipVal, false, news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
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
