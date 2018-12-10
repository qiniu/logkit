package aws

import (
	"errors"
	"fmt"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const cloudTrailRecords = "Records"

var (
	_ transforms.StatsTransformer = &CloudTrail{}
	_ transforms.Transformer      = &CloudTrail{}
	_ transforms.Initializer      = &CloudTrail{}
)

type CloudTrail struct {
	stats      StatsInfo
	numRoutine int
}

func (g *CloudTrail) Init() error {
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *CloudTrail) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("cloudtrail transformer not support rawTransform")
}

func (g *CloudTrail) Transform(datas []Data) ([]Data, error) {
	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int
		numRoutine  = g.numRoutine
		resData     = make([]Data, 0, dataLen)

		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go transform(dataPipeline, resultChan, wg)
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
		resData = append(resData, transformResult.CurDatas...)
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
	return resData, fmtErr

	//var err, cloudTrailErr error
	//errNum := 0
	//var newData []Data
	//for _, v := range datas {
	//	newData, cloudTrailErr = transform(v)
	//	if cloudTrailErr != nil {
	//		errNum, err = transforms.SetError(errNum, cloudTrailErr, transforms.General, "")
	//	}
	//	//不管有没有错误发生，都把返回的newdata加进来，让部分成功的情况也能解析出数据，同时也把原来的数据给进来
	//	retdata = append(retdata, newData...)
	//}
	//
	//g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	//return retdata, fmtErr
}

//如果有错误发生，要把原来的数据给回去
func transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		errNum int
	)
	for transformInfo := range dataPipeline {
		var transformResult = transforms.TransformResult{CurDatas: make([]Data, 0, 100)}
		records, ok := transformInfo.CurData[cloudTrailRecords]
		if !ok {
			errNum++
			resultChan <- transforms.TransformResult{
				Index:    transformInfo.Index,
				CurDatas: []Data{transformInfo.CurData},
				Err:      errors.New("data is not valid aws cloudtrail format as Records field is empty"),
				ErrNum:   errNum,
			}
			continue
		}
		validr, ok := records.([]interface{})
		if !ok {
			errNum++
			resultChan <- transforms.TransformResult{
				Index:    transformInfo.Index,
				CurDatas: []Data{transformInfo.CurData},
				Err:      errors.New("data is not valid aws cloudtrail format as Records field is not []interface{}"),
				ErrNum:   errNum,
			}
			continue
		}
		var (
			failcnt    int
			failreason string
		)
		for _, v := range validr {
			mydata, ok := v.(map[string]interface{})
			if !ok {
				failcnt++
				failreason = "data in Records can not be assert to map[string]interface"
				continue
			}
			mydata = AddExtraField(transformInfo.CurData, mydata)
			transformResult.CurDatas = append(transformResult.CurDatas, mydata)
		}
		if failcnt > 0 {
			errNum++
			transformResult.CurDatas = append(transformResult.CurDatas, transformInfo.CurData)
			transformResult.Err = fmt.Errorf("we got total %v errors as %v", failcnt, failreason)
			transformResult.ErrNum = errNum
			transformResult.Index = transformInfo.Index
			resultChan <- transformResult
			continue
		}

		transformResult.Index = transformInfo.Index
		resultChan <- transformResult
	}
	wg.Done()
}

func AddExtraField(olddata, newData Data) Data {
	for k, v := range olddata {
		if k == cloudTrailRecords {
			continue
		}
		newData[k] = v
	}
	return newData
}

func (g *CloudTrail) Description() string {
	//return "cloudtrail can convert data to cloudtrail format data"
	return "cloudtrail transformer 将AWS CloudTrail 数据解析处理"
}

func (g *CloudTrail) Type() string {
	return "cloudtrail"
}

func (g *CloudTrail) SampleConfig() string {
	return `{
		"type":"cloudtrail"
	}`
}

func (g *CloudTrail) ConfigOptions() []Option {
	return []Option{}
}

func (g *CloudTrail) Stage() string {
	return transforms.StageAfterParser
}

func (g *CloudTrail) Stats() StatsInfo {
	return g.stats
}

func (g *CloudTrail) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("cloudtrail", func() transforms.Transformer {
		return &CloudTrail{}
	})
}
