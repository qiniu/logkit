package mutate

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Filter{}
	_ transforms.Transformer      = &Filter{}
	_ transforms.Initializer      = &Filter{}
)

type Filter struct {
	Key           string `json:"key"`
	StageTime     string `json:"stage"`
	stats         StatsInfo
	KeepData      bool   `json:"keep_data"` // 暂时没用到
	RemovePattern string `json:"remove_pattern"`
	removeRegex   *regexp.Regexp

	discardKeys [][]string

	numRoutine int
}

func (f *Filter) Init() error {
	discardKeys := strings.Split(f.Key, ",")
	f.discardKeys = make([][]string, len(discardKeys))
	for i := range f.discardKeys {
		f.discardKeys[i] = GetKeys(discardKeys[i])
	}
	if f.RemovePattern != "" {
		removeRegex, err := regexp.Compile(f.RemovePattern)
		if err != nil {
			return errors.New("regex compile failed: " + err.Error())
		}
		f.removeRegex = removeRegex
	}
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	f.numRoutine = numRoutine
	return nil
}

func (f *Filter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("convert transformer not support rawTransform")
}

func (f *Filter) Transform(datas []Data) ([]Data, error) {
	if f.discardKeys == nil {
		f.Init()
	}

	var (
		err, fmtErr error
		errNum      int
	)

	numRoutine := f.numRoutine
	if len(datas) < numRoutine {
		numRoutine = len(datas)
	}
	dataPipline := make(chan transforms.TransformInfo)
	resultChan := make(chan transforms.TransformResult)

	wg := new(sync.WaitGroup)
	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go f.transform(dataPipline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipline <- transforms.TransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipline)
	}()

	var transformResultSlice = make(transforms.TransformResultSlice, 0, len(datas))
	for resultInfo := range resultChan {
		transformResultSlice = append(transformResultSlice, resultInfo)
	}
	if numRoutine > 1 {
		sort.Stable(transformResultSlice)
	}

	results := make([]Data, 0, len(datas))
	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
			continue
		}
		results = append(results, transformResult.CurData)
	}

	f.stats, fmtErr = transforms.SetStatsInfo(err, f.stats, int64(errNum), int64(len(datas)), f.Type())
	return results, fmtErr

	//var (
	//	errNum int
	//	err    error
	//	result = make([]Data, 0, len(datas))
	//)
	//for i := range datas {
	//	remove := false
	//	for _, keys := range f.discardKeys {
	//		val, getErr := GetMapValue(datas[i], keys...)
	//		if getErr != nil {
	//			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, strings.Join(keys, "."))
	//			continue
	//		}
	//		if f.removeRegex == nil || f.removeRegex.MatchString(fmt.Sprintf("%v", val)) {
	//			remove = true
	//			break
	//		}
	//	}
	//	if remove {
	//		continue
	//	}
	//	result = append(result, datas[i])
	//}
	//
	//f.stats, _ = transforms.SetStatsInfo(err, f.stats, 0, int64(len(datas)), f.Type())
	//return result, nil
}

func (f *Filter) Description() string {
	//return "convert can use dsl to convert multi-field data to specify data type"
	return `将符合条件的数据丢弃`
}

func (f *Filter) Type() string {
	return "filter"
}

func (f *Filter) SampleConfig() string {
	return `{
		"type":"filter",
		"key":"a.b,c",
		"remove_pattern":".* [DEBUG][.*"
	}`
}

func (f *Filter) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:      "remove_pattern",
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Advance:      true,
			Placeholder:  "fieldone string",
			DefaultNoUse: true,
			Description:  "去除数据匹配的正则表达式(remove_pattern)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (f *Filter) Stage() string {
	return transforms.StageAfterParser
}

func (f *Filter) Stats() StatsInfo {
	return f.stats
}

func (f *Filter) SetStats(err string) StatsInfo {
	f.stats.LastError = err
	return f.stats
}

func init() {
	transforms.Add("filter", func() transforms.Transformer {
		return &Filter{}
	})
}

func (f *Filter) transform(dataPipline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)

	for transformInfo := range dataPipline {
		err = nil
		errNum = 0

		remove := false
		for _, keys := range f.discardKeys {
			val, getErr := GetMapValue(transformInfo.CurData, keys...)
			if getErr != nil {
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, strings.Join(keys, "."))
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					ErrNum:  errNum,
					Err:     err,
				}
				continue
			}
			if f.removeRegex == nil || f.removeRegex.MatchString(fmt.Sprintf("%v", val)) {
				remove = true
				break
			}
		}
		if remove {
			continue
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
		}
	}
	wg.Done()
}
