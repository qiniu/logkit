package mutate

import (
	"errors"
	"fmt"
	"regexp"
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
	Mode          string `json:"mode"`
	Pattern       string `json:"pattern"`
	RemovePattern string `json:"remove_pattern"`
	Regex         *regexp.Regexp

	keys [][]string

	numRoutine int
}

const (
	Keep   = "keep"
	Remove = "remove"
)

func (f *Filter) Init() error {
	keys := strings.Split(f.Key, ",")
	f.keys = make([][]string, len(keys))
	for i := range f.keys {
		f.keys[i] = GetKeys(keys[i])
	}
	f.Mode = strings.TrimSpace(f.Mode)
	f.Mode = strings.ToLower(f.Mode)
	if f.Mode == "" {
		f.Mode = Remove
	}
	if f.RemovePattern != "" {
		f.Pattern = f.RemovePattern
	}
	if f.Pattern != "" {
		Regex, err := regexp.Compile(f.Pattern)
		if err != nil {
			return errors.New("regex compile failed: " + err.Error())
		}
		f.Regex = Regex
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
	if f.keys == nil {
		f.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = f.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go f.transform(dataPipeline, resultChan, wg)
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

	var (
		transformResultSlice = make(transforms.TransformResultSlice, dataLen)
		results              = make([]Data, dataLen)
		resultIndex          = 0
	)
	for resultInfo := range resultChan {
		transformResultSlice[resultInfo.Index] = resultInfo
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.CurData == nil {
			continue
		}

		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}

		results[resultIndex] = transformResult.CurData
		resultIndex++
	}
	f.stats, fmtErr = transforms.SetStatsInfo(err, f.stats, int64(errNum), int64(dataLen), f.Type())
	return results[:resultIndex], fmtErr
}

func (f *Filter) Description() string {
	//return "convert can use dsl to convert multi-field data to specify data type"
	return `保留/移除符合条件的数据`
}

func (f *Filter) Type() string {
	return "filter"
}

func (f *Filter) SampleConfig() string {
	return `{
		"type":"filter",
		"key":"a.b,c",
		"pattern":".* [DEBUG][.*"
	}`
}

func (f *Filter) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		{
			KeyName:       "mode",
			ChooseOnly:    true,
			ChooseOptions: []interface{}{Keep, Remove},
			Default:       Remove,
			Required:      false,
			Advance:       true,
			Placeholder:   "mode string",
			DefaultNoUse:  false,
			Description:   "保留/移除匹配的正则表达式(pattern)的数据",
			Type:          transforms.TransformTypeString,
		},
		{
			KeyName:      "pattern",
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Advance:      true,
			Placeholder:  "pattern string",
			DefaultNoUse: true,
			Description:  "正则表达式(pattern)所匹配的数据做处理",
			ToolTip:      "保留或移除匹配正则表达式的数据，为空且mode为remove则丢弃整条数据，否则保留",
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

func (f *Filter) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)

	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		match := false
		for _, keys := range f.keys {
			val, getErr := GetMapValue(transformInfo.CurData, keys...)
			if getErr != nil {
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, strings.Join(keys, "."))
				continue
			}

			if f.Regex == nil || f.Regex.MatchString(fmt.Sprintf("%v", val)) {
				match = true
				break
			}
		}

		switch f.Mode {
		case Keep:
			if !match {
				continue
			}
		default:
			if match {
				continue
			}
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
