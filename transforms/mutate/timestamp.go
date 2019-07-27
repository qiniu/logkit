package mutate

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Timestamp{}
	_ transforms.Transformer      = &Timestamp{}
	_ transforms.Initializer      = &Timestamp{}
)

const (
	Seconds     = "seconds"
	MilliSecond = "milliseconds"
	MicroSecond = "microseconds"
	NanoSeconds = "nanoseconds"
)

type Timestamp struct {
	Key          string `json:"key"`
	New          string `json:"new"`
	LayoutBefore string `json:"time_layout_before"`
	Offset       int    `json:"offset"`
	Precision    string `json:"precision"`
	Override     bool   `json:"override"`

	keys  []string
	news  []string
	loc   *time.Location
	stats StatsInfo

	numRoutine int
}

func (t *Timestamp) Init() error {
	t.Precision = strings.TrimSpace(t.Precision)
	t.Precision = strings.ToLower(t.Precision)

	if t.LayoutBefore != "" {
		t.LayoutBefore = strings.Replace(t.LayoutBefore, ",", ".", -1)
	}

	// 如果用户设置了 offset，则不默认使用本地时区
	t.loc = time.Local
	if t.Offset != 0 {
		t.loc = time.UTC
	}

	t.keys = GetKeys(t.Key)
	if t.New == "" {
		t.New = t.Key
		t.Override = true
	}
	t.news = GetKeys(t.New)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	t.numRoutine = numRoutine
	return nil
}

func (t *Timestamp) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("timestamp transformer not support rawTransform")
}

func (t *Timestamp) Transform(datas []Data) ([]Data, error) {
	if len(t.keys) == 0 {
		t.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = t.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go t.transform(dataPipeline, resultChan, wg)
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

	t.stats, fmtErr = transforms.SetStatsInfo(err, t.stats, int64(errNum), int64(dataLen), t.Type())
	return datas, fmtErr
}

func (t *Timestamp) Description() string {
	//return "timestamp can convert string to timestamp"
	return `时间戳转换, 如设置标签{key:"2017/03/28 15:41:53 -0000"}, 则转换后为 {"key":1490715713}`
}

func (t *Timestamp) Type() string {
	return "timestamp"
}

func (t *Timestamp) SampleConfig() string {
	return `{
		"type":"timestamp",
		"key":"my_field_keyname",
		"new":"my_field_newname",
		"override":false
	}`
}

func (t *Timestamp) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		{
			KeyName:       "precision",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{Seconds, MilliSecond, MicroSecond, NanoSeconds},
			Default:       Seconds,
			Advance:       true,
			DefaultNoUse:  false,
			Description:   "精度(precision)",
			ToolTip:       "不填默认为秒",
			Type:          transforms.TransformTypeString,
		},
		{
			KeyName:      "time_layout_before",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Advance:      true,
			Description:  "当前时间样式(不填自动解析)(time_layout_before)",
			Type:         transforms.TransformTypeString,
		},
		transforms.KeyTimezoneoffset,
		transforms.KeyOverride,
	}
}

func (t *Timestamp) Stage() string {
	return transforms.StageAfterParser
}

func (t *Timestamp) Stats() StatsInfo {
	return t.stats
}

func (t *Timestamp) SetStats(err string) StatsInfo {
	t.stats.LastError = err
	return t.stats
}

func init() {
	transforms.Add("timestamp", func() transforms.Transformer {
		return &Timestamp{}
	})
}

func (t *Timestamp) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, t.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}

		if !t.Override {
			_, getErr := GetMapValue(transformInfo.CurData, t.news...)
			if getErr == nil {
				existErr := errors.New("the key " + t.Key + " already exists")
				errNum, err = transforms.SetError(errNum, existErr, transforms.General, "")
				resultChan <- transforms.TransformResult{
					Index:   transformInfo.Index,
					CurData: transformInfo.CurData,
					Err:     err,
					ErrNum:  errNum,
				}
				continue
			}
		}

		valStr, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("only support string, but got: %T, origin value is: %v", val, val)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
		}

		valStr = strings.Replace(valStr, ",", ".", -1)
		var valTime time.Time
		if t.LayoutBefore != "" {
			valTime, err = time.ParseInLocation(t.LayoutBefore, valStr, t.loc)
		} else {
			valTime, err = times.StrToTimeLocation(valStr, t.loc)
		}
		if err != nil {
			typeErr := fmt.Errorf("can not parse %v with layout %v", valStr, t.LayoutBefore)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
		}
		valTime = valTime.Add(time.Duration(t.Offset) * time.Hour)
		if valTime.Year() == 0 {
			valTime = valTime.AddDate(time.Now().Year(), 0, 0)
		}

		var valTimestamp = valTime.UnixNano()
		switch t.Precision {
		case Seconds:
			valTimestamp = valTime.Unix()
		case MilliSecond:
			valTimestamp = valTimestamp / 1000000
		case MicroSecond:
			valTimestamp = valTimestamp / 1000
		case NanoSeconds:
		default:
			valTimestamp = valTime.Unix()
		}
		setErr := SetMapValue(transformInfo.CurData, valTimestamp, false, t.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, t.New)
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
