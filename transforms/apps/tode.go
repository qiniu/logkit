package apps

import (
	"errors"
	"strings"
	"sync"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const KeyTode = "key"

var (
	_ transforms.StatsTransformer = &Tode{}
	_ transforms.Transformer      = &Tode{}
	_ transforms.Initializer      = &Tode{}

	OptionTodeKey = Option{
		KeyName:      KeyTode,
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "按tode日志格式进行解析的键(" + KeyTode + ")",
		ToolTip:      "对该字段的值按tode的日志格式进行解析",
		Type:         transforms.TransformTypeString,
	}
)

type Tode struct {
	Key string `json:"key"`

	keys       []string
	stats      StatsInfo
	numRoutine int
}

func (t *Tode) Init() error {
	t.keys = GetKeys(t.Key)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	t.numRoutine = numRoutine
	return nil
}

func (t *Tode) Description() string {
	return `对于日志数据中的每条记录，按照tode日志的格式进行解析。`
}

func (t *Tode) SampleConfig() string {
	return `{
       "type":"tode",
       "key":"myParseKey",
    }`
}

func (t *Tode) ConfigOptions() []Option {
	return []Option{
		OptionTodeKey,
	}
}

func (t *Tode) Type() string {
	return "tode"
}

func (t *Tode) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("tode transformer not support rawTransform")
}

func (t *Tode) Stage() string {
	return transforms.StageAfterParser
}

func (t *Tode) Stats() StatsInfo {
	return t.stats
}

func (t *Tode) SetStats(err string) StatsInfo {
	t.stats.LastError = err
	return t.stats
}

func (t *Tode) Transform(datas []Data) ([]Data, error) {
	if len(t.keys) == 0 {
		t.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int
		numRoutine  = t.numRoutine

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

func parser(raw string) (data Data, err error) {
	//get timestamp
	startIndex := strings.IndexByte(raw, '[')
	endIndex := strings.IndexByte(raw, ']')
	if startIndex+1 > endIndex {
		return nil, errors.New("'[' index is greater than ']' index in string [" + raw + "].It's error format")
	}
	timestamp := raw[startIndex+1 : endIndex]
	var level, source, line string
	if endIndex+1 >= len(raw) {
		return nil, errors.New("log format is fault,failed to find log level in record [" + raw + "]")
	}
	//get log level
	arr := strings.SplitN(raw[endIndex+1:], ":", 5)
	if len(arr) != 5 {
		return nil, errors.New("string [" + raw + "] splitted by ':' length is not equal 5")
	}
	level = arr[0]

	//arr[1] is hostname,arr[2] is server name,arr[3] is pid
	//get source and line from arr[4]
	startIndex = strings.IndexByte(arr[4], '[')
	endIndex = strings.IndexByte(arr[4], ']')
	if startIndex+1 > endIndex {
		return nil, errors.New("'[' index is greater than ']' index in string [" + arr[4] + "].It's error format")
	}
	sourceAndLine := arr[4][startIndex+1 : endIndex]
	coll := strings.SplitN(sourceAndLine, ":", 2)
	if len(coll) != 2 {
		log.Warnf("failed to find source and line number in record [%s]", raw)
		source = "unknown"
		line = "unknown"
	} else {
		source = coll[0]
		line = coll[1]
	}
	data = Data{
		"timestamp_tode": timestamp,
		"level_tode":     level,
		"host_tode":      arr[1],
		"server_tode":    arr[2],
		"pid_tode":       arr[3],
		"source_tode":    source,
		"line_tode":      line,
	}
	//get message
	if endIndex+1 >= len(arr[4]) {
		log.Warnf("failed to find message in record [%s]", raw)
		data["message_tode"] = "unknown"
		return data, nil
	}
	index := strings.IndexByte(arr[4][endIndex+1:], ':')
	offset := endIndex + 1 + index + 1
	if index == -1 || offset >= len(arr[4]) {
		log.Warnf("log format is fault,failed to find message in record [%s]", raw)
		data["message_tode"] = "unknown"
		return data, nil
	}
	data["message_tode"] = strings.TrimSpace(arr[4][offset:])
	return data, nil
}

func init() {
	transforms.Add("tode", func() transforms.Transformer {
		return &Tode{}
	})
}

func (t *Tode) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		val, getErr := GetMapValue(transformInfo.CurData, t.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, t.Key)
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + t.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		if len(strVal) == 0 {
			errNum, err = transforms.SetError(errNum, errors.New("string value is empty,skip this record"), transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		newMap, err := parser(strings.TrimSpace(strVal))
		if err != nil {
			errNum, err = transforms.SetError(errNum, err, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		for k, v := range newMap {
			setErr := SetMapValue(transformInfo.CurData, v, false, k)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, t.Key)
			}
		}
		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			Err:     err,
			ErrNum:  errNum,
		}
	}
}
