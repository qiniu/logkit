package mutate

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/go-logfmt/logfmt"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &KV{}
	_ transforms.Transformer      = &KV{}
	_ transforms.Initializer      = &KV{}

	OptionKVSplitter = Option{
		KeyName:      "splitter",
		ChooseOnly:   false,
		Default:      "=",
		Required:     false,
		Placeholder:  "my_field_splitter",
		DefaultNoUse: true,
		Description:  "分隔符",
		ToolTip:      "使用该分隔符分隔键值对",
		Type:         transforms.TransformTypeString,
	}
)

type KV struct {
	Key        string `json:"key"`
	New        string `json:"new"`
	Splitter   string `json:"splitter"`
	KeepString bool   `json:"keep_string"`
	DiscardKey bool   `json:"discard_key"`

	stats      StatsInfo
	keys       []string
	news       []string
	numRoutine int
}

func (k *KV) Init() error {
	if k.Splitter == "" {
		k.Splitter = "="
	}
	k.keys = GetKeys(k.Key)
	if len(k.keys) < 1 {
		return errors.New("invalid key: " + k.Key + ", key can't be empty")
	}
	k.New = strings.TrimSpace(k.New)
	if k.New == "" {
		k.news = k.keys[:len(k.keys)-1]
	} else {
		k.news = GetKeys(k.New)
	}
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	k.numRoutine = numRoutine
	return nil
}

func (k *KV) Description() string {
	return `对于日志数据中的每条记录，解析键值对。`
}

func (k *KV) SampleConfig() string {
	return `{
       "type":"keyvalue",
       "key":"myParseKey",
    }`
}

func (k *KV) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		OptionKVSplitter,
		{
			KeyName:       "keep_string",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			Advance:       true,
			Placeholder:   "my_field_keep_string",
			DefaultNoUse:  false,
			Description:   "数字是否以字符串形式展现",
			ToolTip:       "数字是否以字符串形式展现",
			Type:          transforms.TransformTypeBoolean,
		},
		{
			KeyName:       "discard_key",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "删除原始key指定的字段名和字段值",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
	}
}

func (k *KV) Type() string {
	return "keyvalue"
}

func (k *KV) Stage() string {
	return transforms.StageAfterParser
}

func (k *KV) Stats() StatsInfo {
	return k.stats
}

func (k *KV) SetStats(err string) StatsInfo {
	k.stats.LastError = err
	return k.stats
}

func (k *KV) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("keyvalue transformer not support rawTransform")
}

func (k *KV) Transform(datas []Data) ([]Data, error) {
	if len(k.keys) == 0 {
		k.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = k.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)
	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go k.transform(dataPipeline, resultChan, wg)
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
			continue
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	k.stats, fmtErr = transforms.SetStatsInfo(err, k.stats, int64(errNum), int64(dataLen), k.Type())
	return datas, fmtErr
}

func init() {
	transforms.Add("keyvalue", func() transforms.Transformer {
		return &KV{}
	})
}

func (k *KV) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, k.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, k.Key)
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
			typeErr := errors.New("transform key " + k.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}

		data, kvErr := kvTransform(strVal, k.Splitter, k.KeepString)
		if kvErr == nil && len(data) == 0 {
			kvErr = errors.New("no value matched in key value transform in " + k.Key)
		}
		if kvErr != nil {
			errNum, err = transforms.SetError(errNum, kvErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue

		}

		errNum, err = k.delete(transformInfo.CurData, errNum, err)
		if len(k.news) != 0 {
			setErr := SetMapValue(transformInfo.CurData, data, false, k.news...)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, k.New)
			}
		} else {
			for k, v := range data {
				transformInfo.CurData[k] = v
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

func kvTransform(strVal string, splitter string, keepString bool) (Data, error) {
	var (
		reader  = bytes.NewReader([]byte(strVal))
		decoder = logfmt.NewDecoder(reader)
		data    = make(Data, 0)
		fields  Data
	)
	for {
		ok := decoder.ScanRecord()
		if !ok {
			err := decoder.Err()
			if err != nil {
				return nil, err
			}
			//此错误仅用于当原始数据解析成功但无解析数据时，保留原始数据之用
			if len(fields) == 0 {
				log.Error("no value was parsed after logfmt, will keep origin data in pandora_stash if disable_record_errdata field is false")
				break
			}
			break
		}
		fields = make(Data)
		for decoder.ScanKeyval(splitter[0]) {
			if string(decoder.Value()) == "" {
				continue
			}
			//type conversions
			value := string(decoder.Value())
			if !keepString {
				if fValue, err := strconv.ParseFloat(value, 64); err == nil {
					fields[string(decoder.Key())] = fValue
					continue
				}
			}
			if bValue, err := strconv.ParseBool(value); err == nil {
				fields[string(decoder.Key())] = bValue
				continue
			}
			fields[string(decoder.Key())] = value
		}
		if len(fields) == 0 {
			continue
		}

		for fieldKey, fieldVal := range fields {
			data[fieldKey] = fieldVal
		}
	}
	return data, nil
}

func (k *KV) delete(curData Data, errNum int, err error) (int, error) {
	if !k.DiscardKey {
		return errNum, err
	}

	keys := make([]string, len(k.keys))
	copy(keys, k.keys)
	for len(keys) > 0 {
		DeleteMapValue(curData, keys...)
		val, getErr := GetMapValue(curData, keys[:len(keys)-1]...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, strings.Join(keys, "."))
			continue
		}
		if val == nil {
			keys = keys[:len(keys)-1]
			continue
		}
		if valMap, ok := val.(map[string]interface{}); ok && len(valMap) == 0 {
			keys = keys[:len(keys)-1]
			continue
		}
		break
	}
	return errNum, err
}
