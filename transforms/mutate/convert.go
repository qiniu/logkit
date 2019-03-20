package mutate

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/qiniu/pandora-go-sdk/pipeline"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Converter{}
	_ transforms.Transformer      = &Converter{}
	_ transforms.Initializer      = &Converter{}
)

type Converter struct {
	DSL string `json:"dsl"`

	stats   StatsInfo
	schemas []DslSchemaEntry
	keysMap map[int][]string

	numRoutine int
}

func (g *Converter) Init() error {
	schemas, err := ParseDsl(g.DSL, 0)
	if err != nil {
		return errors.New("convert typedsl " + g.DSL + " to schema error: " + err.Error())
	}
	g.schemas = schemas
	g.keysMap = map[int][]string{}
	for i, sc := range g.schemas {
		g.keysMap[i] = GetKeys(sc.Key)
	}

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Converter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("convert transformer not support rawTransform")
}

func (g *Converter) Transform(datas []Data) ([]Data, error) {
	if g.schemas == nil {
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
	if len(g.schemas) == 0 {
		err = errors.New("no valid dsl[" + g.DSL + "] to schema, please enter correct format dsl: \"field type\"")
		g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(dataLen), int64(dataLen), g.Type())
		return datas, err
	}

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

func (g *Converter) Description() string {
	//return "convert can use dsl to convert multi-field data to specify data type"
	return `将dsl指定的多个数据字段和类型转换为指定的数据格式, 如将field1转为long则写为 "field1 long"`
}

func (g *Converter) Type() string {
	return "convert"
}

func (g *Converter) SampleConfig() string {
	return `{
		"type":"convert",
		"dsl":"fieldone string"
	}`
}

func (g *Converter) ConfigOptions() []Option {
	return []Option{
		{
			KeyName:      "dsl",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "fieldone string",
			DefaultNoUse: true,
			Description:  "数据转换的dsl描述(dsl)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Converter) Stage() string {
	return transforms.StageAfterParser
}

func (g *Converter) Stats() StatsInfo {
	return g.stats
}

func (g *Converter) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("convert", func() transforms.Transformer {
		return &Converter{}
	})
}

type DslSchemaEntry struct {
	Key       string           `json:"key"`
	ValueType string           `json:"valtype"`
	Default   interface{}      `json:"default,omitempty"`
	ElemType  string           `json:"elemtype,omitempty"`
	Schema    []DslSchemaEntry `json:"schema,omitempty"`
}

func getField(f string) (key, valueType, elementType string, defaultVal interface{}, err error) {
	f = strings.TrimSpace(f)
	if f == "" {
		return "", "", "", nil, nil
	}
	first := strings.IndexFunc(f, unicode.IsSpace)
	if first == -1 {
		return f, "", "", nil, nil
	}
	var defaultStr string
	key = f[:first]
	f = strings.TrimSpace(f[first:])
	last := strings.LastIndexFunc(f, unicode.IsSpace)
	if last != -1 {
		if !strings.ContainsAny(f[last:], ")}") {
			defaultStr = f[last:]
			f = f[:last]
		}
	}
	valueType = f
	//处理arrary类型
	if beg := strings.Index(valueType, "("); beg != -1 {
		ed := strings.Index(valueType, ")")
		if ed <= beg {
			return key, valueType, "", nil, errors.New("field schema " + f + " has no type specified")
		}
		elementType, err = getRawType(valueType[beg+1 : ed])
		if err != nil {
			err = errors.New("array 【" + f + "】: " + err.Error() + ", key " + key + " valuetype " + valueType)
		}
		valueType = "array"
		return key, valueType, elementType, nil, err
	}
	valueType, err = getRawType(valueType)
	if err != nil {
		err = errors.New("normal 【" + f + "】: " + err.Error() + ", key " + key + " valuetype " + valueType)
		return key, valueType, elementType, nil, err
	}
	if len(defaultStr) > 0 {
		defaultVal, err = defaultConvert(defaultStr, valueType)
	}
	return key, valueType, elementType, defaultVal, err
}

/*
DSL创建的规则为`<字段名称> <类型>`,字段名称和类型用空格符隔开，不同字段用逗号隔开。若字段必填，则在类型前加`*`号表示。
    * pandora date类型：`date`,`DATE`,`d`,`D`
    * pandora long类型：`long`,`LONG`,`l`,`L`
    * pandora float类型: `float`,`FLOAT`,`F`,`f`
    * pandora string类型: `string`,`STRING`,`S`,`s`
    * pandora bool类型:  `bool`,`BOOL`,`B`,`b`,`boolean`
    * pandora jsonstring类型： `json`,"JSON","jsonstring","JSONSTRING","j","J"
    * pandora array类型: `array`,`ARRAY`,`A`,`a`;括号中跟具体array元素的类型，如a(l)，表示array里面都是long。
    * pandora map类型: `map`,`MAP`,`M`,`m`;使用花括号表示具体类型，表达map里面的元素，如map{a l,b map{c b,x s}}, 表示map结构体里包含a字段，类型是long，b字段又是一个map，里面包含c字段，类型是bool，还包含x字段，类型是string。
*/

func getRawType(tp string) (schemaType string, err error) {
	tp = strings.TrimSpace(tp)
	schemaType = strings.ToLower(tp)
	switch schemaType {
	case "l", "long":
		schemaType = pipeline.PandoraTypeLong
	case "f", "float":
		schemaType = pipeline.PandoraTypeFloat
	case "s", "string":
		schemaType = pipeline.PandoraTypeString
	case "d", "date":
		schemaType = pipeline.PandoraTypeDate
	case "a", "array":
		return schemaType, errors.New("arrary type must specify data type surrounded by ( )")
	case "m", "map":
		schemaType = pipeline.PandoraTypeMap
	case "b", "bool", "boolean":
		schemaType = pipeline.PandoraTypeBool
	case "j", "json", "jsonstring":
		schemaType = pipeline.PandoraTypeJsonString
	case "": //这个是一种缺省
	default:
		return schemaType, errors.New("schema type " + schemaType + " not supperted")
	}
	return schemaType, nil
}

func ParseDsl(dsl string, depth int) ([]DslSchemaEntry, error) {
	if depth > 5 {
		return nil, errors.New("DslSchemaEntry are nested out of limit 5")
	}
	schemas := make([]DslSchemaEntry, 0)
	dsl = strings.TrimSpace(dsl)
	start := 0
	nestbalance := 0
	neststart, nestend := -1, -1
	dsl += "," //增加一个','保证一定是以","为终结
	dupcheck := make(map[string]struct{})
	for end, c := range dsl {
		if start > end {
			return schemas, errors.New("parse dsl inner error: start index is larger than end")
		}
		switch c {
		case '{':
			if nestbalance == 0 {
				neststart = end
			}
			nestbalance++
		case '}':
			nestbalance--
			if nestbalance == 0 {
				nestend = end
				if nestend <= neststart {
					return schemas, errors.New("parse dsl error: nest end should never less or equal than nest start")
				}
				subschemas, err := ParseDsl(dsl[neststart+1:nestend], depth+1)
				if err != nil {
					return schemas, err
				}
				if neststart <= start {
					return schemas, errors.New("parse dsl error: map{} not specified")
				}
				key, valueType, _, defaultVal, err := getField(dsl[start:neststart])
				if err != nil {
					return schemas, err
				}
				if key != "" {
					if _, ok := dupcheck[key]; ok {
						return schemas, errors.New("parse dsl error: " + key + " is duplicated key")
					}
					dupcheck[key] = struct{}{}
					if valueType == "" {
						valueType = "map"
					}
					schemas = append(schemas, DslSchemaEntry{
						Key:       key,
						ValueType: valueType,
						Default:   defaultVal,
						Schema:    subschemas,
					})
				}
				start = end + 1
			}
		case ',', '\n':
			if nestbalance == 0 {
				if start < end {
					key, valueType, elemtype, defaultVal, err := getField(strings.TrimSpace(dsl[start:end]))
					if err != nil {
						return schemas, err
					}
					if key != "" {
						if _, ok := dupcheck[key]; ok {
							return schemas, errors.New("parse dsl error: " + key + " is duplicated key")
						}
						dupcheck[key] = struct{}{}
						if valueType == "" {
							valueType = pipeline.PandoraTypeString
						}
						schemas = append(schemas, DslSchemaEntry{
							Key:       key,
							ValueType: valueType,
							Default:   defaultVal,
							ElemType:  elemtype,
						})
					}
				}
				start = end + 1
			}
		}
	}
	if nestbalance != 0 {
		return schemas, errors.New("parse dsl error: { and } not match")
	}
	return schemas, nil
}

func defaultConvert(defaultStr, valueType string) (converted interface{}, err error) {
	defaultStr = strings.TrimSpace(defaultStr)
	switch valueType {
	case pipeline.PandoraTypeLong:
		if converted, err = strconv.ParseInt(defaultStr, 10, 64); err == nil {
			return converted, nil
		}
		var floatc float64
		if floatc, err = strconv.ParseFloat(defaultStr, 10); err == nil {
			converted = int64(floatc)
			return converted, nil
		}
		return converted, err
	case pipeline.PandoraTypeFloat:
		return strconv.ParseFloat(defaultStr, 10)
	case pipeline.PandoraTypeString, pipeline.PandoraTypeJsonString, pipeline.PandoraTypeDate:
		return defaultStr, nil
	case pipeline.PandoraTypeBool:
		return strconv.ParseBool(defaultStr)
	default:
		return nil, errors.New("not support default value for type(" + valueType + ")")
	}
}

func dataConvert(data interface{}, schema DslSchemaEntry) (converted interface{}, err error) {
	switch schema.ValueType {
	case pipeline.PandoraTypeLong:
		value := reflect.ValueOf(data)
		switch value.Kind() {
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return data, nil
		case reflect.Float32, reflect.Float64:
			return int64(value.Float()), nil
		case reflect.String:
			val := strings.TrimSpace(value.String())
			if val == "" {
				return schema.Default, nil
			}
			if converted, err = strconv.ParseInt(val, 10, 64); err == nil {
				return converted, nil
			}
			var floatc float64
			if floatc, err = strconv.ParseFloat(val, 10); err == nil {
				return int64(floatc), nil
			}
		}
	case pipeline.PandoraTypeFloat:
		value := reflect.ValueOf(data)
		switch value.Kind() {
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return data, nil
		case reflect.Float32, reflect.Float64:
			return data, nil
		case reflect.String:
			val := strings.TrimSpace(value.String())
			if val == "" {
				return schema.Default, nil
			}
			if converted, err = strconv.ParseFloat(val, 10); err == nil {
				return converted, nil
			}
		}
	case pipeline.PandoraTypeString:
		switch value := data.(type) {
		case json.Number:
			return string(value), nil
		case map[string]interface{}:
			str, err := json.Marshal(value)
			if err == nil {
				return string(str), nil
			}
		case []interface{}:
			str, err := json.Marshal(value)
			if err == nil {
				return string(str), nil
			}
		case time.Time:
			return value.Format(time.RFC3339Nano), nil
		case *time.Time:
			return value.Format(time.RFC3339Nano), nil
		case nil:
			if schema.Default != nil {
				return schema.Default, nil
			}
			return "", nil
		default:
			v := reflect.ValueOf(data)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return strconv.FormatInt(v.Int(), 10), nil
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				return strconv.FormatUint(v.Uint(), 10), nil
			case reflect.Float32, reflect.Float64:
				return strconv.FormatFloat(v.Float(), 'f', -1, 64), nil
			case reflect.Bool:
				return strconv.FormatBool(v.Bool()), nil
			case reflect.Slice, reflect.Array:
				str, err := json.Marshal(data)
				if err == nil {
					return string(str), nil
				}
			case reflect.String:
				return v.String(), nil
			}
		}
	case pipeline.PandoraTypeJsonString:
		switch value := data.(type) {
		case map[string]interface{}:
			str, err := json.Marshal(value)
			if err == nil {
				return string(str), nil
			}
		case []interface{}:
			str, err := json.Marshal(value)
			if err == nil {
				return string(str), nil
			}
		case nil:
			if schema.Default != nil {
				return schema.Default, nil
			}
			return nil, nil
		default:
			v := reflect.ValueOf(data)
			switch v.Kind() {
			case reflect.String:
				return v.String(), nil
			}
		}
	case pipeline.PandoraTypeDate:
		switch value := data.(type) {
		case time.Time, *time.Time:
			return value, nil
		}
		if converted, err = ConvertDate("", "", 0, time.UTC, data); err == nil {
			return converted, nil
		}
	case pipeline.PandoraTypeBool:
		switch value := data.(type) {
		case bool, *bool:
			return value, nil
		case string:
			if converted, err = strconv.ParseBool(value); err == nil {
				return converted, nil
			}
		}
	case pipeline.PandoraTypeArray:
		ret := make([]interface{}, 0)
		switch value := data.(type) {
		case []interface{}:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []string:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []int:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []int64:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []json.Number:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []float64:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []bool:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []float32:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []int8:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []int16:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []int32:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []uint:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []uint8:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []uint16:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []uint32:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case []uint64:
			for _, j := range value {
				vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
				if serr == nil {
					ret = append(ret, vi)
				} else {
					err = serr
				}
			}
			if err == nil {
				return ret, nil
			}
		case string:
			newdata := make([]interface{}, 0)
			err = json.Unmarshal([]byte(value), &newdata)
			if err == nil {
				for _, j := range newdata {
					vi, serr := dataConvert(j, DslSchemaEntry{ValueType: schema.ElemType})
					if serr == nil {
						ret = append(ret, vi)
					} else {
						err = serr
					}
				}
				if err == nil {
					return ret, nil
				}
			}
		}
	case pipeline.PandoraTypeMap:
		switch value := data.(type) {
		case map[string]interface{}:
			return mapDataConvert(value, schema.Schema), nil
		case string:
			newdata := make(map[string]interface{})
			err = json.Unmarshal([]byte(value), &newdata)
			if err == nil {
				return mapDataConvert(newdata, schema.Schema), nil
			}
		}
	}
	if schema.Default != nil {
		return schema.Default, nil
	}
	return data, fmt.Errorf("can not convert data[%v] type(%v) to type(%v), err %v", data, reflect.TypeOf(data), schema.ValueType, err)
}

func mapDataConvert(mpvalue map[string]interface{}, schemas []DslSchemaEntry) (converted interface{}) {
	for _, v := range schemas {
		if subv, ok := mpvalue[v.Key]; ok {
			subconverted, err := dataConvert(subv, v)
			if err != nil {
				continue
			}
			mpvalue[v.Key] = subconverted
		}
	}
	return mpvalue
}

func (g *Converter) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		for k, keys := range g.keysMap {
			val, getErr := GetMapValue(transformInfo.CurData, keys...)
			if getErr != nil {
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.schemas[k].Key)
				continue
			}
			val, convertErr := dataConvert(val, g.schemas[k])
			if convertErr != nil {
				errNum, err = transforms.SetError(errNum, convertErr, transforms.General, "")
			}
			if val == nil {
				DeleteMapValue(transformInfo.CurData, keys...)
				continue
			}
			setErr := SetMapValue(transformInfo.CurData, val, false, keys...)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, strings.Join(keys, "."))
			}
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
