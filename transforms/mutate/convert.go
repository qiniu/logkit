package mutate

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/pipeline"
)

type Converter struct {
	DSL     string `json:"dsl"`
	stats   StatsInfo
	schemas []DslSchemaEntry
}

func (g *Converter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("convert transformer not support rawTransform")
}

func (g *Converter) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	if g.schemas == nil {
		schemas, err := ParseDsl(g.DSL, 0)
		if err != nil {
			err = fmt.Errorf("convert typedsl %s to schema error: %v", g.DSL, err)
			errnums = len(datas)
			g.schemas = make([]DslSchemaEntry, 0)
		} else {
			g.schemas = schemas
		}
	}
	if len(g.schemas) == 0 {
		err = fmt.Errorf("no valid dsl[%v] to schema, please enter correct format dsl: \"field type\"", g.DSL)
		errnums = len(datas)
	} else {
		keyss := map[int][]string{}
		for i, sc := range g.schemas {
			keys := GetKeys(sc.Key)
			keyss[i] = keys
		}
		for i := range datas {
			for k, keys := range keyss {
				val, gerr := GetMapValue(datas[i], keys...)
				if gerr != nil {
					errnums++
					err = fmt.Errorf("transform key %v not exist in data", g.schemas[k].Key)
					continue
				}
				val, err = dataConvert(val, g.schemas[k])
				if err != nil {
					errnums++
				}
				if val == nil {
					DeleteMapValue(datas[i], keys...)
					continue
				}
				SetMapValue(datas[i], val, false, keys...)
			}
		}
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform convert, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
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
		return
	}
	first := strings.IndexFunc(f, unicode.IsSpace)
	if first == -1 {
		key = f
		return
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
			err = fmt.Errorf("field schema %v has no type specified", f)
			return
		}
		elementType, err = getRawType(valueType[beg+1 : ed])
		if err != nil {
			err = fmt.Errorf("array 【%v】: %v, key %v valuetype %v", f, err, key, valueType)
		}
		valueType = "array"
		return
	}
	valueType, err = getRawType(valueType)
	if err != nil {
		err = fmt.Errorf("normal 【%v】: %v, key %v valuetype %v", f, err, key, valueType)
		return
	}
	if len(defaultStr) > 0 {
		defaultVal, err = defaultConvert(defaultStr, valueType)
	}
	return
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
		err = errors.New("arrary type must specify data type surrounded by ( )")
		return
	case "m", "map":
		schemaType = pipeline.PandoraTypeMap
	case "b", "bool", "boolean":
		schemaType = pipeline.PandoraTypeBool
	case "j", "json", "jsonstring":
		schemaType = pipeline.PandoraTypeJsonString
	case "": //这个是一种缺省
	default:
		err = fmt.Errorf("schema type %v not supperted", schemaType)
		return
	}
	return
}

func ParseDsl(dsl string, depth int) (schemas []DslSchemaEntry, err error) {
	if depth > 5 {
		err = fmt.Errorf("DslSchemaEntry are nested out of limit %v", 5)
		return
	}
	schemas = make([]DslSchemaEntry, 0)
	dsl = strings.TrimSpace(dsl)
	start := 0
	nestbalance := 0
	neststart, nestend := -1, -1
	dsl += "," //增加一个','保证一定是以","为终结
	dupcheck := make(map[string]struct{})
	for end, c := range dsl {
		if start > end {
			err = errors.New("parse dsl inner error: start index is larger than end")
			return
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
					err = errors.New("parse dsl error: nest end should never less or equal than nest start")
					return
				}
				subschemas, err := ParseDsl(dsl[neststart+1:nestend], depth+1)
				if err != nil {
					return nil, err
				}
				if neststart <= start {
					return nil, errors.New("parse dsl error: map{} not specified")
				}
				key, valueType, _, defaultVal, err := getField(dsl[start:neststart])
				if err != nil {
					return nil, err
				}
				if key != "" {
					if _, ok := dupcheck[key]; ok {
						return nil, errors.New("parse dsl error: " + key + " is duplicated key")
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
						return nil, err
					}
					if key != "" {
						if _, ok := dupcheck[key]; ok {
							return nil, errors.New("parse dsl error: " + key + " is duplicated key")
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
		err = errors.New("parse dsl error: { and } not match")
		return
	}
	return
}

func defaultConvert(defaultStr, valueType string) (converted interface{}, err error) {
	defaultStr = strings.TrimSpace(defaultStr)
	switch valueType {
	case pipeline.PandoraTypeLong:
		if converted, err = strconv.ParseInt(defaultStr, 10, 64); err == nil {
			return
		}
		var floatc float64
		if floatc, err = strconv.ParseFloat(defaultStr, 10); err == nil {
			converted = int64(floatc)
			return
		}
		return
	case pipeline.PandoraTypeFloat:
		return strconv.ParseFloat(defaultStr, 10)
	case pipeline.PandoraTypeString, pipeline.PandoraTypeJsonString, pipeline.PandoraTypeDate:
		return defaultStr, nil
	case pipeline.PandoraTypeBool:
		return strconv.ParseBool(defaultStr)
	default:
		err = fmt.Errorf("not support default value for type(%v)", valueType)
	}
	return
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
				return nil, nil
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
				return nil, nil
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
		if converted, err = ConvertDate("", "", 0, data); err == nil {
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
