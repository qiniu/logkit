package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

func (c *Pipeline) getSchemas(repoName string) (schemas map[string]RepoSchemaEntry, err error) {
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: repoName,
	})
	if err != nil {
		return
	}
	schemas = make(map[string]RepoSchemaEntry)
	for _, v := range repo.Schema {
		schemas[v.Key] = v
	}
	return
}

// true则删掉,删掉表示后续不增加字段
func deepDeleteCheck(data interface{}, schema RepoSchemaEntry) bool {
	if schema.ValueType != PandoraTypeMap {
		return true
	}
	mval, ok := data.(map[string]interface{})
	if !ok {
		return true
	}
	if len(mval) > len(schema.Schema) {
		return false
	}
	for k, v := range mval {
		notfind := true
		for _, sv := range schema.Schema {
			if sv.Key == k {
				notfind = false
				if sv.ValueType == PandoraTypeMap && !deepDeleteCheck(v, sv) {
					return false
				}
			}
		}
		if notfind {
			return false
		}
	}
	return true
}

func DataConvert(data interface{}, schema RepoSchemaEntry) (converted interface{}, err error) {
	return dataConvert(data, schema)
}

func dataConvert(data interface{}, schema RepoSchemaEntry) (converted interface{}, err error) {
	switch schema.ValueType {
	case PandoraTypeLong:
		value := reflect.ValueOf(data)
		switch value.Kind() {
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return data, nil
		case reflect.Float32, reflect.Float64:
			return int64(value.Float()), nil
		case reflect.String:
			if converted, err = strconv.ParseInt(value.String(), 10, 64); err == nil {
				return
			}
			var floatc float64
			if floatc, err = strconv.ParseFloat(value.String(), 10); err == nil {
				converted = int64(floatc)
				return
			}
		}
	case PandoraTypeFloat:
		value := reflect.ValueOf(data)
		switch value.Kind() {
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return data, nil
		case reflect.Float32, reflect.Float64:
			return data, nil
		case reflect.String:
			if converted, err = strconv.ParseFloat(value.String(), 10); err == nil {
				return
			}
		}
	case PandoraTypeString:
		value := reflect.ValueOf(data)
		switch value.Kind() {
		case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			return strconv.FormatInt(value.Int(), 10), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return strconv.FormatUint(value.Uint(), 10), nil
		case reflect.Float32, reflect.Float64:
			return strconv.FormatFloat(value.Float(), 'f', -1, 64), nil
		default:
			return data, nil
		}
	case PandoraTypeJsonString:
		return data, nil
	case PandoraTypeDate:
		return data, nil
	case PandoraTypeBool:
		return data, nil
	case PandoraTypeArray:
		ret := make([]interface{}, 0)
		switch value := data.(type) {
		case []interface{}:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []string:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []int:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []int64:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []json.Number:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []float64:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []bool:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []float32:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []int8:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []int16:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []int32:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []uint:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []uint8:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []uint16:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []uint32:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case []uint64:
			for _, j := range value {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		case string:
			newdata := make([]interface{}, 0)
			err = json.Unmarshal([]byte(value), &newdata)
			if err != nil {
				return
			}
			for _, j := range newdata {
				vi, err := dataConvert(j, RepoSchemaEntry{ValueType: schema.ElemType})
				if err != nil {
					log.Error(err)
					continue
				}
				ret = append(ret, vi)
			}
		}
		return ret, nil
	case PandoraTypeMap:
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
	return data, fmt.Errorf("can not convert data[%v] to type(%v), err %v", data, reflect.TypeOf(data), err)
}

func mapDataConvert(mpvalue map[string]interface{}, schemas []RepoSchemaEntry) (converted interface{}) {
	for _, v := range schemas {
		if subv, ok := mpvalue[v.Key]; ok {
			subconverted, err := dataConvert(subv, v)
			if err != nil {
				log.Error(err)
				continue
			}
			mpvalue[v.Key] = subconverted
		}
	}
	return mpvalue
}

func copyData(d Data) Data {
	md := make(Data, len(d))
	for k, v := range d {
		if v == nil {
			continue
		}
		md[k] = v
	}
	return md
}

func haveNewData(data Data) bool {
	for _, v := range data {
		if v != nil {
			return true
		}
	}
	return false
}

func checkIgnore(value interface{}, schemeType string) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	if (rv.Kind() == reflect.Map || rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface || rv.Kind() == reflect.Slice) && rv.IsNil() {
		return true
	}
	str, ok := value.(string)
	if !ok || str != "" {
		return false
	}
	switch schemeType {
	case PandoraTypeJsonString, PandoraTypeArray, PandoraTypeMap, PandoraTypeLong, PandoraTypeFloat:
		if str == "" {
			return true
		}
	}
	return false
}

func (c *Pipeline) generatePoint(repoName string, oldData Data, schemaFree bool, option *SchemaFreeOption, repooptions *RepoOptions) (point Point, err error) {

	data := copyData(oldData)
	point = Point{}
	c.repoSchemaMux.Lock()
	schemas := c.repoSchemas[repoName]
	c.repoSchemaMux.Unlock()
	if schemas == nil {
		schemas, err = c.getSchemas(repoName)
		if err != nil {
			reqe, ok := err.(*reqerr.RequestError)
			if ok && reqe.ErrorType != reqerr.NoSuchRepoError {
				return
			}
		}
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[repoName] = schemas
	c.repoSchemaMux.Unlock()
	for name, v := range schemas {
		value, ok := data[name]
		if !ok {
			//不存在，但是必填，需要加上默认值
			if v.Required {
				value = getDefault(v)
			} else {
				continue
			}
		}

		if option != nil && option.ForceDataConvert {
			nvalue, err := dataConvert(value, v)
			if err != nil {
				log.Errorf("convert value %v to schema %v error %v, ignore this value...", value, v, err)
				continue
			}
			value = nvalue
		}

		//对于没有autoupdate的情况就不delete了，节省CPU
		if schemaFree {
			if deepDeleteCheck(value, v) {
				delete(data, name)
			} else {
				//对于schemaFree，且检测发现有字段增加的data，continue掉，以免重复加入
				continue
			}
		}

		//忽略一些不合法的空值
		if !v.Required && checkIgnore(value, v.ValueType) {
			continue
		}

		//加入point，要么已经delete，要么不schemaFree直接加入
		point.Fields = append(point.Fields, PointField{
			Key:   name,
			Value: value,
		})
	}
	/*
		data中剩余的值，但是在schema中不存在的，根据schemaFree增加。
	*/
	if schemaFree && haveNewData(data) {
		//defaultAll 为false时，过滤一批不要的
		valueType := getPandoraKeyValueType(data)
		if err = c.addRepoSchemas(repoName, valueType, option, repooptions); err != nil {
			err = fmt.Errorf("schemafree add Repo schema error %v", err)
			return
		}
		for name, v := range data {
			point.Fields = append(point.Fields, PointField{
				Key:   name,
				Value: v,
			})
		}
	}
	return
}

type Schemas []RepoSchemaEntry

func (s Schemas) Len() int {
	return len(s)
}
func (s Schemas) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}
func (s Schemas) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func mergePandoraSchemas(a, b []RepoSchemaEntry) (ret []RepoSchemaEntry, err error) {
	ret = make([]RepoSchemaEntry, 0)
	if a == nil && b == nil {
		return
	}
	if a == nil {
		ret = b
		return
	}
	if b == nil {
		ret = a
		return
	}
	sa := Schemas(a)
	sb := Schemas(b)
	sort.Sort(sa)
	sort.Sort(sb)
	i, j := 0, 0
	for {
		if i >= len(sa) {
			break
		}
		if j >= len(sb) {
			break
		}
		if sa[i].Key < sb[j].Key {
			ret = append(ret, sa[i])
			i++
			continue
		}
		if sa[i].Key > sb[j].Key {
			ret = append(ret, sb[j])
			j++
			continue
		}
		if sa[i].ValueType != sb[j].ValueType {
			err = fmt.Errorf("type conflict: key %v old type is <%v> want change to type <%v>", sa[i].Key, sa[i].ValueType, sb[j].ValueType)
			return
		}
		if sa[i].ValueType == PandoraTypeMap {
			if sa[i].Schema, err = mergePandoraSchemas(sa[i].Schema, sb[j].Schema); err != nil {
				return
			}
		}
		ret = append(ret, sa[i])
		i++
		j++
	}
	for ; i < len(sa); i++ {
		ret = append(ret, sa[i])
	}
	for ; j < len(sb); j++ {
		ret = append(ret, sb[j])
	}
	return
}

func (c *Pipeline) addRepoSchemas(repoName string, addSchemas map[string]RepoSchemaEntry, option *SchemaFreeOption, repooptions *RepoOptions) (err error) {

	var addScs, oldScs []RepoSchemaEntry
	for _, v := range addSchemas {
		addScs = append(addScs, v)
	}
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: repoName,
	})
	if err != nil {
		reqe, ok := err.(*reqerr.RequestError)
		if ok && reqe.ErrorType != reqerr.NoSuchRepoError {
			return
		}
	} else {
		oldScs = repo.Schema
	}
	schemas, err := mergePandoraSchemas(oldScs, addScs)
	if err != nil {
		return
	}
	if oldScs == nil {
		if err = c.CreateRepo(&CreateRepoInput{
			RepoName: repoName,
			Schema:   schemas,
			Options:  repooptions,
		}); err != nil {
			return
		}
		if option != nil && option.ToLogDB {
			err = c.AutoExportToLogDB(&AutoExportToLogDBInput{
				RepoName:    repoName,
				LogRepoName: option.LogDBRepoName,
				Retention:   option.LogDBRetention,
			})
			if err != nil {
				return
			}
		}
		if option != nil && option.ToKODO {
			err = c.AutoExportToKODO(&AutoExportToKODOInput{
				RepoName:   repoName,
				BucketName: option.KodoBucketName,
				Retention:  option.KodoRetention,
				Email:      option.KodoEmail,
			})
			if err != nil {
				return
			}
		}
		if option != nil && option.ToTSDB {
			err = c.AutoExportToTSDB(&AutoExportToTSDBInput{
				RepoName:     repoName,
				TSDBRepoName: option.TSDBRepoName,
				SeriesName:   option.TSDBSeriesName,
				Retention:    option.TSDBRetention,
				Tags:         option.TSDBtags,
			})
			if err != nil {
				return
			}
		}
		return
	} else {
		err = c.UpdateRepo(&UpdateRepoInput{
			RepoName:    repoName,
			Schema:      schemas,
			Option:      option,
			RepoOptions: repooptions,
		})
	}
	if err != nil {
		return
	}
	mpschemas := RepoSchema{}
	for _, sc := range schemas {
		mpschemas[sc.Key] = sc
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[repoName] = mpschemas
	c.repoSchemaMux.Unlock()
	return
}

/* Pandora类型的支持程度
PandoraTypeLong   ：全部支持
PandoraTypeFloat  ：全部支持
PandoraTypeString ：全部支持
PandoraTypeDate   ：支持rfc3339的string转化
PandoraTypeBool   ：全部支持
PandoraTypeArray  ：全部支持
PandoraTypeMap    ：全部支持
*/
func getPandoraKeyValueType(data Data) (valueType map[string]RepoSchemaEntry) {
	valueType = make(map[string]RepoSchemaEntry)
	for k, v := range data {
		switch nv := v.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			valueType[k] = formValueType(k, PandoraTypeLong)
		case float32, float64:
			valueType[k] = formValueType(k, PandoraTypeFloat)
		case bool:
			valueType[k] = formValueType(k, PandoraTypeBool)
		case json.Number:
			_, err := nv.Int64()
			if err == nil {
				valueType[k] = formValueType(k, PandoraTypeLong)
			} else {
				valueType[k] = formValueType(k, PandoraTypeFloat)
			}
		case map[string]interface{}:
			sc := formValueType(k, PandoraTypeMap)
			follows := getPandoraKeyValueType(Data(nv))
			for _, m := range follows {
				sc.Schema = append(sc.Schema, m)
			}
			valueType[k] = sc
		case []interface{}:
			sc := formValueType(k, PandoraTypeArray)
			if len(nv) > 0 {
				switch nnv := nv[0].(type) {
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
					sc.ElemType = PandoraTypeLong
				case float32, float64:
					sc.ElemType = PandoraTypeFloat
				case bool:
					sc.ElemType = PandoraTypeBool
				case json.Number:
					_, err := nnv.Int64()
					if err == nil {
						sc.ElemType = PandoraTypeLong
					} else {
						sc.ElemType = PandoraTypeFloat
					}
				case nil: // 不处理，不加入
				case string:
					sc.ElemType = PandoraTypeString
				default:
					sc.ElemType = PandoraTypeString
				}
				valueType[k] = sc
			}
			//对于里面没有元素的interface，不添加进去，因为无法判断类型
		case []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeLong
			valueType[k] = sc
		case []float32, []float64:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeFloat
			valueType[k] = sc
		case []bool:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeBool
			valueType[k] = sc
		case []string:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeBool
			valueType[k] = sc
		case []json.Number:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeFloat
			valueType[k] = sc
		case nil: // 不处理，不加入
		case string:
			_, err := time.Parse(time.RFC3339, nv)
			if err == nil {
				valueType[k] = formValueType(k, PandoraTypeDate)
			} else {
				valueType[k] = formValueType(k, PandoraTypeString)
			}
		case time.Time, *time.Time:
			valueType[k] = formValueType(k, PandoraTypeDate)
		default:
			valueType[k] = formValueType(k, PandoraTypeString)
			log.Warnf("find undetected key(%v)-type(%v), read it as string", k, reflect.TypeOf(v))
		}
	}
	return
}

func formValueType(key, vtype string) RepoSchemaEntry {
	return RepoSchemaEntry{
		Key:       key,
		ValueType: vtype,
	}
}

func getDefault(t RepoSchemaEntry) (result interface{}) {
	switch t.ValueType {
	case PandoraTypeLong:
		result = 0
	case PandoraTypeFloat:
		result = 0.0
	case PandoraTypeString:
		result = ""
	case PandoraTypeDate:
		result = time.Now().Format(time.RFC3339Nano)
	case PandoraTypeBool:
		result = false
	case PandoraTypeMap:
		result = make(map[string]interface{})
	case PandoraTypeArray:
		switch t.ElemType {
		case PandoraTypeString:
			result = make([]string, 0)
		case PandoraTypeFloat:
			result = make([]float64, 0)
		case PandoraTypeLong:
			result = make([]int64, 0)
		case PandoraTypeBool:
			result = make([]bool, 0)
		}
	}
	return
}

func (c *Pipeline) getSchemaSorted(input *UpdateRepoInput) (err error) {
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: input.RepoName,
	})
	if err != nil {
		return
	}
	mschemas := make(map[string]RepoSchemaEntry)
	for _, sc := range input.Schema {
		mschemas[sc.Key] = sc
	}
	var schemas []RepoSchemaEntry
	for _, old := range repo.Schema {
		new, ok := mschemas[old.Key]
		if ok {
			schemas = append(schemas, new)
			delete(mschemas, old.Key)
		} else {
			schemas = append(schemas, old)
		}
	}
	for _, v := range mschemas {
		schemas = append(schemas, v)
	}
	input.Schema = schemas
	return
}
