package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const (
	maxMapLevel = 5
)

func (c *Pipeline) getSchemas(repoName string, token PandoraToken) (schemas map[string]RepoSchemaEntry, err error) {
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName:     repoName,
		PandoraToken: token,
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
				break
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
		switch value := data.(type) {
		case json.Number:
			v, err := value.Int64()
			if err == nil {
				return strconv.FormatInt(v, 10), nil
			} else {
				return data, nil
			}
		case map[string]interface{}:
			str, err := json.Marshal(value)
			if err != nil {
				return "", err
			}
			return string(str), nil
		case []interface{}:
			str, err := json.Marshal(value)
			if err != nil {
				return "", err
			}
			return string(str), nil
		case nil:
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
				if err != nil {
					return "", err
				}
				return string(str), nil
			default:
				return data, nil
			}
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

func copyAndConvertData(d Data, mapLevel int) Data {
	md := make(Data, len(d))
	for k, v := range d {
		switch nv := v.(type) {
		case map[string]interface{}:
			if len(nv) == 0 {
				continue
			}
			if mapLevel >= 5 {
				str, err := json.Marshal(nv)
				if err != nil {
					log.Warnf("Nesting depth of repo schema is exceeded: maximum nesting depth %v, data %v will be ignored", maxMapLevel, nv)
				}
				v = string(str)
			} else {
				v = map[string]interface{}(copyAndConvertData(nv, mapLevel+1))
			}
		case []uint64:
			if len(nv) == 0 {
				continue
			}
			newArr := make([]int64, 0)
			for _, value := range nv {
				newArr = append(newArr, int64(value))
			}
			v = newArr
		case []interface{}:
			if len(nv) == 0 {
				continue
			}
			switch nv[0].(type) {
			case uint64:
				newArr := make([]interface{}, 0)
				for _, value := range nv {
					switch newV := value.(type) {
					case uint64:
						newArr = append(newArr, int64(newV))
					default:
						newArr = append(newArr, newV)
					}
				}
				v = newArr
			}
		case uint64:
			v = int64(nv)
		case nil:
			continue
		}
		nk := strings.Replace(k, "-", "_", -1)
		md[nk] = v
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

// 将 entry 以及 entry.Schema 中的 valueType 和 ElemType 的值由 srcType 改为 dstType
func changeElemType(entry RepoSchemaEntry, srcType, dstType string) RepoSchemaEntry {
	if entry.ValueType == srcType {
		entry.ValueType = dstType
	}
	if entry.ElemType == srcType {
		entry.ElemType = dstType
	}
	for i, s := range entry.Schema {
		entry.Schema[i] = changeElemType(s, srcType, dstType)
	}
	return entry
}

func (c *Pipeline) generatePoint(oldData Data, input *SchemaFreeInput) (point Point, repoUpdate bool, err error) {
	// copyAndConvertData 函数会将包含'-'的 key 用 '_' 来代替
	// 同时该函数会去除数据中无法判断类型的部分
	data := copyAndConvertData(oldData, 1)
	point = Point{}
	c.repoSchemaMux.Lock()
	schemas := c.repoSchemas[input.RepoName]
	c.repoSchemaMux.Unlock()
	if schemas == nil {
		if schemas, err = c.getSchemas(input.RepoName, input.PipelineGetRepoToken); err != nil {
			reqe, ok := err.(*reqerr.RequestError)
			if ok && reqe.ErrorType != reqerr.NoSuchRepoError {
				return
			} else {
				err = nil
				schemas = make(map[string]RepoSchemaEntry)
			}
		}
		c.repoSchemaMux.Lock()
		c.repoSchemas[input.RepoName] = schemas
		c.repoSchemaMux.Unlock()
	}
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

		if input.Option != nil && input.Option.ForceDataConvert {
			nvalue, err := dataConvert(value, v)
			if err != nil {
				log.Errorf("convert value %v to schema %v error %v, ignore this value...", value, v, err)
				continue
			}
			value = nvalue
		}

		//对于没有autoupdate的情况就不delete了，节省CPU
		if !input.NoUpdate {
			if deepDeleteCheck(value, v) {
				delete(data, name)
			} else {
				//对于schemaFree，且检测发现有字段增加的data，continue掉，以免重复加入
				continue
			}
		}

		//忽略一些不合法的空值
		if !v.Required && checkIgnore(value, v.ValueType) {
			delete(data, name)
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
	if !input.NoUpdate && haveNewData(data) {
		//defaultAll 为false时，过滤一批不要的
		// 该函数有两个作用，1. 获取 data 中所有字段的 schema; 2. 将 data 中值为 nil, 无法判断类型的键值对，从 data 中删掉
		valueType := getTrimedDataSchema(data)
		// 将 metric 中的 long 都改成 float
		if input.Option != nil && input.Option.IsMetric {
			for key, val := range valueType {
				valueType[key] = changeElemType(val, PandoraTypeLong, PandoraTypeFloat)
			}
		}
		if repoUpdate, err = c.addRepoSchemas(valueType, input.RepoName); err != nil {
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

func mergePandoraSchemas(oldScs, addScs []RepoSchemaEntry) (ret []RepoSchemaEntry, needUpdate bool, err error) {
	ret = make([]RepoSchemaEntry, 0)
	if oldScs == nil && addScs == nil {
		return
	}

	if addScs == nil || len(addScs) == 0 {
		ret = oldScs
		return
	}

	if oldScs == nil {
		ret = addScs
		needUpdate = true
		return
	}

	sOldScs := Schemas(oldScs)
	sAddScs := Schemas(addScs)
	sort.Sort(sOldScs)
	sort.Sort(sAddScs)
	i, j := 0, 0
	for {
		if i >= len(sOldScs) {
			break
		}
		if j >= len(sAddScs) {
			break
		}
		if sOldScs[i].Key < sAddScs[j].Key {
			ret = append(ret, sOldScs[i])
			i++
			continue
		}
		if sOldScs[i].Key > sAddScs[j].Key {
			ret = append(ret, sAddScs[j])
			j++
			needUpdate = true
			continue
		}
		if sOldScs[i].ValueType != sAddScs[j].ValueType {
			err = fmt.Errorf("type conflict: key %v old type is <%v> want change to type <%v>", sOldScs[i].Key, sOldScs[i].ValueType, sAddScs[j].ValueType)
			return
		}
		if sOldScs[i].ValueType == PandoraTypeMap {
			var update bool
			if sOldScs[i].Schema, update, err = mergePandoraSchemas(sOldScs[i].Schema, sAddScs[j].Schema); err != nil {
				return
			}
			if update {
				needUpdate = true
			}
		}
		ret = append(ret, sOldScs[i])
		i++
		j++
	}
	for ; i < len(sOldScs); i++ {
		ret = append(ret, sOldScs[i])
	}
	for ; j < len(sAddScs); j++ {
		needUpdate = true
		ret = append(ret, sAddScs[j])
	}
	return
}

type WorkflowTokens struct {
	StartWorkflowToken     PandoraToken
	StopWorkflowToken      PandoraToken
	GetWorkflowStatusToken PandoraToken
}

func (c *Pipeline) changeWorkflowToStopped(workflow *GetWorkflowOutput, waitStopped bool, tokens WorkflowTokens, isNeedStart *bool) error {
	logger := base.NewDefaultLogger()
	switch workflow.Status {
	case base.WorkflowStarted:
		*isNeedStart = true
		if err := c.StopWorkflow(&StopWorkflowInput{WorkflowName: workflow.Name, PandoraToken: tokens.StopWorkflowToken}); err != nil {
			return err
		}
	case base.WorkflowStarting:
		if err := WaitWorkflowStarted(workflow.Name, c, logger, tokens.GetWorkflowStatusToken); err != nil {
			return err
		}
		*isNeedStart = true
		if err := c.StopWorkflow(&StopWorkflowInput{WorkflowName: workflow.Name, PandoraToken: tokens.StopWorkflowToken}); err != nil {
			return err
		}
	case base.WorkflowStopping:
		// stopping 不做特殊处理，直接等待停止
	default:
		// 当处于 ready, stopped 时可以直接返回
		workflow.Status = base.WorkflowStopped
		return nil
	}
	if waitStopped {
		if err := WaitWorkflowStopped(workflow.Name, c, logger, tokens.GetWorkflowStatusToken); err != nil {
			return err
		}
	}
	workflow.Status = base.WorkflowStopped
	return nil
}

func (c *Pipeline) changeWorkflowToStarted(workflow *GetWorkflowOutput, waitStarted bool, tokens WorkflowTokens) error {
	logger := base.NewDefaultLogger()
	switch workflow.Status {
	case base.WorkflowReady, base.WorkflowStopped:
		if err := c.StartWorkflow(&StartWorkflowInput{WorkflowName: workflow.Name, PandoraToken: tokens.StartWorkflowToken}); err != nil {
			return err
		}
	case base.WorkflowStopping:
		if err := WaitWorkflowStopped(workflow.Name, c, logger, tokens.GetWorkflowStatusToken); err != nil {
			return err
		}
		if err := c.StartWorkflow(&StartWorkflowInput{WorkflowName: workflow.Name, PandoraToken: tokens.StartWorkflowToken}); err != nil {
			return err
		}
	case base.WorkflowStarting:
		// 处于 starting 时，不做特殊处理， 直接等待 started
	default:
		// 处于 started 直接返回
		workflow.Status = base.WorkflowStarted
		return nil
	}

	if waitStarted {
		if err := WaitWorkflowStarted(workflow.Name, c, logger, tokens.GetWorkflowStatusToken); err != nil {
			return err
		}
	}
	workflow.Status = base.WorkflowStarted
	return nil
}

// 这边的逻辑判断是:
//1. 获取 workflow 有错误，并且错误是 workflow 不存在, 创建 workflow:
//	1) 如果创建 workflow 有错误，并且错误是 workflow 已经存在, 重新获取 workflow(为了拿到其状态信息)
//	2) 如果创建 workflow 有错误，并且错误不是 workflow 已经存在, 直接返回错误
//	3) 如果创建 workflow 没有错误，即创建成功，此时记录需要自动启动新建的 workflow, 并填充 workflow 的名称和当前的状态
//2. 获取 workflow 有错误，并且不是 workflow 不存在的错误, 直接返回错误
func (c *Pipeline) getOrCreateWorkflow(input *InitOrUpdateWorkflowInput, ns *bool) (workflow *GetWorkflowOutput, err error) {
	workflow, err = c.GetWorkflow(&GetWorkflowInput{
		WorkflowName: input.WorkflowName,
		PandoraToken: input.PipelineGetWorkflowToken,
	})
	if err != nil && reqerr.IsNoSuchWorkflow(err) {
		if err = c.CreateWorkflow(&CreateWorkflowInput{
			WorkflowName: input.WorkflowName,
			Region:       input.Region,
			PandoraToken: input.PipelineCreateWorkflowToken,
		}); err != nil && reqerr.IsExistError(err) {
			workflow, err = c.GetWorkflow(&GetWorkflowInput{
				WorkflowName: input.WorkflowName,
				PandoraToken: input.PipelineGetWorkflowToken,
			})
		} else if err != nil {
			return
		} else {
			*ns = true
			workflow.Name = input.WorkflowName
			workflow.Status = base.WorkflowReady
		}
	}
	return
}

//这边判断的逻辑为: create repo 然后
//1. 如果有错误, 且错误为 workflow 状态不允许创建: 停止 workflow 停止 workflow 时出现错误直接返回
//	停止 workflow 后, 重新 create repo, 并在接下来的逻辑中处理可能出现的错误
//2. 第一次 create 或者重试 create 如果有错误, 且错误为 repo 已经存在: 获取 repo, merge schema, 更新 repo
func (c *Pipeline) createOrUpdateRepo(input *InitOrUpdateWorkflowInput, workflow *GetWorkflowOutput, ns *bool) (err error) {
	err = c.CreateRepo(&CreateRepoInput{
		RepoName:     input.RepoName,
		Region:       input.Region,
		Schema:       input.Schema,
		Options:      input.RepoOptions,
		Workflow:     input.WorkflowName,
		PandoraToken: input.PipelineCreateRepoToken,
	})
	if err != nil && reqerr.IsWorkflowStatError(err) {
		// 如果当前 workflow 的状态不允许更新，则先等待停止 workflow 再更新
		if subErr := c.changeWorkflowToStopped(workflow, true, WorkflowTokens{
			StartWorkflowToken:     input.PipelineStartWorkflowToken,
			StopWorkflowToken:      input.PipelineStopWorkflowToken,
			GetWorkflowStatusToken: input.PipelineGetWorkflowStatusToken,
		}, ns); subErr != nil {
			return subErr
		}
		err = c.CreateRepo(&CreateRepoInput{
			RepoName:     input.RepoName,
			Region:       input.Region,
			Schema:       input.Schema,
			Options:      input.RepoOptions,
			Workflow:     input.WorkflowName,
			PandoraToken: input.PipelineCreateRepoToken,
		})
	}
	if err != nil && reqerr.IsExistError(err) {
		repo, subErr := c.GetRepo(&GetRepoInput{
			RepoName:     input.RepoName,
			PandoraToken: input.PipelineGetRepoToken,
		})
		if subErr != nil {
			return subErr
		}
		schemas, needUpdate, subErr := mergePandoraSchemas(repo.Schema, input.Schema)
		if subErr != nil {
			return subErr
		}
		if needUpdate {
			if err = c.updateRepo(&UpdateRepoInput{
				RepoName:             input.RepoName,
				Schema:               schemas,
				Option:               input.Option,
				workflow:             repo.Workflow,
				RepoOptions:          input.RepoOptions,
				PandoraToken:         input.PipelineUpdateRepoToken,
				PipelineGetRepoToken: input.PipelineGetRepoToken,
			}); err != nil {
				return err
			}
		}
	}
	return
}

// 初始化/更新 workflow, 有以下几个功能
// 1. 根据参数确保 workflow 存在(如果是打到 dag 的话)
// 2. 根据参数创建导出到 logdb, tsdb, kodo 等
// 3. 根据参数初始化消息队列, 这个初始化将包括 logkit 的 dsl 用户自动创建的字段
// 4. 确保 workflow 处于启动状态，每次发送数据前不再确认 workflow 是否是启动状态
// 注意: 处于兼容性考虑，未删除 AutoExportTo* 这些函数的 GetRepo 请求，即每个函数额外请求一次
func (c *Pipeline) InitOrUpdateWorkflow(input *InitOrUpdateWorkflowInput) error {
	if input.RepoName == "" {
		return fmt.Errorf("repo name can not be empty")
	}
	if input.Option != nil && input.Option.ToLogDB {
		input.Option.AutoExportToLogDBInput.Region = input.Region
	}
	// 获取 repo
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
	})
	// 是否由 logkit 启动 workflow
	needStartWorkflow := false
	if err != nil && reqerr.IsNoSuchResourceError(err) {
		// 如果 repo 不存在
		if len(input.Schema) == 0 {
			return nil
		}
		var workflow *GetWorkflowOutput
		if input.WorkflowName != "" {
			// 如果导出到 dag, 确保 workflow 存在, 不存在时创建
			if workflow, err = c.getOrCreateWorkflow(input, &needStartWorkflow); err != nil {
				return err
			}
		}
		// repo 不存在且传入了非空的 schema, 此时要新建 repo
		if err = c.createOrUpdateRepo(input, workflow, &needStartWorkflow); err != nil {
			return err
		}
		// 创建、更新各种导出
		if input.Option != nil && input.Option.ToKODO {
			if err := c.AutoExportToKODO(&input.Option.AutoExportToKODOInput); err != nil {
				log.Error("AutoExportToKODO error", err)
				return err
			}
		}
		if input.Option != nil && input.Option.ToLogDB {
			if err := c.AutoExportToLogDB(&input.Option.AutoExportToLogDBInput); err != nil {
				log.Error("AutoExportToLogDB error", err)
				return err
			}
		}
		if input.Option != nil && input.Option.ToTSDB {
			if err := c.AutoExportToTSDB(&input.Option.AutoExportToTSDBInput); err != nil {
				log.Error("AutoExportToTSDB error", err)
				return err
			}
		}
		if input.WorkflowName != "" && needStartWorkflow {
			if err := c.changeWorkflowToStarted(workflow, false, WorkflowTokens{
				StartWorkflowToken:     input.PipelineStartWorkflowToken,
				StopWorkflowToken:      input.PipelineStopWorkflowToken,
				GetWorkflowStatusToken: input.PipelineGetWorkflowStatusToken,
			}); err != nil {
				if reqerr.IsWorkflowNoExecutableJob(err) {
					return nil
				}
				return err
			}
		}
	} else if err != nil {
		return err
	} else {
		// 如果 repo 存在, 则要对比下 repo 现有的 schema，是否已经包含参数中的 schema，即是否需要更新 repo
		schemas, needUpdate, err := mergePandoraSchemas(repo.Schema, input.Schema)
		if err != nil {
			return err
		}
		// 当 input.Init = true 时为 sender 创立，可能需要创建导出
		if (needUpdate && input.SchemaFree) || (len(schemas) > 0 && input.InitOptionChange) {
			updateRepoInput := &UpdateRepoInput{
				RepoName:             input.RepoName,
				Schema:               schemas,
				Option:               input.Option,
				workflow:             repo.Workflow,
				RepoOptions:          input.RepoOptions,
				PandoraToken:         input.PipelineUpdateRepoToken,
				PipelineGetRepoToken: input.PipelineGetRepoToken,
			}
			if err := c.UpdateRepo(updateRepoInput); err != nil {
				if reqerr.IsWorkflowStatError(err) {
					// 如果当前 workflow 的状态不允许更新，则先等待停止 workflow 再更新
					workflow, subErr := c.GetWorkflow(&GetWorkflowInput{
						WorkflowName: updateRepoInput.workflow,
						PandoraToken: input.PipelineGetWorkflowToken,
					})
					if subErr != nil {
						return subErr
					}
					if subErr := c.changeWorkflowToStopped(workflow, true, WorkflowTokens{
						StartWorkflowToken:     input.PipelineStartWorkflowToken,
						StopWorkflowToken:      input.PipelineStopWorkflowToken,
						GetWorkflowStatusToken: input.PipelineGetWorkflowStatusToken,
					}, &needStartWorkflow); subErr != nil {
						return subErr
					}
					if subErr = c.UpdateRepo(updateRepoInput); subErr != nil {
						return subErr
					}
				} else if err != nil {
					return err
				}
			}
			// 此处的更新是为了调用方可以拿到最新的 schema
			input.Schema = schemas
		}
		// 如果 repo 已经存在, repo 本身的 fromDag 字段就表明了是否来自workflow
		if repo.FromDag && needStartWorkflow {
			workflow, err := c.GetWorkflow(&GetWorkflowInput{
				WorkflowName: repo.Workflow,
				PandoraToken: input.PipelineGetWorkflowToken,
			})
			if err != nil {
				return err
			}
			if err := c.changeWorkflowToStarted(workflow, false, WorkflowTokens{
				StartWorkflowToken:     input.PipelineStartWorkflowToken,
				StopWorkflowToken:      input.PipelineStopWorkflowToken,
				GetWorkflowStatusToken: input.PipelineGetWorkflowStatusToken,
			}); err != nil {
				if reqerr.IsWorkflowNoExecutableJob(err) {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (c *Pipeline) addRepoSchemas(addSchemas map[string]RepoSchemaEntry, repoName string) (repoUpdate bool, err error) {
	if len(addSchemas) == 0 {
		return
	}
	var oldScs, addScs, mergeScs []RepoSchemaEntry
	for _, v := range addSchemas {
		addScs = append(addScs, v)
	}
	c.repoSchemaMux.Lock()
	for _, v := range c.repoSchemas[repoName] {
		oldScs = append(oldScs, v)
	}
	c.repoSchemaMux.Unlock()
	if mergeScs, repoUpdate, err = mergePandoraSchemas(oldScs, addScs); err != nil {
		return
	}
	mpschemas := RepoSchema{}
	for _, sc := range mergeScs {
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
// 该函数有两个作用，1. 获取 data 中所有字段的 schema; 2. 将 data 中值为 nil, 无法判断类型的键值对，从 data 中删掉
func getTrimedDataSchema(data Data) (valueType map[string]RepoSchemaEntry) {
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
			follows := getTrimedDataSchema(Data(nv))
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
				case nil:
					// 由于数据为空，且无法判断类型, 所以从数据中将该条键值对删掉
					if len(nv) == 1 {
						delete(data, k)
					} else {
						sc.ElemType = PandoraTypeString
					}
				case string:
					sc.ElemType = PandoraTypeString
				default:
					sc.ElemType = PandoraTypeString
				}
				valueType[k] = sc
			} else {
				// 由于数据为空，且无法判断类型, 所以从数据中将该条键值对删掉
				delete(data, k)
			}
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
			sc.ElemType = PandoraTypeString
			valueType[k] = sc
		case []json.Number:
			sc := formValueType(k, PandoraTypeArray)
			sc.ElemType = PandoraTypeFloat
			valueType[k] = sc
		case nil:
			// 由于数据为空，且无法判断类型, 所以从数据中将该条键值对删掉
			delete(data, k)
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
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
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
