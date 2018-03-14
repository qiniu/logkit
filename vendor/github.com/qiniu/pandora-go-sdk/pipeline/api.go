package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"reflect"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/base/request"
	"github.com/qiniu/pandora-go-sdk/logdb"
	"github.com/qiniu/pandora-go-sdk/tsdb"
)

const (
	systemVariableType = "system"
	userVariableType   = "user"
)

func (c *Pipeline) CreateGroup(input *CreateGroupInput) (err error) {
	op := c.NewOperation(base.OpCreateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateGroup(input *UpdateGroupInput) (err error) {
	op := c.NewOperation(base.OpUpdateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) StartGroupTask(input *StartGroupTaskInput) (err error) {
	op := c.NewOperation(base.OpStartGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StopGroupTask(input *StopGroupTaskInput) (err error) {
	op := c.NewOperation(base.OpStopGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) ListGroups(input *ListGroupsInput) (output *ListGroupsOutput, err error) {
	op := c.NewOperation(base.OpListGroups)

	output = &ListGroupsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetGroup(input *GetGroupInput) (output *GetGroupOutput, err error) {
	op := c.NewOperation(base.OpGetGroup, input.GroupName)

	output = &GetGroupOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteGroup(input *DeleteGroupInput) (err error) {
	op := c.NewOperation(base.OpDeleteGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.NewOperation(base.OpCreateRepo, input.RepoName)
	req := c.newRequest(op, input.Token, nil)
	if input.Region == "" {
		input.Region = c.defaultRegion
	}
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) CreateRepoFromDSL(input *CreateRepoDSLInput) (err error) {
	schemas, err := toSchema(input.DSL, 0)
	if err != nil {
		return
	}
	return c.CreateRepo(&CreateRepoInput{
		PandoraToken: input.PandoraToken,
		RepoName:     input.RepoName,
		Region:       input.Region,
		GroupName:    input.GroupName,
		Schema:       schemas,
		Options:      input.Options,
		Workflow:     input.Workflow,
	})
}

func (c *Pipeline) UpdateRepoWithTSDB(input *UpdateRepoInput, ex ExportDesc) error {
	repoName, ok := ex.Spec["destRepoName"].(string)
	if !ok {
		return fmt.Errorf("export tsdb spec destRepoName assert error %v is not string", ex.Spec["destRepoName"])
	}
	seriesName, ok := ex.Spec["series"].(string)
	if !ok {
		return fmt.Errorf("export tsdb spec series assert error %v is not string", ex.Spec["series"])
	}
	tagsi := ex.Spec["tags"]
	tags, ok := tagsi.(map[string]interface{})
	if !ok && tagsi != nil {
		var typeis string
		if tagsi != nil {
			typeis = reflect.TypeOf(tagsi).String()
		} else {
			typeis = "null"
		}
		return fmt.Errorf("export tsdb spec tags assert error %v is not map[string]interface{}, but %v", ex.Spec["tags"], typeis)
	}
	newtags := make(map[string]string)
	for k, v := range tags {
		nk, ok := v.(string)
		if ok {
			newtags[k] = nk
		} else {
			newtags[k] = "#" + k
		}
	}

	fields, ok := ex.Spec["fields"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export tsdb spec fields assert error %v is not map[string]interface{}", ex.Spec["fields"])
	}
	newfields := make(map[string]string)
	for k, v := range fields {
		nk, ok := v.(string)
		if ok {
			newfields[k] = nk
		} else {
			newfields[k] = "#" + k
		}
	}

	for _, v := range input.Schema {
		if input.IsTag(v.Key) {
			newtags[v.Key] = "#" + v.Key
		} else {
			newfields[v.Key] = "#" + v.Key
		}
	}
	spec := &ExportTsdbSpec{DestRepoName: repoName, SeriesName: seriesName, Tags: newtags, Fields: newfields}

	seriesUpdateExportToken, ok := input.Option.AutoExportTSDBTokens.UpdateExportToken[ex.Name]
	if !ok {
		seriesUpdateExportToken = models.PandoraToken{}
	}

	err := c.UpdateExport(&UpdateExportInput{
		RepoName:     input.RepoName,
		ExportName:   ex.Name,
		Spec:         spec,
		PandoraToken: seriesUpdateExportToken,
	})
	if reqerr.IsExportRemainUnchanged(err) {
		err = nil
	}
	return err
}

func schemaNotIn(key string, schemas []logdb.RepoSchemaEntry) bool {
	for _, v := range schemas {
		if v.Key == key {
			return false
		}
	}
	return true
}

func (c *Pipeline) UpdateRepoWithLogDB(input *UpdateRepoInput, ex ExportDesc) error {
	repoName, ok := ex.Spec["destRepoName"].(string)
	if !ok {
		return fmt.Errorf("export logdb spec destRepoName assert error %v is not string", ex.Spec["destRepoName"])
	}
	docs, ok := ex.Spec["doc"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export logdb spec doc assert error %v is not map[string]interface{}", ex.Spec["doc"])
	}
	for _, v := range input.Schema {
		docs[v.Key] = "#" + v.Key
	}
	logdbAPI, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	repoInfo, err := logdbAPI.GetRepo(&logdb.GetRepoInput{
		RepoName:     repoName,
		PandoraToken: input.Option.AutoExportLogDBTokens.GetLogDBRepoToken,
	})
	if err != nil {
		return err
	}
	analyzers := AnalyzerInfo{}
	if input.Option != nil {
		analyzers = input.Option.AutoExportToLogDBInput.AnalyzerInfo
	}
	for _, v := range input.Schema {
		if schemaNotIn(v.Key, repoInfo.Schema) {
			scs := convertSchema2LogDB([]RepoSchemaEntry{v}, analyzers)
			if len(scs) > 0 {
				repoInfo.Schema = append(repoInfo.Schema, scs[0])
			}
			docs[v.Key] = "#" + v.Key
		}
	}
	if err = logdbAPI.UpdateRepo(&logdb.UpdateRepoInput{
		RepoName:     repoName,
		Retention:    repoInfo.Retention,
		Schema:       repoInfo.Schema,
		PandoraToken: input.Option.AutoExportLogDBTokens.UpdateLogDBRepoToken,
	}); err != nil {
		return err
	}
	spec := &ExportLogDBSpec{DestRepoName: repoName, Doc: docs}
	err = c.UpdateExport(&UpdateExportInput{
		RepoName:     input.RepoName,
		ExportName:   ex.Name,
		Spec:         spec,
		PandoraToken: input.Option.AutoExportLogDBTokens.UpdateExportToken,
	})
	if reqerr.IsExportRemainUnchanged(err) {
		err = nil
	}
	return err
}

func (c *Pipeline) UpdateRepoWithKodo(input *UpdateRepoInput, ex ExportDesc) error {
	bucketName, ok := ex.Spec["bucket"].(string)
	if !ok {
		return fmt.Errorf("export kodo spec bucketName assert error %v is not string", ex.Spec["bucket"])
	}
	fields, ok := ex.Spec["fields"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export kodo spec fields assert error %v is not map[string]interface{}", ex.Spec["fields"])
	}
	ak, ok := ex.Spec["accessKey"].(string)
	if !ok {
		return fmt.Errorf("export kodo spec accessKey assert error %v is not string", ex.Spec["accessKey"])
	}
	retention, ok := ex.Spec["retention"].(float64)
	if !ok {
		var typeis string
		if ex.Spec["retention"] != nil {
			typeis = reflect.TypeOf(ex.Spec["retention"]).Name()
		} else {
			typeis = "null"
		}
		return fmt.Errorf("export kodo spec retention assert error %v is not int, but %V", ex.Spec["retention"], typeis)
	}
	compress, ok := ex.Spec["compress"].(bool)
	if !ok {
		return fmt.Errorf("export kodo spec compress assert error %v is not bool", ex.Spec["compress"])
	}
	email, ok := ex.Spec["email"].(string)
	if !ok {
		return fmt.Errorf("export kodo spec email assert error %v is not string", ex.Spec["email"])
	}
	format, ok := ex.Spec["format"].(string)
	if !ok {
		return fmt.Errorf("export kodo spec format assert error %v is not string", ex.Spec["format"])
	}
	keyPrefix, ok := ex.Spec["keyPrefix"].(string)
	if !ok {
		return fmt.Errorf("export kodo spec keyPrefix assert error %v is not string", ex.Spec["keyPrefix"])
	}

	newfields := make(map[string]string)
	for k, v := range fields {
		nk, ok := v.(string)
		if ok {
			newfields[k] = nk
		} else {
			newfields[k] = "#" + k
		}
	}

	for _, v := range input.Schema {
		newfields[v.Key] = "#" + v.Key
	}

	spec := &ExportKodoSpec{
		Bucket:    bucketName,
		Fields:    newfields,
		AccessKey: ak,
		Retention: int(retention),
		Compress:  compress,
		Email:     email,
		Format:    format,
		KeyPrefix: keyPrefix,
	}
	err := c.UpdateExport(&UpdateExportInput{
		RepoName:     input.RepoName,
		ExportName:   ex.Name,
		Spec:         spec,
		PandoraToken: input.Option.AutoExportToKODOInput.UpdateExportToken,
	})
	if reqerr.IsExportRemainUnchanged(err) {
		err = nil
	}
	return err
}

func (c *Pipeline) UpdateRepo(input *UpdateRepoInput) (err error) {
	err = c.getSchemaSorted(input)
	if err != nil {
		return
	}
	if err = c.updateRepo(input); err != nil {
		return err
	}
	if input.Option == nil {
		return nil
	}
	option := input.Option
	//这边是一个优化，对于没有任何服务的情况，节省 listexports的rpc调用
	if !option.ToLogDB && !option.ToTSDB && !option.ToKODO {
		return nil
	}
	var listExportToken models.PandoraToken
	if option.ToLogDB {
		listExportToken = option.AutoExportLogDBTokens.ListExportToken
	}
	if option.ToTSDB {
		listExportToken = option.AutoExportTSDBTokens.ListExportToken
	}
	if option.ToKODO {
		listExportToken = option.AutoExportKodoTokens.ListExportToken
	}
	exports, err := c.ListExports(&ListExportsInput{
		RepoName:     input.RepoName,
		PandoraToken: listExportToken,
	})
	if err != nil {
		return
	}
	exs := make(map[string]ExportDesc)
	for _, ex := range exports.Exports {
		exs[ex.Name] = ex
	}
	if option.ToLogDB {
		ex, ok := exs[base.FormExportName(input.RepoName, ExportTypeLogDB)]
		if ok {
			if ex.Type != ExportTypeLogDB {
				err = fmt.Errorf("export name is %v but type is %v not %v", ex.Name, ex.Type, ExportTypeLogDB)
				return
			}
			err = c.UpdateRepoWithLogDB(input, ex)
			if err != nil {
				return
			}
		} else {
			err = c.AutoExportToLogDB(&option.AutoExportToLogDBInput)
			if err != nil {
				return
			}
		}
	}
	if option.ToTSDB {
		// 对于 metric 信息的多 export，下面的 if 会恒为 false
		ex, ok := exs[base.FormExportTSDBName(input.RepoName, option.SeriesName, ExportTypeTSDB)]
		if ok {
			if ex.Type != ExportTypeTSDB {
				err = fmt.Errorf("export name is %v but type is %v not %v", ex.Name, ex.Type, ExportTypeTSDB)
				return
			}
			err = c.UpdateRepoWithTSDB(input, ex)
			if err != nil {
				return
			}
		} else {
			err = c.AutoExportToTSDB(&option.AutoExportToTSDBInput)
			if err != nil {
				return
			}
		}
	}
	if option.ToKODO {
		ex, ok := exs[base.FormExportName(input.RepoName, ExportTypeKODO)]
		if ok {
			if ex.Type != ExportTypeKODO {
				err = fmt.Errorf("export name is %v but type is %v not %v", ex.Name, ex.Type, ExportTypeKODO)
				return
			}
			err = c.UpdateRepoWithKodo(input, ex)
			if err != nil {
				log.Error("UpdateRepoWithKodo err: ", input, ex, err)
				return
			}
		} else {
			err = c.AutoExportToKODO(&option.AutoExportToKODOInput)
			if err != nil {
				return
			}
		}
	}
	return nil
}

func (c *Pipeline) updateRepo(input *UpdateRepoInput) (err error) {
	op := c.NewOperation(base.OpUpdateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.NewOperation(base.OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) GetSampleData(input *GetSampleDataInput) (output *SampleDataOutput, err error) {
	op := c.NewOperation(base.OpGetSampleData, input.RepoName, input.Count)

	output = &SampleDataOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	var op *request.Operation
	if input.WithDag {
		op = c.NewOperation(base.OpListReposWithDag)
	} else {
		op = c.NewOperation(base.OpListRepos)
	}
	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.NewOperation(base.OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) PostData(input *PostDataInput) (err error) {
	op := c.NewOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Points.Buffer())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostLargeData(input *PostDataInput, timeout time.Duration) (datafailed Points, err error) {
	deadline := time.Now().Add(timeout)
	packages := unpackPoints(input)
	for i, pContext := range packages {
		err = c.PostDataFromBytesWithDeadline(pContext.inputs, deadline)
		if err != nil {
			for j := i; j < len(packages); j++ {
				datafailed = append(datafailed, packages[j].datas...)
			}
			return
		}
	}
	return
}

type standardPointContext struct {
	datas  Points
	inputs *PostDataFromBytesInput
}

func unpackPoints(input *PostDataInput) (packages []standardPointContext) {
	packages = []standardPointContext{}
	var buf bytes.Buffer
	var start = 0
	for i, point := range input.Points {
		pointString := point.ToString()
		// 当buf中有数据，并且加入该条数据后就超过了最大的限制，则提交这个input
		if start < i && buf.Len() > 0 && buf.Len()+len(pointString) >= PandoraMaxBatchSize {
			tmpBuff := make([]byte, buf.Len())
			copy(tmpBuff, buf.Bytes())
			packages = append(packages, standardPointContext{
				datas: input.Points[start:i],
				inputs: &PostDataFromBytesInput{
					RepoName: input.RepoName,
					Buffer:   tmpBuff,
				},
			})
			buf.Reset()
			start = i
		}
		buf.WriteString(pointString)
	}
	tmpBuff := make([]byte, buf.Len())
	copy(tmpBuff, buf.Bytes())
	packages = append(packages, standardPointContext{
		datas: input.Points[start:],
		inputs: &PostDataFromBytesInput{
			RepoName: input.RepoName,
			Buffer:   tmpBuff,
		},
	})
	return
}

type pointContext struct {
	datas  []Data
	inputs *PostDataFromBytesInput
}

func (c *Pipeline) unpack(input *SchemaFreeInput) (packages []pointContext, err error) {
	packages = []pointContext{}
	var buf bytes.Buffer
	var start = 0
	repoUpdate := false
	for i, d := range input.Datas {
		point, update, err := c.generatePoint(d, input)
		if err != nil {
			return nil, err
		}
		if update {
			repoUpdate = update
		}
		pointString := point.ToString()
		// 当buf中有数据，并且加入该条数据后就超过了最大的限制，则提交这个input
		if start < i && buf.Len() > 0 && buf.Len()+len(pointString) >= PandoraMaxBatchSize {
			tmpBuff := make([]byte, buf.Len())
			copy(tmpBuff, buf.Bytes())
			packages = append(packages, pointContext{
				datas: input.Datas[start:i],
				inputs: &PostDataFromBytesInput{
					RepoName: input.RepoName,
					Buffer:   tmpBuff,
				},
			})
			buf.Reset()
			start = i
		}
		buf.WriteString(pointString)
	}
	tmpBuff := make([]byte, buf.Len())
	copy(tmpBuff, buf.Bytes())
	packages = append(packages, pointContext{
		datas: input.Datas[start:],
		inputs: &PostDataFromBytesInput{
			RepoName:     input.RepoName,
			Buffer:       tmpBuff,
			PandoraToken: input.PipelinePostDataToken,
		},
	})
	if repoUpdate {
		var schemas []RepoSchemaEntry
		c.repoSchemaMux.Lock()
		for _, v := range c.repoSchemas[input.RepoName] {
			schemas = append(schemas, v)
		}
		c.repoSchemaMux.Unlock()
		initOrUpdateInput := &InitOrUpdateWorkflowInput{
			InitOptionChange: false,
			Schema:           schemas,
			SchemaFree:       !input.NoUpdate,
			Region:           input.Region,
			RepoName:         input.RepoName,
			WorkflowName:     input.WorkflowName,
			RepoOptions:      input.RepoOptions,
			Option:           input.Option,
			SchemaFreeToken:  input.SchemaFreeToken,
		}
		if err = c.InitOrUpdateWorkflow(initOrUpdateInput); err != nil {
			return
		}
		newSchemas := RepoSchema{}
		for _, sc := range initOrUpdateInput.Schema {
			newSchemas[sc.Key] = sc
		}
		c.repoSchemaMux.Lock()
		c.repoSchemas[input.RepoName] = newSchemas
		c.repoSchemaMux.Unlock()
	}
	return
}

func convertDatas(datas Datas) []map[string]interface{} {
	cdatas := make([]map[string]interface{}, 0)
	for _, v := range datas {
		cdatas = append(cdatas, map[string]interface{}(v))
	}
	return cdatas
}

// PostDataSchemaFree 会更新schema，newSchemas不为nil时就表示更新了，error与否不影响
func (c *Pipeline) PostDataSchemaFree(input *SchemaFreeInput) (newSchemas map[string]RepoSchemaEntry, err error) {
	contexts, err := c.unpack(input)
	if err != nil {
		err = reqerr.NewSendError("Cannot send data to pandora, "+err.Error(), convertDatas(input.Datas), reqerr.TypeDefault)
		return
	}

	failDatas := Datas{}
	errType := reqerr.TypeDefault
	var lastErr error
	c.repoSchemaMux.Lock()
	newSchemas = c.repoSchemas[input.RepoName]
	c.repoSchemaMux.Unlock()
	for _, pContext := range contexts {
		err := c.PostDataFromBytes(pContext.inputs)
		if err != nil {
			reqErr, ok := err.(*reqerr.RequestError)
			if ok {
				switch reqErr.ErrorType {
				case reqerr.InvalidDataSchemaError, reqerr.EntityTooLargeError:
					errType = reqerr.TypeBinaryUnpack
				case reqerr.NoSuchRepoError:
					c.repoSchemaMux.Lock()
					c.repoSchemas[input.RepoName] = make(RepoSchema)
					c.repoSchemaMux.Unlock()
					newSchemas = make(RepoSchema)
				}
			}
			failDatas = append(failDatas, pContext.datas...)
			lastErr = err
		}
	}
	if len(failDatas) > 0 {
		err = reqerr.NewSendError("Cannot send data to pandora, "+lastErr.Error(), convertDatas(failDatas), errType)
	}
	return
}

func (c *Pipeline) PostDataFromFile(input *PostDataFromFileInput) (err error) {
	op := c.NewOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	stfile, err := file.Stat()
	if err != nil {
		return
	}
	req.SetBodyLength(stfile.Size())
	req.SetReaderBody(file)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

// PostDataFromReader 直接把 reader作为 http 的 body 发送，没有处理用户的数据变为符合 pandora 打点协议的 bytes 过程，
// 用户如果使用该接口，需要根据 pandora 打点协议将数据转换为bytes数据流，具体的转换方式见文档：
// https://qiniu.github.io/pandora-docs/#/push_data_api
func (c *Pipeline) PostDataFromReader(input *PostDataFromReaderInput) (err error) {
	op := c.NewOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetReaderBody(input.Reader)
	req.SetBodyLength(input.BodyLength)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostDataFromBytes(input *PostDataFromBytesInput) (err error) {
	op := c.NewOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Buffer)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostDataFromBytesWithDeadline(input *PostDataFromBytesInput, deadline time.Time) (err error) {
	op := c.NewOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Buffer)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)

	ctx := req.HTTPRequest.Context()
	ctx, _ = context.WithDeadline(ctx, deadline)
	req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
	return req.Send()
}

func (c *Pipeline) UploadPlugin(input *UploadPluginInput) (err error) {
	op := c.NewOperation(base.OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()
	req.SetBufferBody(input.Buffer.Bytes())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) UploadPluginFromFile(input *UploadPluginFromFileInput) (err error) {
	op := c.NewOperation(base.OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()

	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	req.SetReaderBody(file)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) ListPlugins(input *ListPluginsInput) (output *ListPluginsOutput, err error) {
	op := c.NewOperation(base.OpListPlugins)

	output = &ListPluginsOutput{}
	req := c.newRequest(op, input.Token, &output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) VerifyPlugin(input *VerifyPluginInput) (output *VerifyPluginOutput, err error) {
	op := c.NewOperation(base.OpVerifyPlugin, input.PluginName)

	output = &VerifyPluginOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetPlugin(input *GetPluginInput) (output *GetPluginOutput, err error) {
	op := c.NewOperation(base.OpGetPlugin, input.PluginName)

	output = &GetPluginOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) DeletePlugin(input *DeletePluginInput) (err error) {
	op := c.NewOperation(base.OpDeletePlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateTransform(input *CreateTransformInput) (err error) {
	op := c.NewOperation(base.OpCreateTransform, input.SrcRepoName, input.TransformName, input.DestRepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateTransform(input *UpdateTransformInput) (err error) {
	op := c.NewOperation(base.OpUpdateTransform, input.SrcRepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListTransforms(input *ListTransformsInput) (output *ListTransformsOutput, err error) {
	op := c.NewOperation(base.OpListTransforms, input.RepoName)

	output = &ListTransformsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetTransform(input *GetTransformInput) (output *GetTransformOutput, err error) {
	op := c.NewOperation(base.OpGetTransform, input.RepoName, input.TransformName)

	output = &GetTransformOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteTransform(input *DeleteTransformInput) (err error) {
	op := c.NewOperation(base.OpDeleteTransform, input.RepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateExport(input *CreateExportInput) (err error) {
	op := c.NewOperation(base.OpCreateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateExport(input *UpdateExportInput) (err error) {
	op := c.NewOperation(base.OpUpdateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListExports(input *ListExportsInput) (output *ListExportsOutput, err error) {
	op := c.NewOperation(base.OpListExports, input.RepoName)

	output = &ListExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetExport(input *GetExportInput) (output *GetExportOutput, err error) {
	op := c.NewOperation(base.OpGetExport, input.RepoName, input.ExportName)

	output = &GetExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteExport(input *DeleteExportInput) (err error) {
	op := c.NewOperation(base.OpDeleteExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateDatasource(input *CreateDatasourceInput) (err error) {
	op := c.NewOperation(base.OpCreateDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListDatasources() (output *ListDatasourcesOutput, err error) {
	op := c.NewOperation(base.OpListDatasources)

	output = &ListDatasourcesOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetDatasource(input *GetDatasourceInput) (output *GetDatasourceOutput, err error) {
	op := c.NewOperation(base.OpGetDatasource, input.DatasourceName)

	output = &GetDatasourceOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteDatasource(input *DeleteDatasourceInput) (err error) {
	op := c.NewOperation(base.OpDeleteDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateJob(input *CreateJobInput) (err error) {
	op := c.NewOperation(base.OpCreateJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListJobs(input *ListJobsInput) (output *ListJobsOutput, err error) {
	query := ""
	values := url.Values{}
	if input.SrcJobName != "" {
		values.Set("srcJob", input.SrcJobName)
	}
	if input.SrcDatasourceName != "" {
		values.Set("srcDatasource", input.SrcDatasourceName)
	}
	if len(values) != 0 {
		query = "?" + values.Encode()
	}
	op := c.NewOperation(base.OpListJobs, query)

	output = &ListJobsOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetJob(input *GetJobInput) (output *GetJobOutput, err error) {
	op := c.NewOperation(base.OpGetJob, input.JobName)

	output = &GetJobOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJob(input *DeleteJobInput) (err error) {
	op := c.NewOperation(base.OpDeleteJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StartJob(input *StartJobInput) (err error) {
	op := c.NewOperation(base.OpStartJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetJobHistory(input *GetJobHistoryInput) (output *GetJobHistoryOutput, err error) {
	op := c.NewOperation(base.OpGetJobHistory, input.JobName)

	output = &GetJobHistoryOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) StopJob(input *StopJobInput) (err error) {
	op := c.NewOperation(base.OpStopJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StopJobBatch(input *StopJobBatchInput) (output *StopJobBatchOutput, err error) {
	op := c.NewOperation(base.OpStopJobBatch)

	output = &StopJobBatchOutput{}
	req := c.newRequest(op, input.Token, output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) RerunJobBatch(input *RerunJobBatchInput) (output *RerunJobBatchOutput, err error) {
	op := c.NewOperation(base.OpRerunJobBatch)

	output = &RerunJobBatchOutput{}
	req := c.newRequest(op, input.Token, output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) CreateJobExport(input *CreateJobExportInput) (err error) {
	op := c.NewOperation(base.OpCreateJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListJobExports(input *ListJobExportsInput) (output *ListJobExportsOutput, err error) {
	op := c.NewOperation(base.OpListJobExports, input.JobName)

	output = &ListJobExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetJobExport(input *GetJobExportInput) (output *GetJobExportOutput, err error) {
	op := c.NewOperation(base.OpGetJobExport, input.JobName, input.ExportName)

	output = &GetJobExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJobExport(input *DeleteJobExportInput) (err error) {
	op := c.NewOperation(base.OpDeleteJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) RetrieveSchema(input *RetrieveSchemaInput) (output *RetrieveSchemaOutput, err error) {
	op := c.NewOperation(base.OpRetrieveSchema)

	output = &RetrieveSchemaOutput{}
	req := c.newRequest(op, input.Token, &output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return output, req.Send()
}

func (c *Pipeline) MakeToken(desc *base.TokenDesc) (string, error) {
	return base.MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}

func (c *Pipeline) GetDefault(entry RepoSchemaEntry) interface{} {
	return getDefault(entry)
}

func (c *Pipeline) GetUpdateSchemas(repoName string) (schemas map[string]RepoSchemaEntry, err error) {
	return c.GetUpdateSchemasWithInput(&GetRepoInput{RepoName: repoName})
}

func (c *Pipeline) GetUpdateSchemasWithInput(input *GetRepoInput) (schemas map[string]RepoSchemaEntry, err error) {
	repo, err := c.GetRepo(input)

	if err != nil {
		return
	}
	schemas = make(map[string]RepoSchemaEntry)
	for _, sc := range repo.Schema {
		schemas[sc.Key] = sc
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[input.RepoName] = schemas
	c.repoSchemaMux.Unlock()
	return
}

func (c *Pipeline) CreateForLogDB(input *CreateRepoForLogDBInput) error {
	pinput := formPipelineRepoInput(input.RepoName, input.Region, input.Schema)
	pinput.PandoraToken = input.PipelineCreateRepoToken
	err := c.CreateRepo(pinput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	linput := convertCreate2LogDB(input)
	logdbapi, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	linput.PandoraToken = input.CreateLogDBRepoToken
	err = logdbapi.CreateRepo(linput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	logDBSpec := c.FormLogDBSpec(input)
	exportInput := c.FormExportInput(input.RepoName, ExportTypeLogDB, logDBSpec)
	exportInput.PandoraToken = input.CreateExportToken
	return c.CreateExport(exportInput)
}

func (c *Pipeline) CreateForLogDBDSL(input *CreateRepoForLogDBDSLInput) error {
	schemas, err := toSchema(input.Schema, 0)
	if err != nil {
		return err
	}
	ci := &CreateRepoForLogDBInput{
		RepoName:              input.RepoName,
		LogRepoName:           input.LogRepoName,
		Region:                input.Region,
		Schema:                schemas,
		Retention:             input.Retention,
		AutoExportLogDBTokens: input.AutoExportLogDBTokens,
	}
	return c.CreateForLogDB(ci)
}

func (c *Pipeline) CreateForTSDB(input *CreateRepoForTSDBInput) error {
	_, err := c.GetRepo(&GetRepoInput{
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
	})
	if err != nil {
		return err
	}
	tsdbapi, err := c.GetTSDBAPI()
	if err != nil {
		return err
	}
	if input.TSDBRepoName == "" {
		input.TSDBRepoName = input.RepoName
	}
	err = tsdbapi.CreateRepo(&tsdb.CreateRepoInput{
		RepoName:     input.TSDBRepoName,
		Region:       input.Region,
		PandoraToken: input.CreateTSDBRepoToken,
	})
	if err != nil && !reqerr.IsExistError(err) {
		log.Error("create repo error", err)
		return err
	}
	if input.SeriesName == "" {
		input.SeriesName = input.RepoName
	}
	seriesToken, ok := input.CreateTSDBSeriesTokens[input.SeriesName]
	if !ok {
		seriesToken = models.PandoraToken{}
	}
	err = tsdbapi.CreateSeries(&tsdb.CreateSeriesInput{
		RepoName:     input.TSDBRepoName,
		SeriesName:   input.SeriesName,
		Retention:    input.Retention,
		PandoraToken: seriesToken,
	})
	if err != nil && !reqerr.IsExistError(err) {
		log.Error("create series error", err)
		return err
	}
	tsdbSpec := c.FormTSDBSpec(input)
	exportInput := c.FormExportInput(input.RepoName, ExportTypeTSDB, tsdbSpec)
	exportInput.ExportName = base.FormExportTSDBName(input.RepoName, input.SeriesName, ExportTypeTSDB)
	createExportToken, ok := input.AutoExportTSDBTokens.CreateExportToken[exportInput.ExportName]
	if !ok {
		createExportToken = models.PandoraToken{}
	}
	exportInput.PandoraToken = createExportToken
	err = c.CreateExport(exportInput)
	if err != nil && reqerr.IsExistError(err) {
		updateExportToken, ok := input.AutoExportTSDBTokens.UpdateExportToken[exportInput.ExportName]
		if !ok {
			updateExportToken = models.PandoraToken{}
		}
		err = c.UpdateExport(&UpdateExportInput{
			RepoName:     exportInput.RepoName,
			ExportName:   exportInput.ExportName,
			Spec:         exportInput.Spec,
			PandoraToken: updateExportToken,
		})
		if err != nil {
			log.Error("update Export error", err)
		}
	}
	return err
}

func (c *Pipeline) CreateForMutiExportTSDB(input *CreateRepoForMutiExportTSDBInput) error {
	_, err := c.GetRepo(&GetRepoInput{
		RepoName:     input.RepoName,
		PandoraToken: input.PipelineGetRepoToken,
	})
	if err != nil {
		return err
	}
	tsdbapi, err := c.GetTSDBAPI()
	if err != nil {
		return err
	}
	if input.TSDBRepoName == "" {
		input.TSDBRepoName = input.RepoName
	}
	err = tsdbapi.CreateRepo(&tsdb.CreateRepoInput{
		RepoName:     input.TSDBRepoName,
		Region:       input.Region,
		PandoraToken: input.CreateTSDBRepoToken,
	})
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	for _, series := range input.SeriesMap {
		seriesToken, ok := input.CreateTSDBSeriesTokens[series.SeriesName]
		if !ok {
			seriesToken = models.PandoraToken{}
		}

		err = tsdbapi.CreateSeries(&tsdb.CreateSeriesInput{
			RepoName:     input.TSDBRepoName,
			SeriesName:   series.SeriesName,
			Retention:    input.Retention,
			PandoraToken: seriesToken,
		})
		if err != nil && !reqerr.IsExistError(err) {
			return err
		}
		tsdbSpec := c.FormMutiSeriesTSDBSpec(&CreateRepoForTSDBInput{
			RepoName:     input.RepoName,
			TSDBRepoName: input.TSDBRepoName,
			Region:       input.Region,
			Schema:       series.Schema,
			Retention:    input.Retention,
			SeriesName:   series.SeriesName,
			Tags:         series.Tags,
			OmitInvalid:  input.OmitInvalid,
			OmitEmpty:    input.OmitEmpty,
			Timestamp:    series.TimeStamp,
		})
		exportInput := c.FormExportInput(input.RepoName, ExportTypeTSDB, tsdbSpec)
		exportInput.ExportName = base.FormExportTSDBName(input.RepoName, series.SeriesName, ExportTypeTSDB)
		createExportToken, ok := input.AutoExportTSDBTokens.CreateExportToken[exportInput.ExportName]
		if !ok {
			createExportToken = models.PandoraToken{}
		}
		exportInput.PandoraToken = createExportToken
		err = c.CreateExport(exportInput)
		if err != nil && reqerr.IsExistError(err) {
			updateExportToken, ok := input.AutoExportTSDBTokens.UpdateExportToken[exportInput.ExportName]
			if !ok {
				updateExportToken = models.PandoraToken{}
			}
			err = c.UpdateExport(&UpdateExportInput{
				RepoName:     exportInput.RepoName,
				ExportName:   exportInput.ExportName,
				Spec:         exportInput.Spec,
				PandoraToken: updateExportToken,
			})
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (c *Pipeline) UploadUdf(input *UploadUdfInput) (err error) {
	op := c.NewOperation(base.OpUploadUdf, input.UdfName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()
	req.SetBufferBody(input.Buffer.Bytes())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) UploadUdfFromFile(input *UploadUdfFromFileInput) (err error) {
	op := c.NewOperation(base.OpUploadUdf, input.UdfName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()

	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	req.SetReaderBody(file)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) PutUdfMeta(input *PutUdfMetaInput) (err error) {
	op := c.NewOperation(base.OpPutUdfMeta, input.UdfName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) DeleteUdf(input *DeleteUdfInfoInput) (err error) {
	op := c.NewOperation(base.OpDeleteUdf, input.UdfName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

const PageFrom = "from"
const PageSize = "size"
const PageSort = "sort"

func (c *Pipeline) ListUdfs(input *ListUdfsInput) (output *ListUdfsOutput, err error) {
	query := ""
	values := url.Values{}
	if input.From > 0 && input.Size > 0 {
		values.Set(PageFrom, fmt.Sprintf("%v", input.From))
		values.Set(PageSize, fmt.Sprintf("%v", input.Size))
	}
	if len(strings.TrimSpace(input.Sort)) > 0 {
		values.Set(PageSort, fmt.Sprintf("%v", input.Sort))
	}
	if len(values) != 0 {
		query = "?" + values.Encode()
	}
	op := c.NewOperation(base.OpListUdfs, query)

	output = &ListUdfsOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) RegisterUdfFunction(input *RegisterUdfFunctionInput) (err error) {
	op := c.NewOperation(base.OpRegUdfFunc, input.FuncName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) DeRegisterUdfFunction(input *DeregisterUdfFunctionInput) (err error) {
	op := c.NewOperation(base.OpDeregUdfFunc, input.FuncName)

	req := c.newRequest(op, input.Token, nil)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListUdfFunctions(input *ListUdfFunctionsInput) (output *ListUdfFunctionsOutput, err error) {
	query := ""
	values := url.Values{}
	if input.From > 0 && input.Size > 0 {
		values.Set(PageFrom, fmt.Sprintf("%v", input.From))
		values.Set(PageSize, fmt.Sprintf("%v", input.Size))
	}
	if len(strings.TrimSpace(input.Sort)) > 0 {
		values.Set(PageSort, fmt.Sprintf("%v", input.Sort))
	}
	if len(input.JarNamesIn) > 0 {
		values.Set("jarName", fmt.Sprintf("%v", strings.Join(input.JarNamesIn, ",")))
	}
	if len(input.FuncNamesIn) > 0 {
		values.Set("funcName", fmt.Sprintf("%v", strings.Join(input.FuncNamesIn, ",")))
	}
	if len(values) != 0 {
		query = "?" + values.Encode()
	}
	op := c.NewOperation(base.OpListUdfFuncs, query)

	output = &ListUdfFunctionsOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) ListBuiltinUdfFunctions(input *ListBuiltinUdfFunctionsInput) (output *ListUdfBuiltinFunctionsOutput, err error) {
	query := ""
	values := url.Values{}
	if input.From > 0 && input.Size > 0 {
		values.Set(PageFrom, fmt.Sprintf("%v", input.From))
		values.Set(PageSize, fmt.Sprintf("%v", input.Size))
	}
	if len(strings.TrimSpace(input.Sort)) > 0 {
		values.Set(PageSort, fmt.Sprintf("%v", input.Sort))
	}
	if len(input.Categories) > 0 {
		values.Set("category", fmt.Sprintf("%v", strings.Join(input.Categories, ",")))
	}
	if len(values) != 0 {
		query = "?" + values.Encode()
	}
	op := c.NewOperation(base.OpListUdfBuiltinFuncs, query)

	output = &ListUdfBuiltinFunctionsOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) CreateWorkflow(input *CreateWorkflowInput) (err error) {
	op := c.NewOperation(base.OpCreateWorkflow, input.WorkflowName)

	req := c.newRequest(op, input.Token, nil)
	if input.Region == "" {
		input.Region = c.defaultRegion
	}
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateWorkflow(input *UpdateWorkflowInput) (err error) {
	op := c.NewOperation(base.OpUpdateWorkflow, input.WorkflowName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return req.Send()
}

func (c *Pipeline) GetWorkflow(input *GetWorkflowInput) (output *GetWorkflowOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpGetWorkflow, input.WorkflowName)
	output = &GetWorkflowOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) GetWorkflowStatus(input *GetWorkflowStatusInput) (output *GetWorkflowStatusOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpGetWorkflowStatus, input.WorkflowName)
	output = &GetWorkflowStatusOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) DeleteWorkflow(input *DeleteWorkflowInput) (err error) {
	op := c.NewOperation(base.OpDeleteWorkflow, input.WorkflowName)

	req := c.newRequest(op, input.Token, nil)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return req.Send()
}

func (c *Pipeline) ListWorkflows(input *ListWorkflowInput) (output *ListWorkflowOutput, err error) {
	op := c.NewOperation(base.OpListWorkflows)

	output = &ListWorkflowOutput{}
	req := c.newRequest(op, input.Token, &output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) StopWorkflow(input *StopWorkflowInput) (err error) {
	op := c.NewOperation(base.OpStopWorkflow, input.WorkflowName)

	req := c.newRequest(op, input.Token, nil)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) StartWorkflow(input *StartWorkflowInput) (err error) {
	op := c.NewOperation(base.OpStartWorkflow, input.WorkflowName)

	req := c.newRequest(op, input.Token, nil)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) SearchWorkflow(input *DagLogSearchInput) (ret *WorkflowSearchRet, err error) {
	op := c.NewOperation(base.OpSearchDAGlog, input.WorkflowName)

	ret = &WorkflowSearchRet{}
	req := c.newRequest(op, input.Token, ret)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return ret, req.Send()
}

func (c *Pipeline) RepoExist(input *RepoExistInput) (output *RepoExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpRepoExists, input.RepoName)

	output = &RepoExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) TransformExist(input *TransformExistInput) (output *TransformExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpTransformExists, input.RepoName, input.TransformName)

	output = &TransformExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}
func (c *Pipeline) ExportExist(input *ExportExistInput) (output *ExportExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpExportExists, input.RepoName, input.ExportName)

	output = &ExportExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DatasourceExist(input *DatasourceExistInput) (output *DatasourceExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpDatasourceExists, input.DatasourceName)

	output = &DatasourceExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) JobExist(input *JobExistInput) (output *JobExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpJobExists, input.JobName)

	output = &JobExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) JobExportExist(input *JobExportExistInput) (output *JobExportExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpJobExportExists, input.JobName, input.ExportName)

	output = &JobExportExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) CreateVariable(input *CreateVariableInput) (err error) {
	op := c.NewOperation(base.OpCreateVariable, input.Name)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) UpdateVariable(input *UpdateVariableInput) (err error) {
	op := c.NewOperation(base.OpUpdateVariable, input.Name)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) DeleteVariable(input *DeleteVariableInput) (err error) {
	op := c.NewOperation(base.OpDeleteVariable, input.Name)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) GetVariable(input *GetVariableInput) (output *GetVariableOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.NewOperation(base.OpGetVariable, input.Name)

	output = &GetVariableOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) ListUserVariables(input *ListVariablesInput) (output *ListVariablesOutput, err error) {
	op := c.NewOperation(base.OpListUserVariables, userVariableType)

	output = &ListVariablesOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) ListSystemVariables(input *ListVariablesInput) (output *ListVariablesOutput, err error) {
	op := c.NewOperation(base.OpListSystemVariables, systemVariableType)
	output = &ListVariablesOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}
