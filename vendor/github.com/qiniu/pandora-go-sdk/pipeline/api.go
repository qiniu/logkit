package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
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
	op := c.newOperation(base.OpCreateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateGroup(input *UpdateGroupInput) (err error) {
	op := c.newOperation(base.OpUpdateGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) StartGroupTask(input *StartGroupTaskInput) (err error) {
	op := c.newOperation(base.OpStartGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StopGroupTask(input *StopGroupTaskInput) (err error) {
	op := c.newOperation(base.OpStopGroupTask, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) ListGroups(input *ListGroupsInput) (output *ListGroupsOutput, err error) {
	op := c.newOperation(base.OpListGroups)

	output = &ListGroupsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetGroup(input *GetGroupInput) (output *GetGroupOutput, err error) {
	op := c.newOperation(base.OpGetGroup, input.GroupName)

	output = &GetGroupOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteGroup(input *DeleteGroupInput) (err error) {
	op := c.newOperation(base.OpDeleteGroup, input.GroupName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.newOperation(base.OpCreateRepo, input.RepoName)
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
		PipelineToken: input.PipelineToken,
		RepoName:      input.RepoName,
		Region:        input.Region,
		GroupName:     input.GroupName,
		Schema:        schemas,
		Options:       input.Options,
		Workflow:      input.Workflow,
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
	tags, ok := ex.Spec["tags"].(map[string]string)
	if !ok {
		return fmt.Errorf("export tsdb spec tags assert error %v is not map[string]interface{}", ex.Spec["tags"])
	}
	fields, ok := ex.Spec["fields"].(map[string]string)
	if !ok {
		return fmt.Errorf("export tsdb spec fields assert error %v is not map[string]interface{}", ex.Spec["fields"])
	}
	for _, v := range input.Schema {
		if input.IsTag(v.Key) {
			tags[v.Key] = v.Key
		} else {
			fields[v.Key] = v.Key
		}
	}
	spec := ExportTsdbSpec{DestRepoName: repoName, SeriesName: seriesName, Tags: tags, Fields: fields}
	err := c.UpdateExport(&UpdateExportInput{
		RepoName:   input.RepoName,
		ExportName: ex.Name,
		Spec:       spec,
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
	repoInfo, err := logdbAPI.GetRepo(&logdb.GetRepoInput{RepoName: repoName})
	if err != nil {
		return err
	}
	for _, v := range input.Schema {
		if schemaNotIn(v.Key, repoInfo.Schema) {
			scs := convertSchema2LogDB([]RepoSchemaEntry{v})
			if len(scs) > 0 {
				repoInfo.Schema = append(repoInfo.Schema, scs[0])
			}
			docs[v.Key] = "#" + v.Key
		}
	}
	if err = logdbAPI.UpdateRepo(&logdb.UpdateRepoInput{
		RepoName:  repoName,
		Retention: repoInfo.Retention,
		Schema:    repoInfo.Schema,
	}); err != nil {
		return err
	}
	spec := &ExportLogDBSpec{DestRepoName: repoName, Doc: docs}
	err = c.UpdateExport(&UpdateExportInput{
		RepoName:   input.RepoName,
		ExportName: ex.Name,
		Spec:       spec,
	})
	if reqerr.IsExportRemainUnchanged(err) {
		err = nil
	}
	return err
}

func (c *Pipeline) UpdateRepoWithKodo(input *UpdateRepoInput, ex ExportDesc) error {
	repoName, ok := ex.Spec["bucket"].(string)
	if !ok {
		return fmt.Errorf("export logdb spec destRepoName assert error %v is not string", ex.Spec["destRepoName"])
	}
	docs, ok := ex.Spec["fields"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("export logdb spec doc assert error %v is not map[string]interface{}", ex.Spec["doc"])
	}
	for _, v := range input.Schema {
		docs[v.Key] = "#" + v.Key
	}

	spec := &ExportLogDBSpec{DestRepoName: repoName, Doc: docs}
	err := c.UpdateExport(&UpdateExportInput{
		RepoName:   input.RepoName,
		ExportName: ex.Name,
		Spec:       spec,
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
	err = c.updateRepo(input)
	if err != nil {
		return
	}
	if input.Option == nil {
		return nil
	}
	option := input.Option
	//这边是一个优化，对于没有任何服务的情况，节省 listexports的rpc调用
	if !option.ToLogDB && !option.ToTSDB && !option.ToKODO {
		return nil
	}
	exports, err := c.ListExports(&ListExportsInput{
		RepoName: input.RepoName,
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
	op := c.newOperation(base.OpUpdateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.newOperation(base.OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) GetSampleData(input *GetSampleDataInput) (output *SampleDataOutput, err error) {
	op := c.newOperation(base.OpGetSampleData, input.RepoName, input.Count)

	output = &SampleDataOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	var op *request.Operation
	if input.WithDag {
		op = c.newOperation(base.OpListReposWithDag)
	} else {
		op = c.newOperation(base.OpListRepos)
	}
	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.newOperation(base.OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) PostData(input *PostDataInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

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
	for i, d := range input.Datas {
		point, err := c.generatePoint(input.RepoName, d, !input.NoUpdate, input.Option, input.RepoOptions)
		if err != nil {
			return nil, err
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
			RepoName: input.RepoName,
			Buffer:   tmpBuff,
		},
	})
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
	op := c.newOperation(base.OpPostData, input.RepoName)

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

func (c *Pipeline) PostDataFromReader(input *PostDataFromReaderInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetReaderBody(input.Reader)
	req.SetBodyLength(input.BodyLength)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostDataFromBytes(input *PostDataFromBytesInput) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Buffer)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeText)
	req.SetFlowLimiter(c.flowLimit)
	req.SetReqLimiter(c.reqLimit)
	return req.Send()
}

func (c *Pipeline) PostDataFromBytesWithDeadline(input *PostDataFromBytesInput, deadline time.Time) (err error) {
	op := c.newOperation(base.OpPostData, input.RepoName)

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
	op := c.newOperation(base.OpUploadPlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()
	req.SetBufferBody(input.Buffer.Bytes())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) UploadPluginFromFile(input *UploadPluginFromFileInput) (err error) {
	op := c.newOperation(base.OpUploadPlugin, input.PluginName)

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
	op := c.newOperation(base.OpListPlugins)

	output = &ListPluginsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) VerifyPlugin(input *VerifyPluginInput) (output *VerifyPluginOutput, err error) {
	op := c.newOperation(base.OpVerifyPlugin, input.PluginName)

	output = &VerifyPluginOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetPlugin(input *GetPluginInput) (output *GetPluginOutput, err error) {
	op := c.newOperation(base.OpGetPlugin, input.PluginName)

	output = &GetPluginOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeletePlugin(input *DeletePluginInput) (err error) {
	op := c.newOperation(base.OpDeletePlugin, input.PluginName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateTransform(input *CreateTransformInput) (err error) {
	op := c.newOperation(base.OpCreateTransform, input.SrcRepoName, input.TransformName, input.DestRepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateTransform(input *UpdateTransformInput) (err error) {
	op := c.newOperation(base.OpUpdateTransform, input.SrcRepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input.Spec); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListTransforms(input *ListTransformsInput) (output *ListTransformsOutput, err error) {
	op := c.newOperation(base.OpListTransforms, input.RepoName)

	output = &ListTransformsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetTransform(input *GetTransformInput) (output *GetTransformOutput, err error) {
	op := c.newOperation(base.OpGetTransform, input.RepoName, input.TransformName)

	output = &GetTransformOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteTransform(input *DeleteTransformInput) (err error) {
	op := c.newOperation(base.OpDeleteTransform, input.RepoName, input.TransformName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateExport(input *CreateExportInput) (err error) {
	op := c.newOperation(base.OpCreateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) UpdateExport(input *UpdateExportInput) (err error) {
	op := c.newOperation(base.OpUpdateExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListExports(input *ListExportsInput) (output *ListExportsOutput, err error) {
	op := c.newOperation(base.OpListExports, input.RepoName)

	output = &ListExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetExport(input *GetExportInput) (output *GetExportOutput, err error) {
	op := c.newOperation(base.OpGetExport, input.RepoName, input.ExportName)

	output = &GetExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteExport(input *DeleteExportInput) (err error) {
	op := c.newOperation(base.OpDeleteExport, input.RepoName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateDatasource(input *CreateDatasourceInput) (err error) {
	op := c.newOperation(base.OpCreateDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListDatasources() (output *ListDatasourcesOutput, err error) {
	op := c.newOperation(base.OpListDatasources)

	output = &ListDatasourcesOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetDatasource(input *GetDatasourceInput) (output *GetDatasourceOutput, err error) {
	op := c.newOperation(base.OpGetDatasource, input.DatasourceName)

	output = &GetDatasourceOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteDatasource(input *DeleteDatasourceInput) (err error) {
	op := c.newOperation(base.OpDeleteDatasource, input.DatasourceName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) CreateJob(input *CreateJobInput) (err error) {
	op := c.newOperation(base.OpCreateJob, input.JobName)

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
	op := c.newOperation(base.OpListJobs, query)

	output = &ListJobsOutput{}
	req := c.newRequest(op, "", &output)
	return output, req.Send()
}

func (c *Pipeline) GetJob(input *GetJobInput) (output *GetJobOutput, err error) {
	op := c.newOperation(base.OpGetJob, input.JobName)

	output = &GetJobOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJob(input *DeleteJobInput) (err error) {
	op := c.newOperation(base.OpDeleteJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StartJob(input *StartJobInput) (err error) {
	op := c.newOperation(base.OpStartJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) GetJobHistory(input *GetJobHistoryInput) (output *GetJobHistoryOutput, err error) {
	op := c.newOperation(base.OpGetJobHistory, input.JobName)

	output = &GetJobHistoryOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) StopJob(input *StopJobInput) (err error) {
	op := c.newOperation(base.OpStopJob, input.JobName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) StopJobBatch(input *StopJobBatchInput) (output *StopJobBatchOutput, err error) {
	op := c.newOperation(base.OpStopJobBatch)

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
	op := c.newOperation(base.OpRerunJobBatch)

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
	op := c.newOperation(base.OpCreateJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) ListJobExports(input *ListJobExportsInput) (output *ListJobExportsOutput, err error) {
	op := c.newOperation(base.OpListJobExports, input.JobName)

	output = &ListJobExportsOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Pipeline) GetJobExport(input *GetJobExportInput) (output *GetJobExportOutput, err error) {
	op := c.newOperation(base.OpGetJobExport, input.JobName, input.ExportName)

	output = &GetJobExportOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DeleteJobExport(input *DeleteJobExportInput) (err error) {
	op := c.newOperation(base.OpDeleteJobExport, input.JobName, input.ExportName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Pipeline) RetrieveSchema(input *RetrieveSchemaInput) (output *RetrieveSchemaOutput, err error) {
	op := c.newOperation(base.OpRetrieveSchema)

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
	repo, err := c.GetRepo(&GetRepoInput{
		RepoName: repoName,
	})

	if err != nil {
		return
	}
	schemas = make(map[string]RepoSchemaEntry)
	for _, sc := range repo.Schema {
		schemas[sc.Key] = sc
	}
	c.repoSchemaMux.Lock()
	c.repoSchemas[repoName] = schemas
	c.repoSchemaMux.Unlock()
	return
}

func (c *Pipeline) CreateForLogDB(input *CreateRepoForLogDBInput) error {
	pinput := formPipelineRepoInput(input.RepoName, input.Region, input.Schema)
	err := c.CreateRepo(pinput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	linput := convertCreate2LogDB(input)
	logdbapi, err := c.GetLogDBAPI()
	if err != nil {
		return err
	}
	err = logdbapi.CreateRepo(linput)
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	logDBSpec := c.FormLogDBSpec(input)
	exportInput := c.FormExportInput(input.RepoName, ExportTypeLogDB, logDBSpec)
	return c.CreateExport(exportInput)
}

func (c *Pipeline) CreateForLogDBDSL(input *CreateRepoForLogDBDSLInput) error {
	schemas, err := toSchema(input.Schema, 0)
	if err != nil {
		return err
	}
	ci := &CreateRepoForLogDBInput{
		RepoName:    input.RepoName,
		LogRepoName: input.LogRepoName,
		Region:      input.Region,
		Schema:      schemas,
		Retention:   input.Retention,
	}
	return c.CreateForLogDB(ci)
}

func (c *Pipeline) CreateForTSDB(input *CreateRepoForTSDBInput) error {
	_, err := c.GetRepo(&GetRepoInput{
		RepoName: input.RepoName,
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
		RepoName: input.TSDBRepoName,
		Region:   input.Region,
	})
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	if input.SeriesName == "" {
		input.SeriesName = input.RepoName
	}
	err = tsdbapi.CreateSeries(&tsdb.CreateSeriesInput{
		RepoName:   input.TSDBRepoName,
		SeriesName: input.SeriesName,
		Retention:  input.Retention,
	})
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	tsdbSpec := c.FormTSDBSpec(input)
	exportInput := c.FormExportInput(input.RepoName, ExportTypeTSDB, tsdbSpec)
	exportInput.ExportName = base.FormExportTSDBName(input.RepoName, input.SeriesName, ExportTypeTSDB)
	err = c.CreateExport(exportInput)
	if err != nil && reqerr.IsExistError(err) {
		err = c.UpdateExport(&UpdateExportInput{
			RepoName:   exportInput.RepoName,
			ExportName: exportInput.ExportName,
			Spec:       exportInput.Spec,
		})
	}
	return err
}

func (c *Pipeline) CreateForMutiExportTSDB(input *CreateRepoForMutiExportTSDBInput) error {
	_, err := c.GetRepo(&GetRepoInput{
		RepoName: input.RepoName,
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
		RepoName: input.TSDBRepoName,
		Region:   input.Region,
	})
	if err != nil && !reqerr.IsExistError(err) {
		return err
	}
	for _, series := range input.SeriesMap {
		err = tsdbapi.CreateSeries(&tsdb.CreateSeriesInput{
			RepoName:   input.TSDBRepoName,
			SeriesName: series.SeriesName,
			Retention:  input.Retention,
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
		err = c.CreateExport(exportInput)
		if err != nil && reqerr.IsExistError(err) {
			err = c.UpdateExport(&UpdateExportInput{
				RepoName:   exportInput.RepoName,
				ExportName: exportInput.ExportName,
				Spec:       exportInput.Spec,
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
	op := c.newOperation(base.OpUploadUdf, input.UdfName)

	req := c.newRequest(op, input.Token, nil)
	req.EnableContentMD5d()
	req.SetBufferBody(input.Buffer.Bytes())
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJar)
	return req.Send()
}

func (c *Pipeline) UploadUdfFromFile(input *UploadUdfFromFileInput) (err error) {
	op := c.newOperation(base.OpUploadUdf, input.UdfName)

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
	op := c.newOperation(base.OpPutUdfMeta, input.UdfName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) DeleteUdf(input *DeleteUdfInfoInput) (err error) {
	op := c.newOperation(base.OpDeleteUdf, input.UdfName)

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
	op := c.newOperation(base.OpListUdfs, query)

	output = &ListUdfsOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) RegisterUdfFunction(input *RegisterUdfFunctionInput) (err error) {
	op := c.newOperation(base.OpRegUdfFunc, input.FuncName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Pipeline) DeRegisterUdfFunction(input *DeregisterUdfFunctionInput) (err error) {
	op := c.newOperation(base.OpDeregUdfFunc, input.FuncName)

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
	op := c.newOperation(base.OpListUdfFuncs, query)

	output = &ListUdfFunctionsOutput{}
	req := c.newRequest(op, input.Token, output)
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
	op := c.newOperation(base.OpListUdfBuiltinFuncs, query)

	output = &ListUdfBuiltinFunctionsOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) CreateWorkflow(input *CreateWorkflowInput) (err error) {
	op := c.newOperation(base.OpCreateWorkflow, input.WorkflowName)

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
	op := c.newOperation(base.OpUpdateWorkflow, input.WorkflowName)

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
	op := c.newOperation(base.OpGetWorkflow, input.WorkflowName)
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
	op := c.newOperation(base.OpGetWorkflowStatus, input.WorkflowName)
	output = &GetWorkflowStatusOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) DeleteWorkflow(input *DeleteWorkflowInput) (err error) {
	op := c.newOperation(base.OpDeleteWorkflow, input.WorkflowName)

	req := c.newRequest(op, input.Token, nil)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return req.Send()
}

func (c *Pipeline) ListWorkflows(input *ListWorkflowInput) (output *ListWorkflowOutput, err error) {
	op := c.newOperation(base.OpListWorkflows)

	output = &ListWorkflowOutput{}
	req := c.newRequest(op, input.Token, &output)
	if input.ResourceOwner != "" {
		req.SetHeader(base.HTTPHeaderResourceOwner, input.ResourceOwner)
	}
	return output, req.Send()
}

func (c *Pipeline) StopWorkflow(input *StopWorkflowInput) (err error) {
	op := c.newOperation(base.OpStopWorkflow, input.WorkflowName)

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
	op := c.newOperation(base.OpStartWorkflow, input.WorkflowName)

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
	op := c.newOperation(base.OpSearchDAGlog, input.WorkflowName)

	ret = &WorkflowSearchRet{}
	req := c.newRequest(op, input.Token, ret)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return ret, req.Send()
}

func (c *Pipeline) RepoExist(input *RepoExistInput) (output *RepoExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.newOperation(base.OpRepoExists, input.RepoName)

	output = &RepoExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) TransformExist(input *TransformExistInput) (output *TransformExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.newOperation(base.OpTransformExists, input.RepoName, input.TransformName)

	output = &TransformExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}
func (c *Pipeline) ExportExist(input *ExportExistInput) (output *ExportExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.newOperation(base.OpExportExists, input.RepoName, input.ExportName)

	output = &ExportExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) DatasourceExist(input *DatasourceExistInput) (output *DatasourceExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.newOperation(base.OpDatasourceExists, input.DatasourceName)

	output = &DatasourceExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) JobExist(input *JobExistInput) (output *JobExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.newOperation(base.OpJobExists, input.JobName)

	output = &JobExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) JobExportExist(input *JobExportExistInput) (output *JobExportExistOutput, err error) {
	if err = input.Validate(); err != nil {
		return
	}
	op := c.newOperation(base.OpJobExportExists, input.JobName, input.ExportName)

	output = &JobExportExistOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) CreateVariable(input *CreateVariableInput) (err error) {
	op := c.newOperation(base.OpCreateVariable, input.Name)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) UpdateVariable(input *UpdateVariableInput) (err error) {
	op := c.newOperation(base.OpUpdateVariable, input.Name)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	return req.Send()
}

func (c *Pipeline) DeleteVariable(input *DeleteVariableInput) (err error) {
	op := c.newOperation(base.OpDeleteVariable, input.Name)

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
	op := c.newOperation(base.OpGetVariable, input.Name)

	output = &GetVariableOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) ListUserVariables(input *ListVariablesInput) (output *ListVariablesOutput, err error) {
	op := c.newOperation(base.OpListUserVariables, userVariableType)

	output = &ListVariablesOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Pipeline) ListSystemVariables(input *ListVariablesInput) (output *ListVariablesOutput, err error) {
	op := c.newOperation(base.OpListSystemVariables, systemVariableType)
	output = &ListVariablesOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}
