package pipeline

import (
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
)

type PipelineAPI interface {
	InitOrUpdateWorkflow(input *InitOrUpdateWorkflowInput) error

	AutoExportToLogDB(*AutoExportToLogDBInput) error

	AutoExportToKODO(*AutoExportToKODOInput) error

	AutoExportToTSDB(*AutoExportToTSDBInput) error

	CreateGroup(*CreateGroupInput) error

	UpdateGroup(*UpdateGroupInput) error

	StartGroupTask(*StartGroupTaskInput) error

	StopGroupTask(*StopGroupTaskInput) error

	ListGroups(*ListGroupsInput) (*ListGroupsOutput, error)

	GetGroup(*GetGroupInput) (*GetGroupOutput, error)

	DeleteGroup(*DeleteGroupInput) error

	CreateRepo(*CreateRepoInput) error

	CreateRepoFromDSL(*CreateRepoDSLInput) error

	UpdateRepo(*UpdateRepoInput) error

	GetRepo(*GetRepoInput) (*GetRepoOutput, error)

	GetSampleData(*GetSampleDataInput) (*SampleDataOutput, error)

	ListRepos(*ListReposInput) (*ListReposOutput, error)

	DeleteRepo(*DeleteRepoInput) error

	PostData(*PostDataInput) error

	PostRawtextData(input *PostRawtextDataInput) error

	PostLargeData(*PostDataInput, time.Duration) (Points, error)

	PostDataSchemaFree(input *SchemaFreeInput) (map[string]RepoSchemaEntry, error)

	PostDataFromFile(*PostDataFromFileInput) error

	PostDataFromReader(*PostDataFromReaderInput) error

	PostDataFromBytes(*PostDataFromBytesInput) error

	UploadPlugin(*UploadPluginInput) error

	UploadPluginFromFile(*UploadPluginFromFileInput) error

	VerifyPlugin(*VerifyPluginInput) (*VerifyPluginOutput, error)

	ListPlugins(*ListPluginsInput) (*ListPluginsOutput, error)

	GetPlugin(*GetPluginInput) (*GetPluginOutput, error)

	DeletePlugin(*DeletePluginInput) error

	CreateTransform(*CreateTransformInput) error

	UpdateTransform(*UpdateTransformInput) error

	GetTransform(*GetTransformInput) (*GetTransformOutput, error)

	ListTransforms(*ListTransformsInput) (*ListTransformsOutput, error)

	DeleteTransform(*DeleteTransformInput) error

	CreateExport(*CreateExportInput) error

	UpdateExport(*UpdateExportInput) error

	GetExport(*GetExportInput) (*GetExportOutput, error)

	ListExports(*ListExportsInput) (*ListExportsOutput, error)

	DeleteExport(*DeleteExportInput) error

	CreateDatasource(*CreateDatasourceInput) error

	GetDatasource(*GetDatasourceInput) (*GetDatasourceOutput, error)

	ListDatasources() (*ListDatasourcesOutput, error)

	DeleteDatasource(*DeleteDatasourceInput) error

	CreateJob(*CreateJobInput) error

	GetJob(*GetJobInput) (*GetJobOutput, error)

	ListJobs(*ListJobsInput) (*ListJobsOutput, error)

	DeleteJob(*DeleteJobInput) error

	// 启动接口不适用于新版本workflow(2017/12/14起), 请使用StopWorkflow来停止当前workflow的jobs
	StartJob(*StartJobInput) error

	// 停止接口不适用于新版本workflow(2017/12/14起)，请使用StartWorkflow来启动当前workflow的jobs
	StopJob(*StopJobInput) error

	GetJobHistory(*GetJobHistoryInput) (*GetJobHistoryOutput, error)

	StopJobBatch(*StopJobBatchInput) (*StopJobBatchOutput, error)

	RerunJobBatch(*RerunJobBatchInput) (*RerunJobBatchOutput, error)

	CreateJobExport(*CreateJobExportInput) error

	GetJobExport(*GetJobExportInput) (*GetJobExportOutput, error)

	ListJobExports(*ListJobExportsInput) (*ListJobExportsOutput, error)

	DeleteJobExport(*DeleteJobExportInput) error

	RetrieveSchema(*RetrieveSchemaInput) (*RetrieveSchemaOutput, error)

	MakeToken(*base.TokenDesc) (string, error)

	GetDefault(RepoSchemaEntry) interface{}

	GetUpdateSchemas(string) (map[string]RepoSchemaEntry, error)

	GetUpdateSchemasWithInput(input *GetRepoInput) (map[string]RepoSchemaEntry, error)

	UploadUdf(input *UploadUdfInput) (err error)

	UploadUdfFromFile(input *UploadUdfFromFileInput) (err error)

	PutUdfMeta(input *PutUdfMetaInput) (err error)

	DeleteUdf(input *DeleteUdfInfoInput) (err error)

	ListUdfs(input *ListUdfsInput) (output *ListUdfsOutput, err error)

	RegisterUdfFunction(input *RegisterUdfFunctionInput) (err error)

	DeRegisterUdfFunction(input *DeregisterUdfFunctionInput) (err error)

	ListUdfFunctions(input *ListUdfFunctionsInput) (output *ListUdfFunctionsOutput, err error)

	ListBuiltinUdfFunctions(input *ListBuiltinUdfFunctionsInput) (output *ListUdfBuiltinFunctionsOutput, err error)

	CreateWorkflow(input *CreateWorkflowInput) (err error)

	UpdateWorkflow(input *UpdateWorkflowInput) (err error)

	GetWorkflow(input *GetWorkflowInput) (output *GetWorkflowOutput, err error)

	GetWorkflowStatus(input *GetWorkflowStatusInput) (output *GetWorkflowStatusOutput, err error)

	DeleteWorkflow(input *DeleteWorkflowInput) (err error)

	StartWorkflow(input *StartWorkflowInput) error

	StopWorkflow(input *StopWorkflowInput) error

	ListWorkflows(input *ListWorkflowInput) (output *ListWorkflowOutput, err error)

	SearchWorkflow(input *DagLogSearchInput) (ret *WorkflowSearchRet, err error)

	RepoExist(input *RepoExistInput) (output *RepoExistOutput, err error)

	TransformExist(input *TransformExistInput) (output *TransformExistOutput, err error)

	ExportExist(input *ExportExistInput) (output *ExportExistOutput, err error)

	DatasourceExist(input *DatasourceExistInput) (output *DatasourceExistOutput, err error)

	JobExist(input *JobExistInput) (output *JobExistOutput, err error)

	JobExportExist(input *JobExportExistInput) (output *JobExportExistOutput, err error)

	CreateVariable(input *CreateVariableInput) (err error)

	UpdateVariable(input *UpdateVariableInput) (err error)

	DeleteVariable(input *DeleteVariableInput) (err error)

	GetVariable(input *GetVariableInput) (output *GetVariableOutput, err error)

	ListUserVariables(input *ListVariablesInput) (output *ListVariablesOutput, err error)

	ListSystemVariables(input *ListVariablesInput) (output *ListVariablesOutput, err error)

	Close() error
}
