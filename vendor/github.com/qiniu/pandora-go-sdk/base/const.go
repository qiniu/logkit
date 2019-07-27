package base

const (
	MethodGet    string = "GET"
	MethodPost   string = "POST"
	MethodDelete string = "DELETE"
	MethodPut    string = "PUT"
)

const (
	HTTPHeaderAppId         string = "X-AppId"
	HTTPHeaderContentType   string = "Content-Type"
	HTTPHeaderContentLength string = "Content-Length"
	HTTPHeaderContentMD5    string = "Content-MD5"
	HTTPHeaderRequestId     string = "X-Reqid"
	HTTPHeaderAuthorization string = "Authorization"
	HTTPHeaderResourceOwner string = "X-Resource-Owner"
)

const (
	OpCreateGroup         string = "CreateGroup"
	OpUpdateGroup         string = "UpdateGroup"
	OpStartGroupTask      string = "StartGroupTask"
	OpStopGroupTask       string = "StopGroupTask"
	OpListGroups          string = "ListGroup"
	OpGetGroup            string = "GetGroup"
	OpDeleteGroup         string = "DeleteGroup"
	OpCreateRepo          string = "CreateRepo"
	OpGetRepo             string = "GetRepo"
	OpGetSampleData       string = "GetSampleData"
	OpListRepos           string = "ListRepo"
	OpListReposWithDag    string = "ListRepoWithDag"
	OpListReposAuthorized    string = "ListRepoAuthorized"
	OpDeleteRepo          string = "DeleteRepo"
	OpRepoExists          string = "RepoExists"
	OpPostData            string = "PostData"
	OpPostRawtextData     string = "PostRawtextData"
	OpPostTextData        string = "PostTextData"
	OpCreateTransform     string = "CreateTransform"
	OpUpdateTransform     string = "UpdateTransform"
	OpGetTransform        string = "GetTransform"
	OpListTransforms      string = "ListTransform"
	OpDeleteTransform     string = "DeleteTransform"
	OpTransformExists     string = "TransformExists"
	OpCreateExport        string = "CreateExport"
	OpUpdateExport        string = "UpdateExport"
	OpGetExport           string = "GetExport"
	OpListExports         string = "ListExport"
	OpDeleteExport        string = "DeleteExport"
	OpExportExists        string = "ExportExists"
	OpUploadPlugin        string = "UploadPlugin"
	OpGetPlugin           string = "GetPlugin"
	OpListPlugins         string = "ListPlugin"
	OpVerifyPlugin        string = "VerifyPlugin"
	OpDeletePlugin        string = "DeletePlugin"
	OpCreateDatasource    string = "CreateDatasource"
	OpGetDatasource       string = "GetDatasource"
	OpListDatasources     string = "ListDatasources"
	OpDeleteDatasource    string = "DeleteDatasource"
	OpDatasourceExists    string = "DatasourceExists"
	OpCreateJob           string = "CreateJob"
	OpGetJob              string = "GetJob"
	OpListJobs            string = "ListJobs"
	OpDeleteJob           string = "DeleteJob"
	OpStartJob            string = "StartJob"
	OpStopJob             string = "StopJob"
	OpJobExists           string = "JobExists"
	OpGetJobHistory       string = "GetJobHistory"
	OpStopJobBatch        string = "StopJobBatch"
	OpRerunJobBatch       string = "RerunJobBatch"
	OpCreateJobExport     string = "CreateJobExport"
	OpGetJobExport        string = "GetJobExport"
	OpListJobExports      string = "ListJobExports"
	OpDeleteJobExport     string = "DeleteJobExport"
	OpJobExportExists     string = "JobExportExists"
	OpRetrieveSchema      string = "RetrieveSchema"
	OpUploadUdf           string = "UploadUdf"
	OpPutUdfMeta          string = "PutUdfMeta"
	OpDeleteUdf           string = "DeleteUdf"
	OpListUdfs            string = "ListUdfs"
	OpRegUdfFunc          string = "RegisterUdfFunction"
	OpDeregUdfFunc        string = "DeregisterUdfFunction"
	OpListUdfFuncs        string = "ListUdfFuncs"
	OpListUdfBuiltinFuncs string = "ListUdfBuiltinFuncs"
	OpCreateWorkflow      string = "CreateWorkflow"
	OpUpdateWorkflow      string = "UpdateWorkflow"
	OpDeleteWorkflow      string = "DeleteWorkflow"
	OpListWorkflows       string = "ListWorkflow"
	OpGetWorkflow         string = "GetWorkflow"
	OpGetWorkflowStatus   string = "GetWorkflowStatus"
	OpStartWorkflow       string = "StartWorkflow"
	OpStopWorkflow        string = "StopWorkflow"
	OpSearchDAGlog        string = "SearchDAGLog"
	OpCreateVariable      string = "CreateVariable"
	OpUpdateVariable      string = "UpdateVariable"
	OpDeleteVariable      string = "DeleteVariable"
	OpGetVariable         string = "GetVariable"
	OpListUserVariables   string = "ListUserVariables"
	OpListSystemVariables string = "ListSystemVariables"

	OpUpdateRepo        string = "UpdateRepo"
	OpSendLog           string = "SendLog"
	OpQueryLog          string = "QueryLog"
	OpQueryScroll       string = "QueryScroll"
	OpQueryHistogramLog string = "QueryHistogramLog"
	OpPutRepoConfig     string = "PutRepoConfig"
	OpGetRepoConfig     string = "GetRepoConfig"
	OpPartialQuery      string = "PartialQuery"
	OpSchemaRef         string = "SchemaRef"

	OpUpdateRepoMetadata string = "UpdataRepoMetadata"
	OpDeleteRepoMetadata string = "DeleteRepoMetadata"
	OpUpdateViewMetadata string = "UpdataViewMetadata"
	OpDeleteViewMetadata string = "DeleteViewMetadata"

	OpCreateSeries         string = "CreateSeries"
	OpUpdateSeriesMetadata string = "UpdataSeriesMetadata"
	OpDeleteSeriesMetadata string = "DeleteSeriesMetadata"
	OpListSeries           string = "ListSeries"
	OpDeleteSeries         string = "DeleteSeries"

	OpCreateView  string = "CreateView"
	OpListView    string = "ListView"
	OpGetView     string = "GetView"
	OpDeleteView  string = "DeleteView"
	OpQueryPoints string = "QueryPoints"
	OpWritePoints string = "WritePoints"

	OpActivateUser   string = "ActivateUser"
	OpCreateDatabase string = "CreateDatabase"
	OpListDatabases  string = "ListDatabases"
	OpDeleteDatabase string = "DeleteDatabase"
	OpCreateTable    string = "CreateTable"
	OpUpdateTable    string = "UpdateTable"
	OpListTables     string = "ListTables"
	OpDeleteTable    string = "DeleteTable"
	OpGetTable       string = "GetTable"
)

const (
	ContentTypeJson        string = "application/json"
	ContentTypeJar         string = "application/java-archive"
	ContentTypeText        string = "text/plain"
	ContentTypeOctetStream string = "application/octet-stream"
)

const (
	NestLimit int = 5
)

const (
	// workflow 状态
	WorkflowReady    = "Ready"    // 新建状态
	WorkflowStarting = "Starting" // 存在任一资源为 Starting
	WorkflowStarted  = "Started"  // 所有资源为 Started
	WorkflowStopping = "Stopping" // 存在任一资源为 Stopping
	WorkflowStopped  = "Stopped"  // 所有资源为 Stopped
	WorkflowUnknown  = "Unknown"  // 获取状态失败时的异常状态，
)
