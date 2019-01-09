package reqerr

import "fmt"

const (
	DefaultRequestError = iota
	InvalidArgs
	NoSuchRepoError
	RepoAlreadyExistsError
	InvalidSliceArgumentError
	UnmatchedSchemaError
	UnauthorizedError
	InternalServerError
	NotImplementedError
	NoSuchGroupError
	GroupAlreadyExistsError
	NoSuchTransformError
	TransformAlreadyExistsError
	NoSuchExportError
	ExportAlreadyExistsError
	NoSuchPluginError
	PluginAlreadyExistsError
	RepoCascadingError
	RepoInCreatingError
	InvalidTransformSpecError
	ErrInvalidTransformInterval
	ErrInvalidTransformSql
	ErrInvalidTransformPluginOutput
	ErrInvalidFieldInSQL
	ErrDuplicateField
	ErrUnsupportedFieldType
	InvalidExportSpecError
	ExportSpecRemainUnchanged
	NoSuchRetentionError
	SeriesAlreadyExistsError
	NoSuchSeriesError
	InvalidSeriesNameError
	InvalidViewNameError
	InvalidViewSqlError
	ViewFuncNotSupportError
	NoSuchViewError
	ViewAlreadyExistsError
	InvalidViewStatementError
	PointsNotInSameRetentionError
	TimestampTooFarFromNowError
	InvalidQuerySql
	QueryInterruptError
	ExecuteSqlError
	EntityTooLargeError
	ErrInvalidVariableType
	InvalidDataSchemaError
	ErrIncompatibleRepoSchema
	ErrDBNameInvalidError
	ErrInvalidSqlError
	ErrInternalServerError
	ErrInvalidParameterError
	ErrDBNotFoundError
	ErrTableNotFoundError
	ErrInvalidDataSourceName
	ErrDataSourceExist
	ErrDataSourceNotExist
	ErrDataSourceCascading
	ErrInvalidJobName
	ErrJobExist
	ErrJobNotExist
	ErrJobArgumentCount
	ErrJobCascading
	ErrInvalidJobExportName
	ErrJobExportExist
	ErrJobExportNotExist
	ErrJobSrcNotExist
	ErrDuplicateTableName
	ErrInvalidBatchSpec
	ErrIncompatibleSourceSchema
	ErrInvalidTransformPlugin
	ErrInvalidJobSQL
	ErrBucketNotExist
	ErrDatasourceNoFiles
	ErrStartJob
	ErrStopJob
	ErrFileFormatMismatch
	ErrJobRunIdNotExist
	ErrBatchCannotRerun
	ErrUdfJarNotExist
	ErrInvalidUdfJarName
	ErrInvalidUdfFuncName
	ErrInvalidJavaClassName
	ErrStartExport
	ErrStopExport
	ErrUdfClassTypeError
	ErrUdfClassNotFound
	ErrUdfFunctionNotImplement
	ErrUdfFunctionNotFound
	ErrUdfJarExisted
	ErrUdfFuncExisted
	ErrDuplicationWithSystemFunc
	ErrIllegalCharacterInPath
	ErrInvalidDstRepoSchema
	ErrInvalidDstRepoSchemaLength
	ErrBatchStatusCannotStop

	ErrInvalidWorkflowName
	ErrWorkflowAlreadyExists
	ErrNoSuchWorkflow
	ErrWorkflowSpecContent
	ErrUpdateWorkflow
	ErrStartWorkflow
	ErrStopWorkflow
	ErrWorkflowStructure
	ErrStartTransform
	ErrStopTransform
	ErrBatchStatusCannotRerun
	ErrNoExecutableJob
	ErrJobExportSpec
	ErrWorkflowCreatingTooManyRepos
	ErrWorkflowJobsCoexist
	ErrInvalidVariableName
	ErrInvalidVariableValue
	ErrPathFilter
	ErrVariableNotExist
	ErrVariableAlreadyExist
	ErrSameToSystemVariable
	ErrTransformUpdate
	ErrSQLWithUndefinedVariable
	ErrWorkflowNameSameToRepoOrDatasource
	ErrJobReRunOrCancel
	ErrStartOrStopBatchJob
	ErrTimeFormatInvalid
	ErrNoSuchResourceOwner
	ErrAccessDenied
	ErrTransformRepeatRestart
	ErrFusionPathUsedStringVariable
	ErrFusionPathWithUndefinedVariable
	ErrTooManySchema
	ErrSchemaLimitUnderflow

	ErrRequestBodyInvalid
	ErrInvalidArgs
	ErrJobInfoInvalid
	ErrAgentRegister
	ErrAgentReportJobStates
	ErrUnknownAction
	ErrGetUserInformation
	ErrGetPagingInfo
	ErrGetAgentInfo
	ErrGetAgentIdList
	ErrGetRunnerInfo
	ErrRemoveAgentInfo
	ErrRemoveRunner
	ErrRemoveJobStates
	ErrUpdateAgentInfo
	ErrAgentReportMetrics
	ErrGetVersionConfigItem
	ErrNoValidAgentsFound
	ErrScheduleAgent
	ErrNoSuchAgent
	ErrGetMachineInfo
	ErrGetAgents
	ErrDoLogDBMSearch
	ErrGetJobJnfos
	ErrGetJobStates
	ErrGetUserToken
	ErrSystemNotSupport
	ErrGetSenderPandora
	ErrStatusDecodeBase64
	ErrGetGrokCheck
	ErrParamsCheck
	ErrGetTagList
	ErrGetConfigList
	ErrInsertConfigs
	ErrAssignConfigs
	ErrNoSuchEntry
	ErrGetRunnerList
	ErrUpdateAssignConfigs
	ErrRawDataSize
	ErrHeadPattern
	ErrConfig
	ErrUnmarshal
	ErrLogParser
	ErrTransformer
	ErrSender
	ErrRouter
	ErrUpdateTags
	ErrAgentInfoNotFound
	ErrGetAgentRelease
	ErrJobRelease
	ErrAgentsDisconnect
	ErrUpdateConfigs
	ErrRemoveConfigs
	ErrUpdateRunners
	ErrAssignTags
	ErrAddTags
	ErrRemoveTags
	ErrNotFoundRecord
	ErrExistRecord
	ErrDoLogDBJob
	ErrDoLogDBAnalysis
	ErrTimeStamp
	ErrNoRunnerInfo
	ErrMachineMetricNotOn
	ErrDeleteRunner
	ErrAddRunner
	ErrDisableDeleteMetrics
	ErrDisablePostMetrics
	ErrUnprocessableEntity
	ErrDisablePostMetricsLogdb

	ErrInvalidRepoDescription
	ErrInvalidRepoSchemaDescription

	ErrSchemaFieldNotExist
	ErrTagsDecodeError

	ErrInvalidSchemaKey
	ErrInvalidTimestamp
	ErrInvalidUrl
	ErrInvalidDestinationField
	ErrRepoFieldNotExist
	ErrEmptySourceField
	ErrFieldNotExist
	ErrIncompatibleTypes
	ErrInvalidFieldName
	ErrFieldMissed
	ErrInvalidFieldType
	ErrEmptyField
	ErrInvalidPrefix
	ErrInvalidFieldValue
	ErrInvalidExportType
	ErrNoSuchBucket
	ErrSourceFieldInvalidPrefix
	ErrFieldTypeError
	ErrRepoNotExistError
	ErrNoSuchSeries
	ErrInvalidTagName
	ErrDuplicatedKey
	ErrMissingDelimiterForCsv
	ErrRotateSizeExceed
	ErrNoSuchDatabase
	ErrNoSuchTables
	ErrDestinationEmptyError
	ErrInvalidSourceField
	ErrInvalidEmptySourceField
	ErrConnectHdfsFailed
	ErrStatFileFailed
	ErrExportTypeDisabled
)

type ErrBuilder interface {
	Build(message, rawText, reqId string, statusCode int) error
}

func NewInvalidArgs(name, message string) *RequestError {
	return &RequestError{
		Message:   fmt.Sprintf("Invalid args, argName: %s, reason: %s", name, message),
		ErrorType: InvalidArgs,
		Component: "pandora",
	}
}

//WithComponent  增加错误属于哪个组件的提示
func (re *RequestError) WithComponent(component string) *RequestError {
	re.Component = component
	return re
}

type RequestError struct {
	Message    string `json:"error"`
	StatusCode int    `json:"-"`
	RequestId  string `json:"-"`
	RawMessage string `json:"-"`
	ErrorType  int    `json:"-"`
	Component  string `json:"-"`
}

func New(message, rawText, reqId string, statusCode int) *RequestError {
	return &RequestError{
		Message:    message,
		StatusCode: statusCode,
		RequestId:  reqId,
		RawMessage: rawText,
		ErrorType:  DefaultRequestError,
		Component:  "pandora",
	}
}

func (r RequestError) Error() string {
	return fmt.Sprintf("[%s] error: StatusCode=%d, ErrorMessage=%s, RequestId=%s", r.Component, r.StatusCode, r.Message, r.RequestId)
}

func IsExistError(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == RepoAlreadyExistsError || reqErr.ErrorType == SeriesAlreadyExistsError {
		return true
	}
	if reqErr.ErrorType == ExportAlreadyExistsError || reqErr.ErrorType == ErrWorkflowAlreadyExists {
		return true
	}
	if reqErr.ErrorType == ErrExistRecord {
		return true
	}
	return false
}

func IsNoSuchWorkflow(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == ErrNoSuchWorkflow {
		return true
	}
	return false
}

func IsWorkflowStatError(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == ErrUpdateWorkflow {
		return true
	}
	return false
}

func IsWorkflowNoExecutableJob(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == ErrNoExecutableJob {
		return true
	}
	return false
}

func IsNoSuchResourceError(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == ErrNoSuchWorkflow {
		return true
	}
	if reqErr.ErrorType == NoSuchRepoError {
		return true
	}
	if reqErr.ErrorType == NoSuchTransformError {
		return true
	}
	if reqErr.ErrorType == NoSuchExportError {
		return true
	}
	if reqErr.ErrorType == NoSuchSeriesError {
		return true
	}
	if reqErr.ErrorType == ErrDataSourceNotExist {
		return true
	}
	if reqErr.ErrorType == ErrJobNotExist {
		return true
	}
	if reqErr.ErrorType == ErrJobExportNotExist {
		return true
	}
	if reqErr.ErrorType == ErrNotFoundRecord {
		return true
	}
	return false
}

func IsExportRemainUnchanged(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == ExportSpecRemainUnchanged {
		return true
	}
	return false
}

//SendErrorType 表达是否需要外部对数据做特殊处理
type SendErrorType string

const (
	TypeDefault = SendErrorType("")

	//TypeBinaryUnpack 表示外部需要进一步二分数据
	TypeBinaryUnpack = SendErrorType("Data Need Binary Unpack")

	//TypeContainInvalidPoint 表示点无效，使用TypeBinaryUnpack类似逻辑将无效的点找出来
	TypeContainInvalidPoint = SendErrorType("Contain Invalid Point")

	//TypeSchemaFreeRetry 表示在schemafree情况下服务端schema更新带来的localcache错误，重试即可
	TypeSchemaFreeRetry = SendErrorType("if schema free then retry")
)

type SendError struct {
	failDatas []map[string]interface{}
	failLines []string
	isline    bool
	msg       string
	ErrorType SendErrorType
}

func NewSendError(msg string, failDatas []map[string]interface{}, eType SendErrorType) *SendError {
	se := SendError{
		msg:       msg,
		failDatas: failDatas,
		ErrorType: eType,
	}
	return &se
}

func NewRawSendError(msg string, failDatas []string, eType SendErrorType) *SendError {
	se := SendError{
		msg:       msg,
		failLines: failDatas,
		ErrorType: eType,
		isline:    true,
	}
	return &se
}

func (e *SendError) Error() string {
	return fmt.Sprintf("SendError: %v, failDatas size : %v", e.msg, len(e.failDatas))
}

func (e *SendError) GetFailDatas() []map[string]interface{} {
	return e.failDatas
}
