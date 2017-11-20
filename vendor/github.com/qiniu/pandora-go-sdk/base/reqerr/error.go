package reqerr

import "fmt"

const (
	DefaultRequestError int = iota
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
	ErrVariableNotExist
	ErrVariableAlreadyExist
	ErrSameToSystemVariable
)

type ErrBuilder interface {
	Build(message, rawText, reqId string, statusCode int) error
}

func NewInvalidArgs(name, message string) *RequestError {
	return &RequestError{
		Message:   fmt.Sprintf("Invalid args, argName: %s, reason: %s", name, message),
		ErrorType: InvalidArgs,
	}
}

type RequestError struct {
	Message    string `json:"error"`
	StatusCode int    `json:"-"`
	RequestId  string `json:"-"`
	RawMessage string `json:"-"`
	ErrorType  int    `json:"-"`
}

func New(message, rawText, reqId string, statusCode int) *RequestError {
	return &RequestError{
		Message:    message,
		StatusCode: statusCode,
		RequestId:  reqId,
		RawMessage: rawText,
		ErrorType:  DefaultRequestError,
	}
}

func (r RequestError) Error() string {
	return fmt.Sprintf("pandora error: StatusCode=%d, ErrorMessage=%s, RequestId=%s", r.StatusCode, r.Message, r.RequestId)
}

func IsExistError(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == RepoAlreadyExistsError || reqErr.ErrorType == SeriesAlreadyExistsError {
		return true
	}
	return false
}

func IsNoSuchResourceError(err error) bool {
	reqErr, ok := err.(*RequestError)
	if !ok {
		return false
	}
	if reqErr.ErrorType == NoSuchRepoError {
		return true
	}
	if reqErr.ErrorType == NoSuchExportError {
		return true
	}
	if reqErr.ErrorType == NoSuchSeriesError {
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
)

type SendError struct {
	failDatas []map[string]interface{}
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

func (e *SendError) Error() string {
	return fmt.Sprintf("SendError: %v, failDatas size : %v", e.msg, len(e.failDatas))
}

func (e *SendError) GetFailDatas() []map[string]interface{} {
	return e.failDatas
}
