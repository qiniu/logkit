package pipeline

import (
	"fmt"

	"strings"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const errCodePrefixLen = 6

type PipelineErrBuilder struct{}

func (e PipelineErrBuilder) Build(msg, text, reqId string, code int) error {

	err := reqerr.New(msg, text, reqId, code)
	if len(msg) <= errCodePrefixLen {
		return err
	}
	errId := msg[:errCodePrefixLen]
	if strings.Contains(errId, ":") {
		spls := strings.Split(errId, ":")
		if len(spls) > 0 {
			errId = spls[0]
		}
	}
	switch errId {
	case "E18005":
		err.ErrorType = reqerr.EntityTooLargeError
	case "E18016":
		err.ErrorType = reqerr.ErrInvalidVariableType
	case "E18017":
		err.ErrorType = reqerr.ErrInvalidVariableName
	case "E18018":
		err.ErrorType = reqerr.ErrInvalidVariableValue
	case "E18019":
		err.ErrorType = reqerr.ErrPathFilter
	case "E18120":
		err.ErrorType = reqerr.NoSuchGroupError
	case "E18218":
		err.ErrorType = reqerr.GroupAlreadyExistsError
	case "E18102":
		err.ErrorType = reqerr.NoSuchRepoError
	case "E18101":
		err.ErrorType = reqerr.RepoAlreadyExistsError
	case "E18134":
		err.ErrorType = reqerr.ErrTooManySchema
	case "E18135":
		err.ErrorType = reqerr.ErrSchemaLimitUnderflow
	case "E18202":
		err.ErrorType = reqerr.NoSuchTransformError
	case "E18201":
		err.ErrorType = reqerr.TransformAlreadyExistsError
	case "E18302":
		err.ErrorType = reqerr.NoSuchExportError
	case "E18301":
		err.ErrorType = reqerr.ExportAlreadyExistsError
	case "E18216":
		err.ErrorType = reqerr.NoSuchPluginError
	case "E18217":
		err.ErrorType = reqerr.PluginAlreadyExistsError
	case "E18124":
		err.ErrorType = reqerr.RepoInCreatingError
	case "E18112":
		err.ErrorType = reqerr.RepoCascadingError
	case "E18207", "E18210":
		err.ErrorType = reqerr.InvalidTransformSpecError
	case "E18208":
		err.ErrorType = reqerr.ErrInvalidTransformInterval
	case "E18209":
		err.ErrorType = reqerr.ErrInvalidTransformSql
	case "E18211":
		err.ErrorType = reqerr.ErrInvalidTransformPluginOutput
	case "E18228":
		err.ErrorType = reqerr.ErrInvalidFieldInSQL
	case "E18104":
		err.ErrorType = reqerr.ErrDuplicateField
	case "E18107":
		err.ErrorType = reqerr.ErrUnsupportedFieldType
	case "E18125", "E18123", "E18111", "E18110":
		err.ErrorType = reqerr.InvalidDataSchemaError
	case "E18128":
		err.ErrorType = reqerr.ErrIncompatibleRepoSchema
	case "E18305":
		err.ErrorType = reqerr.InvalidExportSpecError
	case "E18600":
		err.ErrorType = reqerr.ErrInvalidDataSourceName
	case "E18601":
		err.ErrorType = reqerr.ErrDataSourceExist
	case "E18602":
		err.ErrorType = reqerr.ErrDataSourceNotExist
	case "E18603":
		err.ErrorType = reqerr.ErrDataSourceCascading
	case "E18604":
		err.ErrorType = reqerr.ErrInvalidJobName
	case "E18605":
		err.ErrorType = reqerr.ErrJobExist
	case "E18606":
		err.ErrorType = reqerr.ErrJobNotExist
	case "E18607":
		err.ErrorType = reqerr.ErrJobArgumentCount
	case "E18608":
		err.ErrorType = reqerr.ErrJobCascading
	case "E18609":
		err.ErrorType = reqerr.ErrInvalidJobExportName
	case "E18610":
		err.ErrorType = reqerr.ErrJobExportExist
	case "E18611":
		err.ErrorType = reqerr.ErrJobExportNotExist
	case "E18612":
		err.ErrorType = reqerr.ErrJobSrcNotExist
	case "E18613":
		err.ErrorType = reqerr.ErrDuplicateTableName
	case "E18614":
		err.ErrorType = reqerr.ErrInvalidBatchSpec
	case "E18615":
		err.ErrorType = reqerr.ErrIncompatibleSourceSchema
	case "E18617":
		err.ErrorType = reqerr.ErrInvalidTransformPlugin
	case "E18618":
		err.ErrorType = reqerr.ErrInvalidJobSQL
	case "E18619":
		err.ErrorType = reqerr.ErrBucketNotExist
	case "E18620":
		err.ErrorType = reqerr.ErrDatasourceNoFiles
	case "E18621":
		err.ErrorType = reqerr.ErrStartJob
	case "E18622":
		err.ErrorType = reqerr.ErrStopJob
	case "E18623":
		err.ErrorType = reqerr.ErrFileFormatMismatch
	case "E18624":
		err.ErrorType = reqerr.ErrJobRunIdNotExist
	case "E18625":
		err.ErrorType = reqerr.ErrBatchCannotRerun
	case "E18626":
		err.ErrorType = reqerr.ErrBatchStatusCannotStop
	case "E18627":
		err.ErrorType = reqerr.ErrUdfJarNotExist
	case "E18628":
		err.ErrorType = reqerr.ErrInvalidUdfJarName
	case "E18629":
		err.ErrorType = reqerr.ErrInvalidUdfFuncName
	case "E18630":
		err.ErrorType = reqerr.ErrInvalidJavaClassName
	case "E18306":
		err.ErrorType = reqerr.ErrStartExport
	case "E18307":
		err.ErrorType = reqerr.ErrStopExport
	case "E18631":
		err.ErrorType = reqerr.ErrUdfClassTypeError
	case "E18632":
		err.ErrorType = reqerr.ErrUdfClassNotFound
	case "E18633":
		err.ErrorType = reqerr.ErrUdfFunctionNotImplement
	case "E18634":
		err.ErrorType = reqerr.ErrUdfFunctionNotFound
	case "E18635":
		err.ErrorType = reqerr.ErrUdfFuncExisted
	case "E18636":
		err.ErrorType = reqerr.ErrUdfJarExisted
	case "E18637":
		err.ErrorType = reqerr.ErrDuplicationWithSystemFunc
	case "E18638":
		err.ErrorType = reqerr.ErrIllegalCharacterInPath
	case "E18229":
		err.ErrorType = reqerr.ErrInvalidDstRepoSchema
	case "E18230":
		err.ErrorType = reqerr.ErrInvalidDstRepoSchemaLength
	case "E18639":
		err.ErrorType = reqerr.ErrInvalidWorkflowName
	case "E18640":
		err.ErrorType = reqerr.ErrWorkflowAlreadyExists
	case "E18641":
		err.ErrorType = reqerr.ErrNoSuchWorkflow
	case "E18642":
		err.ErrorType = reqerr.ErrWorkflowSpecContent
	case "E18643":
		err.ErrorType = reqerr.ErrUpdateWorkflow
	case "E18644":
		err.ErrorType = reqerr.ErrStartWorkflow
	case "E18645":
		err.ErrorType = reqerr.ErrStopWorkflow
	case "E18646":
		err.ErrorType = reqerr.ErrWorkflowStructure
	case "E18647":
		err.ErrorType = reqerr.ErrStartTransform
	case "E18648":
		err.ErrorType = reqerr.ErrStopTransform
	case "E18649":
		err.ErrorType = reqerr.ErrBatchStatusCannotRerun
	case "E18650":
		err.ErrorType = reqerr.ErrNoExecutableJob
	case "E18651":
		err.ErrorType = reqerr.ErrJobExportSpec
	case "E18652":
		err.ErrorType = reqerr.ErrWorkflowCreatingTooManyRepos
	case "E18653":
		err.ErrorType = reqerr.ErrWorkflowJobsCoexist
	case "E18654":
		err.ErrorType = reqerr.ErrVariableNotExist
	case "E18655":
		err.ErrorType = reqerr.ErrVariableAlreadyExist
	case "E18656":
		err.ErrorType = reqerr.ErrSameToSystemVariable
	case "E18657":
		err.ErrorType = reqerr.ErrSQLWithUndefinedVariable
	case "E18658":
		err.ErrorType = reqerr.ErrTimeFormatInvalid
	case "E18660":
		err.ErrorType = reqerr.ErrTransformUpdate
	case "E18661":
		err.ErrorType = reqerr.ErrWorkflowNameSameToRepoOrDatasource
	case "E18662":
		err.ErrorType = reqerr.ErrJobReRunOrCancel
	case "E18663":
		err.ErrorType = reqerr.ErrStartOrStopBatchJob
	case "E18664":
		err.ErrorType = reqerr.ErrNoSuchResourceOwner
	case "E18665":
		err.ErrorType = reqerr.ErrAccessDenied
	case "E18703":
		err.ErrorType = reqerr.ErrTransformRepeatRestart
	case "E18704":
		err.ErrorType = reqerr.ErrFusionPathUsedStringVariable
	case "E18705":
		err.ErrorType = reqerr.ErrFusionPathWithUndefinedVariable
	case "E9000":
		err.ErrorType = reqerr.InternalServerError
	case "E9001":
		err.ErrorType = reqerr.NotImplementedError
		err.Message = fmt.Sprintf("this function is not implemented on server, ask server admin for explain: %s", msg)
	default:
		if code == 401 {
			err.Message = fmt.Sprintf("unauthorized: %v. 1. Please check your qiniu access_key and secret_key are both correct and you're authorized qiniu pandora user. 2. Please check the local time to ensure the consistent with the server time. 3. If you are using the token, please make sure that token has not expired.", msg)
			err.ErrorType = reqerr.UnauthorizedError
		}
	}
	return err

}
