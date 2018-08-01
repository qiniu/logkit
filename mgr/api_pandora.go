package mgr

import (
	"net/http"
	"sort"

	"github.com/labstack/echo"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/pipeline"

	. "github.com/qiniu/logkit/utils/models"
)

// GET /pandora/repos?ak=<accessKey>&sk=<secretKey>&pipeline=<pipelineHost>
func (rs *RestService) GetPandoraRepos() echo.HandlerFunc {
	return func(c echo.Context) error {
		var ak, sk, pipelineHost string
		ak = c.QueryParam("ak")
		sk = c.QueryParam("sk")
		pipelineHost = c.QueryParam("pipeline")
		if ak == "" || sk == "" {
			return RespError(c, http.StatusBadRequest, ErrRunnerAdd, "ak or sk is empty")
		}
		if pipelineHost == "" {
			pipelineHost = config.DefaultPipelineEndpoint
		}

		config := pipeline.NewConfig().WithAccessKeySecretKey(ak, sk)
		err := base.CheckEndPoint(pipelineHost)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrPandoraPipelineConfig, "check params pipeline host err: "+err.Error())
		}
		config = config.WithEndpoint(pipelineHost)

		pAPI, err := pipeline.New(config)
		if err != nil {
			return RespError(c, http.StatusInternalServerError, ErrPandoraPipelineAPI, "new pipeline API client err: "+err.Error())
		}
		defer pAPI.Close()
		reposOut, err := pAPI.ListRepos(&pipeline.ListReposInput{WithDag: true})
		if err != nil {
			return RespError(c, http.StatusInternalServerError, ErrPandoraPipelineRepos, "list repos err: "+err.Error())
		}

		ret := make([]PandoraRepo, len(reposOut.Repos))
		for idx, v := range reposOut.Repos {
			p := PandoraRepo{
				Pipeline: v.Workflow,
				Repo:     v.RepoName,
			}
			if v.RuleNames != nil {
				for _, r := range *v.RuleNames {
					p.Rules = append(p.Rules, r)
				}
			}
			ret[idx] = p
		}
		return RespSuccess(c, ret)
	}
}

// GET /pandora/workflows?ak=<accessKey>&sk=<secretKey>&pipeline=<pipelineHost>
func (rs *RestService) GetPandoraWorkflows() echo.HandlerFunc {
	return func(c echo.Context) error {
		var ak, sk, pipelineHost string
		ak = c.QueryParam("ak")
		sk = c.QueryParam("sk")
		pipelineHost = c.QueryParam("pipeline")
		if ak == "" || sk == "" {
			return RespError(c, http.StatusBadRequest, ErrRunnerAdd, "ak or sk is empty")
		}
		if pipelineHost == "" {
			pipelineHost = config.DefaultPipelineEndpoint
		}

		config := pipeline.NewConfig().WithAccessKeySecretKey(ak, sk)
		err := base.CheckEndPoint(pipelineHost)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrPandoraPipelineConfig, "check params pipeline host err: "+err.Error())
		}
		config = config.WithEndpoint(pipelineHost)

		pAPI, err := pipeline.New(config)
		if err != nil {
			return RespError(c, http.StatusInternalServerError, ErrPandoraPipelineAPI, "new pipeline API client err: "+err.Error())
		}
		defer pAPI.Close()
		reposOut, err := pAPI.ListWorkflows(&pipeline.ListWorkflowInput{})
		if err != nil {
			return RespError(c, http.StatusInternalServerError, ErrPandoraPipelineWorkflows, "list workflows err: "+err.Error())
		}

		ret := make(PandoraWorkflowSlice, len(*reposOut))
		for idx, v := range *reposOut {
			ret[idx] = PandoraWorkflow{
				Name:       v.Name,
				Status:     v.Status,
				CreateTime: v.CreateTime,
			}
		}
		sort.Stable(ret)
		return RespSuccess(c, ret)
	}
}
