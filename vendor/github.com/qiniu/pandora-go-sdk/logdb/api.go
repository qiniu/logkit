package logdb

import (
	"net/url"

	"github.com/qiniu/pandora-go-sdk/base"
)

func (c *Logdb) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.newOperation(base.OpCreateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Logdb) CreateRepoFromDSL(input *CreateRepoDSLInput) (err error) {
	schemas, err := toSchema(input.DSL, 0)
	if err != nil {
		return
	}
	return c.CreateRepo(&CreateRepoInput{
		LogdbToken: input.LogdbToken,
		RepoName:   input.RepoName,
		Region:     input.Region,
		Retention:  input.Retention,
		Schema:     schemas,
	})
}

func (c *Logdb) UpdateRepo(input *UpdateRepoInput) (err error) {
	op := c.newOperation(base.OpUpdateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Logdb) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.newOperation(base.OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Logdb) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	op := c.newOperation(base.OpListRepos)

	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Logdb) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.newOperation(base.OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Logdb) SendLog(input *SendLogInput) (output *SendLogOutput, err error) {
	op := c.newOperation(base.OpSendLog, input.RepoName, input.OmitInvalidLog)

	output = &SendLogOutput{}
	req := c.newRequest(op, input.Token, &output)
	buf, err := input.Logs.Buf()
	if err != nil {
		return
	}
	req.SetBufferBody(buf)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return output, req.Send()
}

func (c *Logdb) QueryLog(input *QueryLogInput) (output *QueryLogOutput, err error) {
	var highlight bool
	if input.Highlight != nil {
		highlight = true
	}
	op := c.newOperation(base.OpQueryLog, input.RepoName, url.QueryEscape(input.Query), input.Sort, input.From, input.Size, input.Scroll, highlight)

	output = &QueryLogOutput{}
	req := c.newRequest(op, input.Token, output)
	if input.Highlight != nil {
		if err = req.SetVariantBody(input.Highlight); err != nil {
			return
		}
		req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	}
	return output, req.Send()
}

func (c *Logdb) QueryScroll(input *QueryScrollInput) (output *QueryLogOutput, err error) {
	op := c.newOperation(base.OpQueryScroll, input.RepoName)
	output = &QueryLogOutput{}
	req := c.newRequest(op, input.Token, output)
	buf, err := input.Buf()
	if err != nil {
		return
	}
	req.SetBufferBody(buf)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return output, req.Send()
}

func (c *Logdb) QueryHistogramLog(input *QueryHistogramLogInput) (output *QueryHistogramLogOutput, err error) {
	op := c.newOperation(base.OpQueryHistogramLog, input.RepoName, url.QueryEscape(input.Query), input.From, input.To, input.Field)

	output = &QueryHistogramLogOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Logdb) PutRepoConfig(input *PutRepoConfigInput) (err error) {
	op := c.newOperation(base.OpPutRepoConfig, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return req.Send()
}

func (c *Logdb) GetRepoConfig(input *GetRepoConfigInput) (output *GetRepoConfigOutput, err error) {
	op := c.newOperation(base.OpGetRepoConfig, input.RepoName)

	output = &GetRepoConfigOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Logdb) MakeToken(desc *base.TokenDesc) (string, error) {
	return base.MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}

func (c *Logdb) PartialQuery(input *PartialQueryInput) (output *PartialQueryOutput, err error) {
	op := c.newOperation(base.OpPartialQuery, input.RepoName)
	output = &PartialQueryOutput{}
	req := c.newRequest(op, input.Token, output)
	buf, err := input.Buf()
	if err != nil {
		return
	}
	req.SetBufferBody(buf)
	req.SetHeader(base.HTTPHeaderContentType, base.ContentTypeJson)
	return output, req.Send()
}
