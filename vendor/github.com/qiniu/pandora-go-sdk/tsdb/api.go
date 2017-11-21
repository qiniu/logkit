package tsdb

import (
	"os"

	. "github.com/qiniu/pandora-go-sdk/base"
)

func (c *Tsdb) CreateRepo(input *CreateRepoInput) (err error) {
	op := c.newOperation(OpCreateRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Tsdb) GetRepo(input *GetRepoInput) (output *GetRepoOutput, err error) {
	op := c.newOperation(OpGetRepo, input.RepoName)

	output = &GetRepoOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Tsdb) ListRepos(input *ListReposInput) (output *ListReposOutput, err error) {
	op := c.newOperation(OpListRepos)

	output = &ListReposOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Tsdb) UpdateRepoMetadata(input *UpdateRepoMetadataInput) (err error) {
	op := c.newOperation(OpUpdateRepoMetadata, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Tsdb) DeleteRepoMetadata(input *DeleteRepoMetadataInput) (err error) {
	op := c.newOperation(OpDeleteRepoMetadata, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Tsdb) DeleteRepo(input *DeleteRepoInput) (err error) {
	op := c.newOperation(OpDeleteRepo, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Tsdb) CreateSeries(input *CreateSeriesInput) (err error) {
	op := c.newOperation(OpCreateSeries, input.RepoName, input.SeriesName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Tsdb) ListSeries(input *ListSeriesInput) (output *ListSeriesOutput, err error) {
	op := c.newOperation(OpListSeries, input.RepoName)

	output = &ListSeriesOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()

}

func (c *Tsdb) UpdateSeriesMetadata(input *UpdateSeriesMetadataInput) (err error) {
	op := c.newOperation(OpUpdateSeriesMetadata, input.RepoName, input.SeriesName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Tsdb) DeleteSeriesMetadata(input *DeleteSeriesMetadataInput) (err error) {
	op := c.newOperation(OpDeleteSeriesMetadata, input.RepoName, input.SeriesName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Tsdb) DeleteSeries(input *DeleteSeriesInput) (err error) {
	op := c.newOperation(OpDeleteSeries, input.RepoName, input.SeriesName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Tsdb) CreateView(input *CreateViewInput) (err error) {
	op := c.newOperation(OpCreateView, input.RepoName, input.ViewName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Tsdb) ListView(input *ListViewInput) (output *ListViewOutput, err error) {
	op := c.newOperation(OpListView, input.RepoName)

	output = &ListViewOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Tsdb) GetView(input *GetViewInput) (output *GetViewOutput, err error) {
	op := c.newOperation(OpGetView, input.RepoName, input.ViewName)

	output = &GetViewOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()
}

func (c *Tsdb) DeleteView(input *DeleteViewInput) (err error) {
	op := c.newOperation(OpDeleteView, input.RepoName, input.ViewName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Tsdb) PostPoints(input *PostPointsInput) (err error) {
	op := c.newOperation(OpWritePoints, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Points.Buffer())
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Tsdb) QueryPoints(input *QueryInput) (output *QueryOutput, err error) {
	op := c.newOperation(OpQueryPoints, input.RepoName)

	output = &QueryOutput{}

	req := c.newRequest(op, input.Token, output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)

	return output, req.Send()
}

func (c *Tsdb) PostPointsFromFile(input *PostPointsFromFileInput) (err error) {
	op := c.newOperation(OpWritePoints, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	file, err := os.Open(input.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	req.SetReaderBody(file)
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Tsdb) PostPointsFromReader(input *PostPointsFromReaderInput) (err error) {
	op := c.newOperation(OpWritePoints, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetReaderBody(input.Reader)
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Tsdb) PostPointsFromBytes(input *PostPointsFromBytesInput) (err error) {
	op := c.newOperation(OpWritePoints, input.RepoName)

	req := c.newRequest(op, input.Token, nil)
	req.SetBufferBody(input.Buffer)
	req.SetHeader(HTTPHeaderContentType, ContentTypeText)
	return req.Send()
}

func (c *Tsdb) MakeToken(desc *TokenDesc) (string, error) {
	return MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}
