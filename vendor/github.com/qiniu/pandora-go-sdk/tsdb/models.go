package tsdb

import (
	"bytes"
	"fmt"
	"io"
	"regexp"

	. "github.com/qiniu/pandora-go-sdk/base/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

const (
	seriesNamePattern = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	repoNamePattern   = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
)

func validateSeriesName(seriesName string) (err error) {
	matched, err := regexp.MatchString(seriesNamePattern, seriesName)
	if err != nil {
		return reqerr.NewInvalidArgs("SeriesName", err.Error()).WithComponent("tsdb")
	}
	if !matched {
		return reqerr.NewInvalidArgs("SeriesName", fmt.Sprintf("invalid series name: %s", seriesName)).WithComponent("tsdb")
	}
	return
}

func validateRepoName(repoName string) (err error) {
	matched, err := regexp.MatchString(repoNamePattern, repoName)
	if err != nil {
		return reqerr.NewInvalidArgs("RepoName", err.Error()).WithComponent("tsdb")
	}
	if !matched {
		return reqerr.NewInvalidArgs("RepoName", fmt.Sprintf("invalid repo name: %s", repoName)).WithComponent("tsdb")
	}
	return
}

//repo related
type CreateRepoInput struct {
	PandoraToken
	RepoName string
	Region   string            `json:"region"`
	Metadata map[string]string `json:"metadata"`
}

func (r *CreateRepoInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}

	if r.Region == "" {
		return reqerr.NewInvalidArgs("Region", "region should not be empty").WithComponent("tsdb")
	}
	return
}

type GetRepoInput struct {
	PandoraToken
	RepoName string
}

type GetRepoOutput struct {
	RepoName   string            `json:"name"`
	Region     string            `json:"region"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreateTime string            `json:"createTime"`
	Deleting   string            `json:"deleting"`
}

type RepoDesc struct {
	RepoName   string            `json:"name"`
	Region     string            `json:"region"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreateTime string            `json:"createTime"`
	Deleting   string            `json:"deleting"`
}

type ListReposInput struct {
	PandoraToken
}

type ListReposOutput []RepoDesc

type DeleteRepoInput struct {
	PandoraToken
	RepoName string
}

type UpdateRepoMetadataInput struct {
	PandoraToken
	RepoName string
	Metadata map[string]string `json:"metadata"`
}

func (r *UpdateRepoMetadataInput) Validate() (err error) {
	if r.Metadata == nil {
		return reqerr.NewInvalidArgs("Metadata", "metadata should not be empty").WithComponent("tsdb")
	}
	return
}

type DeleteRepoMetadataInput struct {
	PandoraToken
	RepoName string
}

//series related
type CreateSeriesInput struct {
	PandoraToken
	RepoName   string
	SeriesName string
	Retention  string            `json:"retention"`
	Metadata   map[string]string `json:"metadata"`
}

func (s *CreateSeriesInput) Validate() (err error) {
	if err = validateSeriesName(s.SeriesName); err != nil {
		return
	}

	return
}

type UpdateSeriesMetadataInput struct {
	PandoraToken
	RepoName   string
	SeriesName string
	Metadata   map[string]string `json:"metadata"`
}

func (s *UpdateSeriesMetadataInput) Validate() (err error) {
	if s.Metadata == nil {
		return reqerr.NewInvalidArgs("Metadata", "metadata should not be empty").WithComponent("tsdb")
	}
	return
}

type DeleteSeriesMetadataInput struct {
	PandoraToken
	RepoName   string
	SeriesName string
}

type ListSeriesInput struct {
	PandoraToken
	RepoName string
	ShowMeta bool
}

type SeriesDesc struct {
	Name       string            `json:"name"`
	Retention  string            `json:"retention"`
	Metadata   map[string]string `json:"metadata"`
	CreateTime string            `json:"createTime"`
	Type       string            `json:"type"`
	Deleting   string            `json:"deleting"`
}

type ListSeriesOutput []SeriesDesc

type DeleteSeriesInput struct {
	RepoName   string
	SeriesName string
	PandoraToken
}

//view related
type CreateViewInput struct {
	PandoraToken
	RepoName  string
	ViewName  string
	Sql       string            `json:"sql"`
	Retention string            `json:"retention"`
	Metadata  map[string]string `json:"metadata"`
}

func (v *CreateViewInput) Validate() (err error) {
	return
}

type ListViewInput struct {
	PandoraToken
	RepoName string
}

type ListViewOutput []ViewDesc

type ViewDesc struct {
	Name       string `json:"name"`
	Retention  string `json:"retention"`
	CreateTime string `json:"createTime"`
	Deleting   string `json:"deleting"`
}

type GetViewInput struct {
	PandoraToken
	RepoName string
	ViewName string
}

type GetViewOutput struct {
	Retention  string            `json:"retention"`
	Sql        string            `json:"sql"`
	Metadata   map[string]string `json:"metadata"`
	CreateTime string            `json:"createTime"`
	Deleting   string            `json:"deleting"`
}

type DeleteViewInput struct {
	PandoraToken
	RepoName string
	ViewName string
}

//write points related

type Point struct {
	SeriesName string
	Tags       map[string]string
	Fields     map[string]interface{}
	Time       uint64
}

type Points []Point

func (ps Points) Buffer() []byte {
	var buf bytes.Buffer
	for _, p := range ps {
		buf.WriteString(p.String())
		buf.WriteByte('\n')
	}
	if len(ps) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	return buf.Bytes()
}

type PostPointsInput struct {
	PandoraToken
	RepoName string
	Points   Points
}

type PostPointsFromFileInput struct {
	PandoraToken
	RepoName string
	FilePath string
}

type PostPointsFromReaderInput struct {
	PandoraToken
	RepoName string
	Reader   io.ReadSeeker
}

type PostPointsFromBytesInput struct {
	PandoraToken
	RepoName string
	Buffer   []byte
}

// query related
type QueryInput struct {
	PandoraToken
	RepoName string
	Sql      string `json:"sql"`
}

func (q *QueryInput) Validate() (err error) {
	if q.Sql == "" {
		return reqerr.NewInvalidArgs("QueryInput", "sql should not be empty").WithComponent("tsdb")
	}
	return
}

type QueryOutput struct {
	Results []Result `json:"results,omitempty"`
}

type Result struct {
	Series []Serie `json:"series,omitempty"`
}

type Serie struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Error   error             `json:"err,omitempty"`
}
