package tsdb

import (
	"bytes"
	"fmt"
	"io"
	"regexp"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

type TsdbToken struct {
	Token string `json:"-"`
}

const (
	seriesNamePattern = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
	repoNamePattern   = "^[a-zA-Z_][a-zA-Z0-9_]{0,127}$"
)

func validateSeriesName(seriesName string) (err error) {
	matched, err := regexp.MatchString(seriesNamePattern, seriesName)
	if err != nil {
		return reqerr.NewInvalidArgs("SeriesName", err.Error())
	}
	if !matched {
		return reqerr.NewInvalidArgs("SeriesName", fmt.Sprintf("invalid series name: %s", seriesName))
	}
	return
}

func validateRepoName(repoName string) (err error) {
	matched, err := regexp.MatchString(repoNamePattern, repoName)
	if err != nil {
		return reqerr.NewInvalidArgs("RepoName", err.Error())
	}
	if !matched {
		return reqerr.NewInvalidArgs("RepoName", fmt.Sprintf("invalid repo name: %s", repoName))
	}
	return
}

//repo related
type CreateRepoInput struct {
	TsdbToken
	RepoName string
	Region   string            `json:"region"`
	Metadata map[string]string `json:"metadata"`
}

func (r *CreateRepoInput) Validate() (err error) {
	if err = validateRepoName(r.RepoName); err != nil {
		return
	}

	if r.Region == "" {
		return reqerr.NewInvalidArgs("Region", "region should not be empty")
	}
	return
}

type GetRepoInput struct {
	TsdbToken
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
	TsdbToken
}

type ListReposOutput []RepoDesc

type DeleteRepoInput struct {
	TsdbToken
	RepoName string
}

type UpdateRepoMetadataInput struct {
	TsdbToken
	RepoName string
	Metadata map[string]string `json:"metadata"`
}

func (r *UpdateRepoMetadataInput) Validate() (err error) {
	if r.Metadata == nil {
		return reqerr.NewInvalidArgs("Metadata", "metadata should not be empty")
	}
	return
}

type DeleteRepoMetadataInput struct {
	TsdbToken
	RepoName string
}

//series related
type CreateSeriesInput struct {
	TsdbToken
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
	TsdbToken
	RepoName   string
	SeriesName string
	Metadata   map[string]string `json:"metadata"`
}

func (s *UpdateSeriesMetadataInput) Validate() (err error) {
	if s.Metadata == nil {
		return reqerr.NewInvalidArgs("Metadata", "metadata should not be empty")
	}
	return
}

type DeleteSeriesMetadataInput struct {
	TsdbToken
	RepoName   string
	SeriesName string
}

type ListSeriesInput struct {
	TsdbToken
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
	TsdbToken
}

//view related
type CreateViewInput struct {
	TsdbToken
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
	TsdbToken
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
	TsdbToken
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
	TsdbToken
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
	TsdbToken
	RepoName string
	Points   Points
}

type PostPointsFromFileInput struct {
	TsdbToken
	RepoName string
	FilePath string
}

type PostPointsFromReaderInput struct {
	TsdbToken
	RepoName string
	Reader   io.ReadSeeker
}

type PostPointsFromBytesInput struct {
	TsdbToken
	RepoName string
	Buffer   []byte
}

// query related
type QueryInput struct {
	TsdbToken
	RepoName string
	Sql      string `json:"sql"`
}

func (q *QueryInput) Validate() (err error) {
	if q.Sql == "" {
		return reqerr.NewInvalidArgs("QueryInput", "sql should not be empty")
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
