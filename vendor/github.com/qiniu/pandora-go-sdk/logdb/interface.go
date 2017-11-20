package logdb

import (
	"github.com/qiniu/pandora-go-sdk/base"
)

type LogdbAPI interface {
	CreateRepo(*CreateRepoInput) error

	GetRepo(*GetRepoInput) (*GetRepoOutput, error)

	ListRepos(*ListReposInput) (*ListReposOutput, error)

	DeleteRepo(*DeleteRepoInput) error

	UpdateRepo(*UpdateRepoInput) error

	SendLog(*SendLogInput) (*SendLogOutput, error)

	QueryLog(*QueryLogInput) (*QueryLogOutput, error)

	QueryScroll(*QueryScrollInput) (*QueryLogOutput, error)

	QueryHistogramLog(*QueryHistogramLogInput) (*QueryHistogramLogOutput, error)

	PutRepoConfig(*PutRepoConfigInput) error

	GetRepoConfig(*GetRepoConfigInput) (*GetRepoConfigOutput, error)

	MakeToken(*base.TokenDesc) (string, error)

	PartialQuery(input *PartialQueryInput) (output *PartialQueryOutput, err error)
}
