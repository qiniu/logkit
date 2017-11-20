package tsdb

import (
	"github.com/qiniu/pandora-go-sdk/base"
)

type TsdbAPI interface {
	CreateRepo(*CreateRepoInput) error

	GetRepo(*GetRepoInput) (*GetRepoOutput, error)

	ListRepos(*ListReposInput) (*ListReposOutput, error)

	UpdateRepoMetadata(*UpdateRepoMetadataInput) error

	DeleteRepoMetadata(*DeleteRepoMetadataInput) error

	DeleteRepo(*DeleteRepoInput) error

	CreateSeries(*CreateSeriesInput) error

	ListSeries(*ListSeriesInput) (*ListSeriesOutput, error)

	UpdateSeriesMetadata(*UpdateSeriesMetadataInput) error

	DeleteSeriesMetadata(*DeleteSeriesMetadataInput) error

	DeleteSeries(*DeleteSeriesInput) error

	CreateView(*CreateViewInput) error

	ListView(*ListViewInput) (*ListViewOutput, error)

	GetView(*GetViewInput) (*GetViewOutput, error)

	DeleteView(*DeleteViewInput) error

	PostPoints(*PostPointsInput) error

	PostPointsFromFile(*PostPointsFromFileInput) error

	PostPointsFromReader(*PostPointsFromReaderInput) error

	PostPointsFromBytes(*PostPointsFromBytesInput) error

	QueryPoints(*QueryInput) (*QueryOutput, error)

	MakeToken(*base.TokenDesc) (string, error)
}
