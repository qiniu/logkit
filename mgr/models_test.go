package mgr

import (
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestRSClone(t *testing.T) {
	rs := &RunnerStatus{
		Name: "nihao",
		ReaderStats: StatsInfo{
			Success: 2,
		},
		SenderStats: map[string]StatsInfo{
			"nn": {
				Success: 3,
			},
		},
		TransformStats: map[string]StatsInfo{},
		Url:            "abc",
	}
	exp := RunnerStatus{
		Name: "nihao",
		ReaderStats: StatsInfo{
			Success: 2,
		},
		SenderStats: map[string]StatsInfo{
			"nn": {
				Success: 3,
			},
		},
		TransformStats: map[string]StatsInfo{},
		Url:            "abc",
	}
	got := rs.Clone()
	assert.Equal(t, exp, got)

	rs.ReaderStats.Success = 3
	rs.SenderStats["hah"] = StatsInfo{Success: 2}
	assert.Equal(t, exp, got)
}
