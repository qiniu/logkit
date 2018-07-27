package mgr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestRunnerStatusClone(t *testing.T) {
	// 所有类型的错误都有值
	{
		errQueue := NewErrorQueue(3)
		errQueue.Put(ErrorInfo{"this is a test error", time.Now().UnixNano(), 10})

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
			HistoryErrors: &ErrorsList{
				ReadErrors:  errQueue,
				ParseErrors: errQueue,
				TransformErrors: map[string]*ErrorQueue{
					"t1": errQueue,
				},
				SendErrors: map[string]*ErrorQueue{
					"s1": errQueue,
				},
			},
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
			HistoryErrors: &ErrorsList{
				ReadErrors:  errQueue,
				ParseErrors: errQueue,
				TransformErrors: map[string]*ErrorQueue{
					"t1": errQueue,
				},
				SendErrors: map[string]*ErrorQueue{
					"s1": errQueue,
				},
			},
		}
		got := rs.Clone()
		assert.Equal(t, exp, got)

		rs.ReaderStats.Success = 3
		rs.SenderStats["hah"] = StatsInfo{Success: 2}
		assert.Equal(t, exp, got)
	}

	// 部分类型的错误有值
	{
		errQueue := NewErrorQueue(3)
		errQueue.Put(ErrorInfo{"this is a test error", time.Now().UnixNano(), 10})

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
			HistoryErrors: &ErrorsList{
				ParseErrors: errQueue,
				SendErrors: map[string]*ErrorQueue{
					"s1": errQueue,
				},
			},
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
			HistoryErrors: &ErrorsList{
				ParseErrors: errQueue,
				SendErrors: map[string]*ErrorQueue{
					"s1": errQueue,
				},
			},
		}
		got := rs.Clone()
		assert.Equal(t, exp, got)

		rs.ReaderStats.Success = 3
		rs.SenderStats["hah"] = StatsInfo{Success: 2}
		assert.Equal(t, exp, got)
	}
}
