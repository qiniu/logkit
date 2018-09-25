package mgr

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/utils/equeue"
	. "github.com/qiniu/logkit/utils/models"
)

func TestRunnerStatusClone(t *testing.T) {
	// 所有类型的错误都有值
	{
		errQueue1 := equeue.New(3)
		randinsert(errQueue1, 4)
		errQueue2 := equeue.New(3)
		randinsert(errQueue2, 4)
		errQueue3 := equeue.New(3)
		randinsert(errQueue2, 2)
		errQueue4 := equeue.New(3)
		randinsert(errQueue2, 0)

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
				ReadErrors:  errQueue1,
				ParseErrors: errQueue2,
				TransformErrors: map[string]*equeue.ErrorQueue{
					"t1": errQueue3,
				},
				SendErrors: map[string]*equeue.ErrorQueue{
					"s1": errQueue4,
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
				ReadErrors:  errQueue1,
				ParseErrors: errQueue2,
				TransformErrors: map[string]*equeue.ErrorQueue{
					"t1": errQueue3,
				},
				SendErrors: map[string]*equeue.ErrorQueue{
					"s1": errQueue4,
				},
			},
		}
		got := rs.Clone()
		assert.Equal(t, exp.Name, got.Name)
		assert.Equal(t, exp.ReaderStats, got.ReaderStats)
		assert.Equal(t, exp.SenderStats, got.SenderStats)
		assert.Equal(t, exp.TransformStats, got.TransformStats)
		assert.Equal(t, exp.Url, got.Url)
		assert.Equal(t, exp.HistoryErrors.ReadErrors.List(), got.HistoryErrors.ReadErrors.List())
		assert.Equal(t, exp.HistoryErrors.ParseErrors.List(), got.HistoryErrors.ParseErrors.List())
		assert.Equal(t, exp.HistoryErrors.TransformErrors["t1"].List(), got.HistoryErrors.TransformErrors["t1"].List())
		assert.Equal(t, exp.HistoryErrors.SendErrors["s1"].List(), got.HistoryErrors.SendErrors["s1"].List())

		rs.ReaderStats.Success = 3
		rs.SenderStats["hah"] = StatsInfo{Success: 2}

		assert.Equal(t, exp.Name, got.Name)
		assert.Equal(t, exp.ReaderStats, got.ReaderStats)
		assert.Equal(t, exp.SenderStats, got.SenderStats)
		assert.Equal(t, exp.TransformStats, got.TransformStats)
		assert.Equal(t, exp.Url, got.Url)
		assert.Equal(t, exp.HistoryErrors.ReadErrors.List(), got.HistoryErrors.ReadErrors.List())
		assert.Equal(t, exp.HistoryErrors.ParseErrors.List(), got.HistoryErrors.ParseErrors.List())
		assert.Equal(t, exp.HistoryErrors.TransformErrors["t1"].List(), got.HistoryErrors.TransformErrors["t1"].List())
		assert.Equal(t, exp.HistoryErrors.SendErrors["s1"].List(), got.HistoryErrors.SendErrors["s1"].List())
	}

	// 部分类型的错误有值
	{
		errQueue1 := equeue.New(3)
		randinsert(errQueue1, 4)
		errQueue2 := equeue.New(3)
		randinsert(errQueue2, 4)

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
				ParseErrors: errQueue1,
				SendErrors: map[string]*equeue.ErrorQueue{
					"s1": errQueue2,
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
				ParseErrors: errQueue1,
				SendErrors: map[string]*equeue.ErrorQueue{
					"s1": errQueue2,
				},
			},
		}
		got := rs.Clone()

		assert.Equal(t, exp.Name, got.Name)
		assert.Equal(t, exp.ReaderStats, got.ReaderStats)
		assert.Equal(t, exp.SenderStats, got.SenderStats)
		assert.Equal(t, exp.TransformStats, got.TransformStats)
		assert.Equal(t, exp.Url, got.Url)
		assert.Equal(t, exp.HistoryErrors.ReadErrors.List(), got.HistoryErrors.ReadErrors.List())
		assert.Equal(t, exp.HistoryErrors.ParseErrors.List(), got.HistoryErrors.ParseErrors.List())
		assert.Equal(t, exp.HistoryErrors.TransformErrors["t1"].List(), got.HistoryErrors.TransformErrors["t1"].List())
		assert.Equal(t, exp.HistoryErrors.SendErrors["s1"].List(), got.HistoryErrors.SendErrors["s1"].List())

		rs.ReaderStats.Success = 3
		rs.SenderStats["hah"] = StatsInfo{Success: 2}

		assert.Equal(t, exp.Name, got.Name)
		assert.Equal(t, exp.ReaderStats, got.ReaderStats)
		assert.Equal(t, exp.SenderStats, got.SenderStats)
		assert.Equal(t, exp.TransformStats, got.TransformStats)
		assert.Equal(t, exp.Url, got.Url)
		assert.Equal(t, exp.HistoryErrors.ReadErrors.List(), got.HistoryErrors.ReadErrors.List())
		assert.Equal(t, exp.HistoryErrors.ParseErrors.List(), got.HistoryErrors.ParseErrors.List())
		assert.Equal(t, exp.HistoryErrors.TransformErrors["t1"].List(), got.HistoryErrors.TransformErrors["t1"].List())
		assert.Equal(t, exp.HistoryErrors.SendErrors["s1"].List(), got.HistoryErrors.SendErrors["s1"].List())
	}
}

func TestErrList(t *testing.T) {
	el := ErrorsList{
		TransformErrors: make(map[string]*equeue.ErrorQueue),
		SendErrors:      make(map[string]*equeue.ErrorQueue),
	}
	tel := el.Clone()
	assert.Nil(t, tel)

	assert.Equal(t, true, el.Empty())
	sendName1 := "s1"
	el.SendErrors[sendName1] = equeue.New(2)
	assert.Equal(t, true, el.Empty())
	el.SendErrors[sendName1].Put(equeue.ErrorInfo{Error: "send1"})
	assert.Equal(t, false, el.Empty())
	assert.Equal(t, true, el.HasSendErr())

	assert.Equal(t, false, el.HasReadErr())
	el.ReadErrors = equeue.New(2)
	el.ReadErrors.Put(equeue.ErrorInfo{Error: "read1"})
	el.ReadErrors.Put(equeue.ErrorInfo{Error: "read2"})
	assert.Equal(t, true, el.HasReadErr())

	el.ParseErrors = equeue.New(2)
	assert.Equal(t, false, el.HasParseErr())
	el.ParseErrors.Put(equeue.ErrorInfo{Error: "parse1"})
	assert.Equal(t, true, el.HasParseErr())
	el.ParseErrors.Put(equeue.ErrorInfo{Error: "parse2"})
	el.ParseErrors.Put(equeue.ErrorInfo{Error: "parse3"})

	assert.Equal(t, false, el.HasTransformErr())
	transname := "t1"
	el.TransformErrors[transname] = equeue.New(2)
	el.TransformErrors[transname].Put(equeue.ErrorInfo{Error: "trans1"})
	assert.Equal(t, true, el.HasTransformErr())

	assert.Equal(t, false, el.Empty())
	nel := el.Clone()
	nel.SendErrors[sendName1].Put(equeue.ErrorInfo{Error: "send2"})
	nel.ReadErrors.Put(equeue.ErrorInfo{Error: "read3"})
	nel.ReadErrors.Put(equeue.ErrorInfo{Error: "read3"})
	assert.Equal(t, equeue.ErrorInfo{Error: "read3", Count: 2}, nel.ReadErrors.End())

	assert.Equal(t, ErrorsResult{
		ReadErrors:  []equeue.ErrorInfo{{Error: "read1", Count: 1}, {Error: "read2", Count: 1}},
		ParseErrors: []equeue.ErrorInfo{{Error: "parse2", Count: 1}, {Error: "parse3", Count: 1}},
		TransformErrors: map[string][]equeue.ErrorInfo{
			transname: {{Error: "trans1", Count: 1}},
		},
		SendErrors: map[string][]equeue.ErrorInfo{
			sendName1: {{Error: "send1", Count: 1}},
		},
	}, el.List())

	el.Reset()
	assert.Equal(t, true, el.Empty())
	rel := el.Clone()
	assert.Equal(t, true, rel.Empty())
}
