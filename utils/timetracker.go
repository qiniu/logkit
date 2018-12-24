package utils

import (
	"fmt"
	"time"
)

type Stage struct {
	Duration time.Duration
	Note     string
}

type Tracker struct {
	Start  time.Time
	Stages []Stage
}

func NewTracker() *Tracker {
	return &Tracker{
		Start:  time.Now(),
		Stages: make([]Stage, 0, 4), //预分配四个空间
	}
}

func (t *Tracker) Track(stage string) {
	t.Stages = append(t.Stages, Stage{
		Note:     stage,
		Duration: time.Now().Sub(t.Start),
	})
}

func (t *Tracker) Print() string {
	t.Track("printed")
	ret := "Tracker started at: " + t.Start.Format(time.RFC3339Nano) + "\n"
	for i, v := range t.Stages {
		ret += fmt.Sprintf("after %v stage %s, it used %s times\n", i, v.Note, v.Duration.String())
	}
	return ret
}

func (t *Tracker) Reset() {
	t.Start = time.Now()
	t.Stages = make([]Stage, 0, len(t.Stages))
}
