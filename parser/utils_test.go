package parser

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type cmdArgs struct {
	CmdArgs []string
}

func Test_UtilsTime(t *testing.T) {
	t1 := 1473680977
	t2 := 1473680977 + 24*60*60
	t3 := 1473680977 + 5*60

	s2, _ := strconv.Atoi(Time1Day(int64(t2)))
	s1, _ := strconv.Atoi(Time1Day(int64(t1)))

	s3, _ := strconv.Atoi(Time5Min(int64(t1)))
	s4, _ := strconv.Atoi(Time5Min(int64(t3)))

	if s1 != 1473638400 {
		t.Errorf("Time1Day err: t1: %v, s1: %v", t1, s1)
	}
	if s3 != 1473680700 {
		t.Errorf("Time5Min err: t1: %v, s3: %v", t1, s3)
	}
	if s2 != s1+86400 {
		t.Errorf("Time1Day err : t1: %v, s1: %v, t2: %v, s2: %v ", t1, s1, t2, s2)
	}
	if s4 != s3+300 {
		t.Errorf("Time5Min err: t1: %v, s3: %v, t3: %v, s4: %v", t1, s3, t3, s4)
	}
}

func Test_getLabels(t *testing.T) {
	tests := []struct {
		labelList []string
		nameLabel map[string]struct{}
		exp       []Label
	}{
		{
			labelList: []string{"a v", "x y"},
			nameLabel: map[string]struct{}{},
			exp:       []Label{{Name: "a", Value: "v"}, {Name: "x", Value: "y"}},
		},
		{
			labelList: []string{"a v", "x"},
			nameLabel: map[string]struct{}{},
			exp:       []Label{{Name: "a", Value: "v"}},
		},
		{
			labelList: []string{"a v", "x y"},
			nameLabel: map[string]struct{}{"x": struct{}{}},
			exp:       []Label{{Name: "a", Value: "v"}},
		},
	}
	for _, ti := range tests {
		labes := GetLabels(ti.labelList, ti.nameLabel)
		if !reflect.DeepEqual(labes, ti.exp) {
			t.Errorf("Test_getLabels error exp %v but got %v", ti.exp, labes)
		}
	}
}

func TestParseTimeZoneOffset(t *testing.T) {
	tests := []struct {
		s   string
		exp int
	}{
		{
			s:   "+08",
			exp: 8,
		},
		{
			s:   "+8",
			exp: 8,
		},
		{
			s:   "8",
			exp: 8,
		},
		{
			s:   "-8",
			exp: -8,
		},
		{
			s:   "-08",
			exp: -8,
		},
		{
			s:   "-1",
			exp: -1,
		},
		{
			s:   "0",
			exp: 0,
		},
	}
	for _, ti := range tests {
		got := ParseTimeZoneOffset(ti.s)
		assert.Equal(t, ti.exp, got)
	}
}
