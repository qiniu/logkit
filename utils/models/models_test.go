package models

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_KeyValueSlice(t *testing.T) {
	testData := struct {
		origin KeyValueSlice
		expect KeyValueSlice
	}{
		origin: KeyValueSlice{
			{
				"test_start1",
				"",
				"a",
			},
			{
				"test_start2",
				"",
				"a",
			},
			{
				"kafka",
				"",
				"kafa",
			},
			{
				"test_final1",
				"",
				"",
			},
			{
				"test_final2",
				"",
				"",
			},
			{
				"kafkaNew",
				"",
				"kafaNew",
			},
		},
		expect: KeyValueSlice{
			{
				"test_final1",
				"",
				"",
			},
			{
				"test_final2",
				"",
				"",
			},
			{
				"test_start1",
				"",
				"a",
			},
			{
				"test_start2",
				"",
				"a",
			},
			{
				"kafka",
				"",
				"kafa",
			},
			{
				"kafkaNew",
				"",
				"kafaNew",
			},
		},
	}
	sort.Stable(testData.origin)
	assert.Equal(t, testData.expect, testData.origin)
}
