package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeepCopyByJSON(t *testing.T) {
	tests := []struct {
		src    map[string]interface{}
		dst    map[string]interface{}
		expect map[string]interface{}
	}{
		{
			src: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
			expect: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
		},
		{
			src:    nil,
			expect: nil,
		},
		{
			src: map[string]interface{}{
				"a": map[string]interface{}{"b": []interface{}{"c", "d", "e"}},
			},
			expect: map[string]interface{}{
				"a": map[string]interface{}{"b": []interface{}{"c", "d", "e"}},
			},
		},
	}

	for _, test := range tests {
		DeepCopyByJSON(&test.dst, &test.src)
		assert.Equal(t, len(test.expect), len(test.dst))
		for key, value := range test.expect {
			assert.Equal(t, value, test.dst[key])
		}
	}
}
