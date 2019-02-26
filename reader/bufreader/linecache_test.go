package bufreader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinecahe(t *testing.T) {
	l := NewLineCache()
	l.Append("abc")
	assert.Equal(t, 3, l.TotalLen())
	l.Append("456")
	assert.Equal(t, 6, l.TotalLen())
	assert.Equal(t, 2, l.Size())
	assert.Equal(t, "abc456", string(l.Combine()))
	l.Set([]string{"bbb"})
	assert.Equal(t, 3, l.TotalLen())
	l.Append("haha")
	assert.Equal(t, 7, l.TotalLen())
	assert.Equal(t, 2, l.Size())
	assert.Equal(t, "bbbhaha", string(l.Combine()))
}
