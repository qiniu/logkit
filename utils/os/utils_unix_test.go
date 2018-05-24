// +build darwin linux

package os

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var uid uint64

func BenchmarkTestGetIdentifyIDByPath(b *testing.B) {
	filename := "BenchmarkTestGetIdentifyIDByPath"
	err := ioutil.WriteFile(filename, []byte(`xsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxsxsxnsixssxsxsxsxsxsxsxsxs`), os.ModePerm)
	assert.NoError(b, err)
	defer os.Remove(filename)
	for i := 0; i < b.N; i++ {
		x, err := GetIdentifyIDByPath(filename)
		if err != nil {
			b.Fatal(err)
		}
		uid = x
	}
}
