package audit

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAudit(t *testing.T) {
	dir := "./TestAudit"
	audit, err := NewAuditLogger(dir, 5*1024)
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	for i := 0; i < 1000; i++ {
		audit.Log(Message{"haha", time.Now().UnixNano() / 1000000, rand.Int63n(100000000), rand.Int63n(100000), rand.Int63n(100000), "", 123})
		time.Sleep(time.Millisecond)
	}
	files, err := ioutil.ReadDir(dir)
	for _, f := range files {
		t.Log(f.Name(), " ", f.Size())
	}
	assert.NoError(t, err)
	assert.Equal(t, 9, len(files))
}

//200000	      8027 ns/op	     208 B/op	       1 allocs/op
func BenchmarkAudit(b *testing.B) {
	b.ReportAllocs()
	dir := "./BenchmarkAudit"
	auidt, err := NewAuditLogger(dir, 5*1024*1024)
	if err != nil {
		b.Error(err)
		return
	}
	defer os.RemoveAll(dir)
	for i := 0; i < b.N; i++ {
		auidt.Log(Message{"haha", time.Now().UnixNano() / 1000000, 123239232, 123343, 13201200, "", 123})
	}
}

func TestIota(t *testing.T) {
	buf := bytes.Buffer{}
	itoa(&buf, 10)
	assert.Equal(t, "10", string(buf.Bytes()))

	buf = bytes.Buffer{}
	itoa(&buf, 0)
	assert.Equal(t, "0", string(buf.Bytes()))

	buf = bytes.Buffer{}
	itoa(&buf, 123)
	assert.Equal(t, "123", string(buf.Bytes()))

	buf = bytes.Buffer{}
	itoa(&buf, 12322121212121112)
	assert.Equal(t, "12322121212121112", string(buf.Bytes()))

}
