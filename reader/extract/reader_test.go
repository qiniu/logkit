package extract

import (
	"io/ioutil"
	"testing"

	"github.com/qiniu/logkit/reader"
	"github.com/stretchr/testify/assert"
)

func TestTarGz(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "testdata/456.tar.gz", Opts{})
	assert.NoError(t, err)

	assert.Equal(t, "targz:testdata/456.tar.gz", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "a\n", string(data))

	assert.Equal(t, "456/c.txt", rd.Source())
	assert.NoError(t, rd.Close())

	rd, err = NewReader(&reader.Meta{}, "testdata/b.txt.tar.gz", Opts{})
	assert.NoError(t, err)

	assert.Equal(t, "targz:testdata/b.txt.tar.gz", rd.Name())
	data, err = ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "b\n", string(data))

	assert.Equal(t, "b.txt", rd.Source())
	assert.NoError(t, rd.Close())
}

func TestTar(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "testdata/123.tar", Opts{})
	assert.NoError(t, err)

	assert.Equal(t, "tar:testdata/123.tar", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "v\nia\n", string(data))

	assert.NoError(t, rd.Close())
}

func TestGz(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "testdata/a.txt.gz", Opts{})
	assert.NoError(t, err)

	assert.Equal(t, "gz:testdata/a.txt.gz", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "ia\n", string(data))

	assert.Equal(t, "testdata/a.txt", rd.Source())
	assert.NoError(t, rd.Close())
}

func TestZip(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "testdata/a.zip", Opts{})
	assert.NoError(t, err)

	assert.Equal(t, "zip:testdata/a.zip", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "b\n\na\n", string(data))
	assert.NoError(t, rd.Close())
}
