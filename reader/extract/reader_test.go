package extract

import (
	"io/ioutil"
	"testing"

	"github.com/qiniu/logkit/reader"
	"github.com/stretchr/testify/assert"
)

func TestTarGz(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "../../utils/testdata/456.tar.gz")
	assert.NoError(t, err)

	assert.Equal(t, "targz:../../utils/testdata/456.tar.gz", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "a\n", string(data))

	assert.Equal(t, "456/c.txt", rd.Source())
	assert.NoError(t, rd.Close())

	rd, err = NewReader(&reader.Meta{}, "../../utils/testdata/b.txt.tar.gz")
	assert.NoError(t, err)

	assert.Equal(t, "targz:../../utils/testdata/b.txt.tar.gz", rd.Name())
	data, err = ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "b\n", string(data))

	assert.Equal(t, "b.txt", rd.Source())
	assert.NoError(t, rd.Close())
}

func TestTar(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "../../utils/testdata/123.tar")
	assert.NoError(t, err)

	assert.Equal(t, "tar:../../utils/testdata/123.tar", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "v\nia\n", string(data))

	assert.NoError(t, rd.Close())
}

func TestGz(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "../../utils/testdata/a.txt.gz")
	assert.NoError(t, err)

	assert.Equal(t, "gz:../../utils/testdata/a.txt.gz", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "ia\n", string(data))

	assert.Equal(t, "../../utils/testdata/a.txt", rd.Source())
	assert.NoError(t, rd.Close())
}

func TestZip(t *testing.T) {
	rd, err := NewReader(&reader.Meta{}, "../../utils/testdata/a.zip")
	assert.NoError(t, err)

	assert.Equal(t, "zip:../../utils/testdata/a.zip", rd.Name())
	data, err := ioutil.ReadAll(rd)
	assert.NoError(t, err)
	assert.Equal(t, "b\n\na\n", string(data))
	assert.NoError(t, rd.Close())
}
