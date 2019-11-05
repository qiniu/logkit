package system

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_readProcFile(t *testing.T) {
	t.Parallel()
	data, err := readProcFile("Test_readProcFile")
	assert.Nil(t, data)
	assert.Nil(t, err)

	err = createFile("./Test_readProcFile", "Test_readProcFile")
	defer os.RemoveAll("./Test_readProcFile")
	assert.Nil(t, err)

	data, err = readProcFile("Test_readProcFile")
	assert.EqualValues(t, []byte("Test_readProcFile"), data)
	assert.Nil(t, err)
}

func Test_execPS(t *testing.T) {
	t.Parallel()
	data, err := execPS()
	assert.Nil(t, err)
	assert.NotNil(t, data)
}

func createFile(filename, content string) error {
	if err := ioutil.WriteFile(filename, []byte(content), 0755); err != nil {
		return err
	}

	return nil
}
