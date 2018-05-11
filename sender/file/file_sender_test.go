package file

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestFileSender(t *testing.T) {
	path := "TestFileSender"
	defer os.RemoveAll(path)
	fsender, err := NewFileSender(conf.MapConf{sender.KeyFileSenderPath: filepath.Join(path, "%Y%m%d.log")})
	assert.NoError(t, err)
	err = fsender.Send([]Data{{"abc": 123}})
	assert.NoError(t, err)
	err = fsender.Close()
	assert.NoError(t, err)
	datet := time.Now().Format("20060102")
	body, err := ioutil.ReadFile(filepath.Join(path, datet+".log"))
	assert.NoError(t, err)
	assert.Equal(t, `[{"abc":123}]
`, string(body))
}
