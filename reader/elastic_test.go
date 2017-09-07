package reader

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

func TestElasticReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath: metaDir,
		KeyFileDone: metaDir,
		KeyMode:     ModeElastic,
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)
	er := &ElasticReader{
		meta:      meta,
		esindex:   "app",
		estype:    "type",
		eshost:    "127.0.0.1:9200",
		readBatch: 100,
		status:    StatusInit,
		offset:    "TestElasticReader",
		readChan:  make(chan json.RawMessage),
	}
	assert.EqualValues(t, "ESReader:127.0.0.1:9200_app_type", er.Name())
	er.SyncMeta()
	got, _, err := er.meta.ReadOffset()
	assert.NoError(t, err)
	assert.EqualValues(t, er.offset, got)

	sts := er.Status()
	assert.Equal(t, utils.StatsInfo{}, sts)
}
