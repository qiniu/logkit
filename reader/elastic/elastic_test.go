package elastic

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestElasticReader(t *testing.T) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath: MetaDir,
		reader.KeyFileDone: MetaDir,
		reader.KeyMode:     reader.ModeElastic,
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer DestroyDir()
	er := &Reader{
		meta:      meta,
		esindex:   "app",
		estype:    "type",
		eshost:    "127.0.0.1:9200",
		readBatch: 100,
		status:    reader.StatusInit,
		offset:    "TestElasticReader",
		readChan:  make(chan json.RawMessage),
	}
	assert.EqualValues(t, "ESReader:127.0.0.1:9200_app_type", er.Name())
	er.SyncMeta()
	got, _, err := er.meta.ReadOffset()
	assert.NoError(t, err)
	assert.EqualValues(t, er.offset, got)

	sts := er.Status()
	assert.Equal(t, StatsInfo{}, sts)
}
