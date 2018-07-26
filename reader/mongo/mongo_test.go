package mongo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestMongoReader(t *testing.T) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath: MetaDir,
		reader.KeyFileDone: MetaDir,
		reader.KeyMode:     reader.ModeMongo,
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	obj := bson.NewObjectId()
	er := &Reader{
		meta:       meta,
		host:       "127.0.0.1:12701",
		database:   "testdb",
		collection: "coll",
		offsetkey:  DefaultOffsetKey,
		offset:     obj,

		collectionFilters: map[string]CollectionFilter{},
		status:            reader.StatusInit,
		readChan:          make(chan []byte),
	}
	assert.EqualValues(t, "MongoReader<127.0.0.1:12701_testdb_coll>", er.Name())
	er.SyncMeta()
	got, gotoffset, err := er.meta.ReadOffset()
	assert.NoError(t, err)
	assert.EqualValues(t, obj.Hex(), got)
	assert.EqualValues(t, 0, gotoffset)
	er.offsetkey = "testkey"
	er.offset = 123
	er.SyncMeta()
	got, gotoffset, err = er.meta.ReadOffset()
	assert.NoError(t, err)
	assert.EqualValues(t, er.offsetkey, got)
	assert.EqualValues(t, int64(123), gotoffset)

	assert.Equal(t, StatsInfo{}, er.Status())
}
