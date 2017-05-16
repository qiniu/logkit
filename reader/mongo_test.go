package reader

import (
	"os"
	"testing"

	"github.com/qiniu/logkit/conf"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestMongoReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath: metaDir,
		KeyFileDone: metaDir,
		KeyMode:     ModeMongo,
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)
	obj := bson.NewObjectId()
	er := &MongoReader{
		meta:       meta,
		host:       "127.0.0.1:12701",
		database:   "testdb",
		collection: "coll",
		offsetkey:  MongoDefaultOffsetKey,
		offset:     obj,

		collectionFilters: map[string]CollectionFilter{},
		status:            StatusInit,
		readChan:          make(chan []byte),
	}
	assert.EqualValues(t, "MongoReader:127.0.0.1:12701_testdb_coll", er.Name())
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
}
