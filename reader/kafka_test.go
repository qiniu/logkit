package reader

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

func TestKafkaReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath: metaDir,
		KeyFileDone: metaDir,
		KeyMode:     ModeElastic,
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)
	er := &KafkaReader{
		meta:             meta,
		ConsumerGroup:    "group1",
		Topics:           []string{"topic1"},
		ZookeeperPeers:   []string{"localhost:2181"},
		ZookeeperTimeout: time.Second,
		Whence:           "oldest",
		readChan:         make(chan json.RawMessage),
		errs:             make(chan error, 1000),
		status:           StatusInit,
		mux:              sync.Mutex{},
		started:          false,
	}
	assert.EqualValues(t, "KafkaReader:[topic1],[group1]", er.Name())

	assert.Equal(t, utils.StatsInfo{}, er.Status())
}
