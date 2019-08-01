package kafka

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestKafkaReader(t *testing.T) {

	logkitConf := conf.MapConf{
		KeyMetaPath: MetaDir,
		KeyFileDone: MetaDir,
		KeyMode:     ModeElastic,
	}
	meta, err := reader.NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	er := &Reader{
		meta:             meta,
		ConsumerGroup:    "group1",
		Topics:           []string{"topic1"},
		ZookeeperPeers:   []string{"localhost:2181"},
		ZookeeperTimeout: time.Second,
		Whence:           "oldest",
		errChan:          make(chan error, 1000),
		lock:             new(sync.Mutex),
		statsLock:        new(sync.RWMutex),
	}
	assert.EqualValues(t, "KafkaReader:[topic1],[group1]", er.Name())

	assert.Equal(t, StatsInfo{}, er.Status())
}
