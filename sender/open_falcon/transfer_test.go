package open_falcon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConverToTransferData(t *testing.T) {
	sender := TransferSender{}
	var timeStamp int64 = 1000000
	data, ok := sender.converToTransferData("key1", "test", timeStamp)
	assert.Equal(t, false, ok)
	data, ok = sender.converToTransferData("key2", "0.02", timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, 0.02, data.Value)
	data, ok = sender.converToTransferData("key3", 10, timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(10), data.Value)
	data, ok = sender.converToTransferData("key2", json.Number("0.02"), timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, 0.02, data.Value)
}

func TestSetTags(t *testing.T) {
	tags := setTags("", "hostname", "10.10.1.1")
	assert.EqualValues(t, "hostname=10.10.1.1", tags)

	tags = setTags(tags, "clustername", "cluster1")
	assert.EqualValues(t, "hostname=10.10.1.1,clustername=cluster1", tags)
}
