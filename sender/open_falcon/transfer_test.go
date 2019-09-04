package open_falcon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertToTransferData(t *testing.T) {
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
	data, ok = sender.converToTransferData("key3", int32(0), timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(0), data.Value)
	data, ok = sender.converToTransferData("key3", uint32(0), timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(0), data.Value)
	data, ok = sender.converToTransferData("key3", uint32(0), timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(0), data.Value)
	data, ok = sender.converToTransferData("key3", uint(0), timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, float64(0), data.Value)
	data, ok = sender.converToTransferData("key2", json.Number("0.02"), timeStamp)
	assert.Equal(t, true, ok)
	assert.Equal(t, 0.02, data.Value)
}

func TestGetEndpoint(t *testing.T) {
	tagsEndpoint := map[string]string{"vmname": "vm", "endpoint": "endpoint", "esxhostname": "host", "dsname": "ds"}

	v := getEndpoint("vsphere.vm.", "", ".", tagsEndpoint)
	assert.Equal(t, v, "endpoint")
	delete(tagsEndpoint, "endpoint")
	v = getEndpoint("vsphere.vm.", "", ".", tagsEndpoint)
	assert.Equal(t, v, "vm")
}

func TestSetTags(t *testing.T) {
	tags := setTags("", nil, "vccenter", "10.10.1.1")
	assert.EqualValues(t, "vccenter=10.10.1.1", tags)

	tags = setTags(tags, nil, "dcname", "cluster1")
	assert.EqualValues(t, "vccenter=10.10.1.1,dcname=cluster1", tags)

	tags = setTags(tags, nil, "esxhostname", 1.2)
	assert.EqualValues(t, "vccenter=10.10.1.1,dcname=cluster1,esxhostname=1.2", tags)

	tags = setTags(tags, nil, "clustername", struct {
		Name string
	}{"name"})
	assert.EqualValues(t, "vccenter=10.10.1.1,dcname=cluster1,esxhostname=1.2,clustername={name}", tags)

	tags = setTags("", map[string]bool{"a": true}, "vccenter", "10.10.1.1")
	assert.EqualValues(t, "", tags)

	tags = setTags(tags, map[string]bool{"a": true}, "a", "10.10.1.1")
	assert.EqualValues(t, "a=10.10.1.1", tags)

	tags = setTags(tags, map[string]bool{"b": true}, "b", "cluster1")
	assert.EqualValues(t, "a=10.10.1.1,b=cluster1", tags)
}
