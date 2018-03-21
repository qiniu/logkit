package router

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSenderRouter(t *testing.T) {
	senderCnt := 3
	routerConf := RouterConfig{}

	// keyName 为空
	r, err := NewSenderRouter(routerConf, senderCnt)
	assert.Nil(t, r)
	assert.NoError(t, err)

	routerConf.KeyName = "a"
	routerConf.MatchType = "aa"

	// MatchType 不存在
	r, err = NewSenderRouter(routerConf, senderCnt)
	assert.Nil(t, r)
	assert.Error(t, err)

	routerConf.MatchType = MTypeEqualName
	routerConf.DefaultIndex = 3

	// DefaultIndex 非法, 超出 sender 下标
	r, err = NewSenderRouter(routerConf, senderCnt)
	assert.Nil(t, r)
	assert.Error(t, err)

	routerConf.DefaultIndex = 2

	// 正常创建，但是路由为空
	r, err = NewSenderRouter(routerConf, senderCnt)
	assert.NoError(t, err)
	assert.Empty(t, r.routes)

	routerConf.Routes = map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}
	// 路由中有超出 sender 下标的值
	r, err = NewSenderRouter(routerConf, senderCnt)
	assert.Nil(t, r)
	assert.Error(t, err)

	routes := map[string]int{
		"a": 1,
		"b": 2,
		"c": 0,
	}
	routerConf.Routes = routes
	// 正常创建
	r, err = NewSenderRouter(routerConf, senderCnt)
	assert.NoError(t, err)
	assert.Equal(t, "a", r.key)
	assert.Equal(t, 2, r.defaultIndex)
	assert.Equal(t, MTypeEqualName, r.matchType.name())
	assert.Equal(t, len(routes), len(r.routes))
	for val, id := range routes {
		v, ok := r.routes[val]
		assert.Equal(t, true, ok)
		assert.Equal(t, id, v)
	}
}

func TestSenderValueToString(t *testing.T) {
	testData := []struct {
		input    interface{}
		getValue string
		isOk     bool
	}{
		{
			input:    "123",
			getValue: "123",
			isOk:     true,
		}, {
			input:    int(123),
			getValue: "123",
			isOk:     true,
		}, {
			input:    int16(123),
			getValue: "123",
			isOk:     true,
		}, {
			input:    int64(123),
			getValue: "123",
			isOk:     true,
		}, {
			input:    uint32(123),
			getValue: "123",
			isOk:     true,
		}, {
			input:    uint64(123),
			getValue: "123",
			isOk:     true,
		}, {
			input:    float32(123.123),
			getValue: "123.123",
			isOk:     true,
		}, {
			input:    float64(123.123),
			getValue: "123.123",
			isOk:     true,
		}, {
			input:    interface{}(123),
			getValue: "123",
			isOk:     true,
		}, {
			input:    []int{123, 134},
			getValue: "",
			isOk:     false,
		}, {
			input:    123.123,
			getValue: "123.123",
			isOk:     true,
		},
	}

	for _, val := range testData {
		gotValue, isOk := senderValueToString(val.input)
		assert.Equal(t, val.getValue, gotValue)
		assert.Equal(t, val.isOk, isOk)
	}
}

func TestMtypeEqual(t *testing.T) {
	equalFunc := MatchTypeRegistry[MTypeEqualName]
	equal := equalFunc()
	testData := []struct {
		input     interface{}
		matchData string
		isOk      bool
	}{
		{
			input:     "123",
			matchData: "123",
			isOk:      true,
		}, {
			input:     int(123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     int16(123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     int64(123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     uint32(123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     uint64(123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     float32(123.123),
			matchData: "123.123",
			isOk:      true,
		}, {
			input:     float64(123.123),
			matchData: "123.123",
			isOk:      true,
		}, {
			input:     interface{}(123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     []int{123, 134},
			matchData: "",
			isOk:      false,
		}, {
			input:     123.123,
			matchData: "123.123",
			isOk:      true,
		},
	}

	for _, val := range testData {
		gotRes := equal.isMatch(val.input, val.matchData)
		assert.Equal(t, val.isOk, gotRes)
	}
}

func TestMtypeContains(t *testing.T) {
	containFunc := MatchTypeRegistry[MTypeContainsName]
	contain := containFunc()
	testData := []struct {
		input     interface{}
		matchData string
		isOk      bool
	}{
		{
			input:     "123456",
			matchData: "123",
			isOk:      true,
		}, {
			input:     int(13),
			matchData: "123",
			isOk:      false,
		}, {
			input:     int16(4123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     int64(13423),
			matchData: "123",
			isOk:      false,
		}, {
			input:     uint32(4545123),
			matchData: "123",
			isOk:      true,
		}, {
			input:     uint64(123098),
			matchData: "123",
			isOk:      true,
		}, {
			input:     float32(123.123),
			matchData: "123.123",
			isOk:      true,
		}, {
			input:     float64(123.23123),
			matchData: "123.123",
			isOk:      false,
		}, {
			input:     interface{}(12345),
			matchData: "123",
			isOk:      true,
		}, {
			input:     []int{123, 134},
			matchData: "",
			isOk:      false,
		}, {
			input:     123.123,
			matchData: "123.123",
			isOk:      true,
		},
	}

	for id, val := range testData {
		gotRes := contain.isMatch(val.input, val.matchData)
		assert.Equal(t, val.isOk, gotRes, strconv.Itoa(id))
	}
}
