package apps

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestParserWithV2(t *testing.T) {
	cases := []struct {
		log string
		exp Data
	}{
		{
			log: "[14204] 02 Apr 11:19:26.804 # You requested maxclients of 10000 requiring at least 10032 max file descriptors.",
			exp: Data{
				"pid_redis":       "14204",
				"timestamp_redis": "02 Apr 11:19:26.804",
				"loglevel_redis":  "warning",
				"message_redis":   "You requested maxclients of 10000 requiring at least 10032 max file descriptors.",
			},
		},
	}
	for _, v := range cases {
		data, err := parserWithV2(v.log)
		assert.NoError(t, err)
		assert.Equal(t, v.exp, data)
	}
}

func TestParserWithV3(t *testing.T) {
	cases := []struct {
		log string
		exp Data
	}{
		{
			log: "41027:S 19 May 18:11:58.145 * Partial resynchronization not possible (no cached master)",
			exp: Data{
				"pid_redis":       "41027",
				"timestamp_redis": "19 May 18:11:58.145",
				"loglevel_redis":  "notice",
				"role_redis":      "slave",
				"message_redis":   "Partial resynchronization not possible (no cached master)",
			},
		},
	}
	for _, v := range cases {
		data, err := parserWithV3(v.log)
		assert.NoError(t, err)
		assert.Equal(t, v.exp, data)
	}
}

func TestGetRole(t *testing.T) {
	assert.Equal(t, "sentinel", getRole("X"))
	assert.Equal(t, "RDB/AOF writing child", getRole("C"))
	assert.Equal(t, "slave", getRole("S"))
	assert.Equal(t, "master", getRole("M"))
	assert.Equal(t, "unknown", getRole("A"))
}

func TestGetLogLevel(t *testing.T) {
	assert.Equal(t, "debug", getLogLevel("."))
	assert.Equal(t, "verbose", getLogLevel("-"))
	assert.Equal(t, "notice", getLogLevel("*"))
	assert.Equal(t, "warning", getLogLevel("#"))
	assert.Equal(t, "unknown", getLogLevel("/"))
}
