package rfc3164

import (
	"time"

	"github.com/influxdata/go-syslog"
	"github.com/influxdata/go-syslog/common"
)

type syslogMessage struct {
	prioritySet  bool // We explictly flag the setting of priority since its zero value is a valid priority by RFC 3164
	timestampSet bool // We explictly flag the setting of timestamp since its zero value is a valid timestamp by RFC 3164
	priority     uint8
	timestamp    time.Time
	hostname     string
	tag          string
	content      string
	message      string
}

func (sm *syslogMessage) minimal() bool {
	return sm.prioritySet && common.ValidPriority(sm.priority)
}

// export is meant to be called on minimally-valid messages
// thus it presumes priority and version values exists and are correct
func (sm *syslogMessage) export() syslog.LogParts {
	timestamp := time.Now().UTC().Round(time.Second)
	if sm.timestampSet {
		timestamp = sm.timestamp
	}

	hostname := ""
	if sm.hostname != "-" && sm.hostname != "" {
		hostname = sm.hostname
	}

	tag := ""
	if sm.tag != "-" && sm.tag != "" {
		tag = sm.tag
	}

	procId := ""
	if sm.content != "-" && sm.content != "" {
		// Content is usually process ID
		// See https://tools.ietf.org/html/rfc3164#section-5.3
		procId = sm.content
	}

	content := ""
	if sm.message != "" {
		content = sm.message
	}

	out := syslog.LogParts{
		"priority":  sm.priority,
		"facility":  uint8(sm.priority / 8),
		"severity":  uint8(sm.priority % 8),
		"hostname":  hostname,
		"timestamp": timestamp,
		"proc_id":   procId,
		"tag":       tag,
		"content":   content,
	}

	return out
}

// SyslogMessage represents a RFC3164 syslog message.
type SyslogMessage struct {
	syslog.Base
}
