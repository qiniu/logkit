package qplayerqos

var (
	// DefaultEventTypes 默认的事件类型。
	DefaultEventTypes = []string{
		"play.v5",
		"play_start_op.v5",
		"play_start.v5",
		"play_end_op.v5",
		"play_end.v5",
		"network_change.v5",
		"play_tcp.v5",
		"play_seek.v5",
		"play_errcode.v5",
	}

	// DefaultEventSpecs 默认的事件字段列表。
	DefaultEventSpecs = map[string]*EventSchema{
		"play.v5": {
			// 播放中
			Type:        "play.v5",
			MinFieldNum: 23,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},       // 0
				1:  {Key: "tag", Type: FieldTypeString},             // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp},  // 2
				3:  {Key: "device", Type: FieldTypeString},          // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},     // 4
				5:  {Key: "protocol", Type: FieldTypeString},        // 5
				6:  {Key: "domain", Type: FieldTypeString},          // 6
				7:  {Key: "path", Type: FieldTypeString},            // 7
				8:  {Key: "reqid", Type: FieldTypeString},           // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},       // 9
				10: {Key: "begin", Type: FieldTypeTimestamp},        // 10
				11: {Key: "end", Type: FieldTypeTimestamp},          // 11
				12: {Key: "buffering", Type: FieldTypeInt},          // 12
				13: {Key: "video_src_fps", Type: FieldTypeFloat},    // 13
				14: {Key: "video_drops", Type: FieldTypeInt},        // 14
				15: {Key: "audio_src_fps", Type: FieldTypeInt},      // 15
				16: {Key: "audio_drops", Type: FieldTypeInt},        // 16
				17: {Key: "video_render_fps", Type: FieldTypeFloat}, // 17
				18: {Key: "audio_render_fps", Type: FieldTypeFloat}, // 18
				19: {Key: "video_buffer_size", Type: FieldTypeInt},  // 19
				20: {Key: "audio_buffer_size", Type: FieldTypeInt},  // 20
				21: {Key: "audio_bitrate", Type: FieldTypeInt},      // 21
				22: {Key: "video_bitrate", Type: FieldTypeInt},      // 22
				23: {Key: "app_id", Type: FieldTypeString},          // 23
				24: {Key: "play_src_type", Type: FieldTypeInt},      // 24
				25: {Key: "session_id", Type: FieldTypeString},      // 25
			},
		}, // end of play.v5
		"play_start.v5": {
			// 播放开始
			Type:        "play_start.v5",
			MinFieldNum: 17,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},            // 0
				1:  {Key: "tag", Type: FieldTypeString},                  // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp},       // 2
				3:  {Key: "device", Type: FieldTypeString},               // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},          // 4
				5:  {Key: "protocol", Type: FieldTypeString},             // 5
				6:  {Key: "domain", Type: FieldTypeString},               // 6
				7:  {Key: "path", Type: FieldTypeString},                 // 7
				8:  {Key: "reqid", Type: FieldTypeString},                // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},            // 9
				10: {Key: "first_video_render_time", Type: FieldTypeInt}, // 10
				11: {Key: "first_audio_render_time", Type: FieldTypeInt}, // 11
				12: {Key: "gop_time", Type: FieldTypeInt},                // 12
				13: {Key: "video_codec", Type: FieldTypeString},          // 13
				14: {Key: "audio_codec", Type: FieldTypeString},          // 14
				15: {Key: "tcp_connect_time", Type: FieldTypeTimestamp},  // 15
				16: {Key: "first_byte_time", Type: FieldTypeTimestamp},   // 16
				17: {Key: "app_id", Type: FieldTypeString},               // 17
				18: {Key: "play_src_type", Type: FieldTypeInt},           // 18
				19: {Key: "session_id", Type: FieldTypeString},           // 19
			},
		}, // end of play_start.v5
		"play_end.v5": {
			// 播放结束
			Type:        "play_end.v5",
			MinFieldNum: 41,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},         // 0
				1:  {Key: "tag", Type: FieldTypeString},               // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp},    // 2
				3:  {Key: "device", Type: FieldTypeString},            // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},       // 4
				5:  {Key: "protocol", Type: FieldTypeString},          // 5
				6:  {Key: "domain", Type: FieldTypeString},            // 6
				7:  {Key: "path", Type: FieldTypeString},              // 7
				8:  {Key: "reqid", Type: FieldTypeString},             // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},         // 9
				10: {Key: "begin", Type: FieldTypeTimestamp},          // 10
				11: {Key: "end", Type: FieldTypeTimestamp},            // 11
				12: {Key: "buffer_count", Type: FieldTypeInt},         // 12
				13: {Key: "buffer_duration", Type: FieldTypeInt},      // 13
				14: {Key: "downloaded_bytes", Type: FieldTypeInt},     // 14
				15: nil,                                               // 15 - skipped
				16: {Key: "gop_time", Type: FieldTypeInt},             // 16
				17: {Key: "device_type", Type: FieldTypeString},       // 17
				18: {Key: "device_os", Type: FieldTypeString},         // 18
				19: {Key: "device_os_version", Type: FieldTypeString}, // 19
				20: {Key: "app_id", Type: FieldTypeString},            // 20
				21: {Key: "app_version", Type: FieldTypeString},       // 21
				22: {Key: "system_cpu_usage", Type: FieldTypeFloat},   // 22
				23: {Key: "app_cpu_usage", Type: FieldTypeFloat},      // 23
				24: {Key: "system_mem_usage", Type: FieldTypeFloat},   // 24
				25: {Key: "app_mem_usage", Type: FieldTypeFloat},      // 25
				26: nil,                                               // 26 - skipped
				27: nil,                                               // 27 - skipped
				28: {Key: "network_type", Type: FieldTypeString},      // 28
				29: {Key: "device_ip", Type: FieldTypeString},         // 29
				30: {Key: "resolver_ip", Type: FieldTypeString},       // 30
				31: {Key: "wifi_name", Type: FieldTypeString},         // 31
				32: {Key: "isp_name", Type: FieldTypeString},          // 32
				33: {Key: "signal_db", Type: FieldTypeInt},            // 33
				34: {Key: "signal_level", Type: FieldTypeString},      // 34
				35: {Key: "connect_server_time", Type: FieldTypeInt},  // 35
				36: {Key: "connect_server_time", Type: FieldTypeInt},  // 36
				37: {Key: "first_byte_time", Type: FieldTypeInt},      // 37
				38: nil,                                               // 38 - skipped
				39: nil,                                               // 39 - skipped
				40: nil,                                               // 40 - skipped
				41: {Key: "app_id", Type: FieldTypeString},            // 41
				42: {Key: "play_src_type", Type: FieldTypeInt},        // 42
				43: {Key: "session_id", Type: FieldTypeString},        // 43
			}, // end of play_end.v5
		},
		"play_start_op.v5": {
			// 开启播放操作。
			Type:        "play_start_op.v5",
			MinFieldNum: 15,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},         // 0
				1:  {Key: "tag", Type: FieldTypeString},               // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp},    // 2
				3:  {Key: "device", Type: FieldTypeString},            // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},       // 4
				5:  {Key: "protocol", Type: FieldTypeString},          // 5
				6:  {Key: "domain", Type: FieldTypeString},            // 6
				7:  {Key: "path", Type: FieldTypeString},              // 7
				8:  {Key: "reqid", Type: FieldTypeString},             // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},         // 9
				10: {Key: "device_type", Type: FieldTypeString},       // 10
				11: {Key: "device_os", Type: FieldTypeString},         // 11
				12: {Key: "device_os_version", Type: FieldTypeString}, // 12
				13: {Key: "app_id", Type: FieldTypeString},            // 13
				14: {Key: "app_version", Type: FieldTypeString},       // 14
				15: {Key: "app_id", Type: FieldTypeString},            // 15
				16: {Key: "play_src_type", Type: FieldTypeInt},        // 16
				17: {Key: "session_id", Type: FieldTypeString},        // 17
			},
		}, // end of play_start_op.v5
		"play_end_op.v5": {
			Type:        "play_end_op.v5",
			MinFieldNum: 27,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},         // 0
				1:  {Key: "tag", Type: FieldTypeString},               // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp},    // 2
				3:  {Key: "device", Type: FieldTypeString},            // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},       // 4
				5:  {Key: "protocol", Type: FieldTypeString},          // 5
				6:  {Key: "domain", Type: FieldTypeString},            // 6
				7:  {Key: "path", Type: FieldTypeString},              // 7
				8:  {Key: "reqid", Type: FieldTypeString},             // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},         // 9
				10: {Key: "device_type", Type: FieldTypeString},       // 10
				11: {Key: "device_os", Type: FieldTypeString},         // 11
				12: {Key: "device_os_version", Type: FieldTypeString}, // 12
				13: {Key: "app_id", Type: FieldTypeString},            // 13
				14: {Key: "app_version", Type: FieldTypeString},       // 14
				15: {Key: "watch_duration", Type: FieldTypeInt},       // 15
				16: {Key: "video_duration", Type: FieldTypeInt},       // 16
				17: {Key: "video_codec", Type: FieldTypeString},       // 17
				18: {Key: "audio_codec", Type: FieldTypeString},       // 18
				19: {Key: "first_frame_time", Type: FieldTypeInt},     // 19
				20: {Key: "is_wifi", Type: FieldTypeInt},              // 20
				21: {Key: "video_src_fps", Type: FieldTypeFloat},      // 21
				22: {Key: "audio_src_fps", Type: FieldTypeFloat},      // 22
				23: {Key: "audio_bitrate", Type: FieldTypeInt},        // 23
				24: {Key: "video_bitrate", Type: FieldTypeInt},        // 24
				25: {Key: "err_code", Type: FieldTypeInt},             // 25
				26: {Key: "system_err_code", Type: FieldTypeInt},      // 26
				27: {Key: "app_id", Type: FieldTypeString},            // 27
				28: {Key: "play_src_type", Type: FieldTypeInt},        // 28
				29: {Key: "session_id", Type: FieldTypeString},        // 29
			},
		}, // end of play_end_op.v5
		"network_change.v5": {
			// 网络环境变化。
			Type:        "network_change.v5",
			MinFieldNum: 17,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},      // 0
				1:  {Key: "tag", Type: FieldTypeString},            // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp}, // 2
				3:  {Key: "device", Type: FieldTypeString},         // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},    // 4
				5:  {Key: "protocol", Type: FieldTypeString},       // 5
				6:  {Key: "domain", Type: FieldTypeString},         // 6
				7:  {Key: "path", Type: FieldTypeString},           // 7
				8:  {Key: "reqid", Type: FieldTypeString},          // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},      // 9
				10: {Key: "network_type", Type: FieldTypeString},   // 10
				11: {Key: "device_ip", Type: FieldTypeString},      // 11
				12: {Key: "resolver_ip", Type: FieldTypeString},    // 12
				13: {Key: "wifi_name", Type: FieldTypeString},      // 13
				14: {Key: "isp_name", Type: FieldTypeString},       // 14
				15: {Key: "signal_db", Type: FieldTypeInt},         // 15
				16: {Key: "signal_level", Type: FieldTypeString},   // 16
				17: {Key: "app_id", Type: FieldTypeString},         // 17
				18: {Key: "play_src_type", Type: FieldTypeInt},     // 18
				19: {Key: "session_id", Type: FieldTypeString},     // 19
			},
		}, // end of network_change.v5
		"play_tcp.v5": {
			// 播放器建立TCP连接。
			Type:        "play_tcp.v5",
			MinFieldNum: 16,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},      // 0
				1:  {Key: "tag", Type: FieldTypeString},            // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp}, // 2
				3:  {Key: "device", Type: FieldTypeString},         // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},    // 4
				5:  {Key: "protocol", Type: FieldTypeString},       // 5
				6:  {Key: "domain", Type: FieldTypeString},         // 6
				7:  {Key: "path", Type: FieldTypeString},           // 7
				8:  {Key: "reqid", Type: FieldTypeString},          // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},      // 9
				10: {Key: "start_time", Type: FieldTypeTimestamp},  // 10
				11: {Key: "duration", Type: FieldTypeInt},          // 11
				12: {Key: "err_code", Type: FieldTypeInt},          // 12
				13: {Key: "app_id", Type: FieldTypeString},         // 13
				14: {Key: "play_src_type", Type: FieldTypeInt},     // 14
				15: {Key: "session_id", Type: FieldTypeString},     // 15
			},
		}, // end of play_tcp.v5
		"play_seek.v5": {
			// seek 操作，拖动进度条等。
			Type:        "play_seek.v5",
			MinFieldNum: 17,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},      // 0
				1:  {Key: "tag", Type: FieldTypeString},            // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp}, // 2
				3:  {Key: "device", Type: FieldTypeString},         // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},    // 4
				5:  {Key: "protocol", Type: FieldTypeString},       // 5
				6:  {Key: "domain", Type: FieldTypeString},         // 6
				7:  {Key: "path", Type: FieldTypeString},           // 7
				8:  {Key: "reqid", Type: FieldTypeString},          // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},      // 9
				10: {Key: "seek_time", Type: FieldTypeTimestamp},   // 10
				11: {Key: "seek_from", Type: FieldTypeInt},         // 11
				12: {Key: "seek_to", Type: FieldTypeInt},           // 12
				13: {Key: "duration", Type: FieldTypeInt},          // 13
				14: {Key: "app_id", Type: FieldTypeString},         // 14
				15: {Key: "play_src_type", Type: FieldTypeInt},     // 15
				16: {Key: "session_id", Type: FieldTypeString},     // 16
			},
		}, // end of play_seek.v5
		"play_errcode.v5": {
			Type:        "play_errcode.v5",
			MinFieldNum: 20,
			MaxFieldNum: DefaultMaxFields,
			FieldSpecs: map[int]*FieldSpec{
				0:  {Key: "client_ip", Type: FieldTypeString},         // 0
				1:  {Key: "tag", Type: FieldTypeString},               // 1
				2:  {Key: "client_time", Type: FieldTypeTimestamp},    // 2
				3:  {Key: "device", Type: FieldTypeString},            // 3
				4:  {Key: "sdk_version", Type: FieldTypeString},       // 4
				5:  {Key: "protocol", Type: FieldTypeString},          // 5
				6:  {Key: "domain", Type: FieldTypeString},            // 6
				7:  {Key: "path", Type: FieldTypeString},              // 7
				8:  {Key: "reqid", Type: FieldTypeString},             // 8
				9:  {Key: "remote_ip", Type: FieldTypeString},         // 9
				10: {Key: "device_type", Type: FieldTypeString},       // 10
				11: {Key: "device_os", Type: FieldTypeString},         // 11
				12: {Key: "device_os_version", Type: FieldTypeString}, // 12
				13: {Key: "app_id", Type: FieldTypeString},            // 13
				14: {Key: "app_version", Type: FieldTypeString},       // 14
				15: {Key: "err_time", Type: FieldTypeTimestamp},       // 15
				16: {Key: "err_code", Type: FieldTypeInt},             // 16
				17: {Key: "app_id", Type: FieldTypeString},            // 17
				18: {Key: "play_src_type", Type: FieldTypeInt},        // 18
				19: {Key: "session_id", Type: FieldTypeString},        // 19
			},
		}, // end of play_errcode.v5
	}
)
