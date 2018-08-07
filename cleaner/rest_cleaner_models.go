package cleaner

import (
	. "github.com/qiniu/logkit/utils/models"
)

var (
	OptionDeleteEnable = Option{
		KeyName:       KeyCleanEnable,
		Element:       Checkbox,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"true", "false"},
		Default:       "false",
		DefaultNoUse:  false,
		Description:   "开启日志删除功能(delete_enable)",
		Advance:       false,
		ToolTip:       `删除控制功能，默认不开启，当为true时开启。开启后，对于已经读取完毕的数据，cleaner会负责通知删除`,
	}

	OptionCleanInterval = Option{
		KeyName:      KeyCleanInterval,
		ChooseOnly:   false,
		Default:      "300",
		DefaultNoUse: false,
		Description:  "执行周期,单位为秒(s)(delete_interval)",
		Advance:      true,
		ToolTip:      `在每个周期检查是否符合删除的条件"`,
	}

	OptionReserveFileNumber = Option{
		KeyName:      KeyReserveFileNumber,
		ChooseOnly:   false,
		Default:      "10",
		DefaultNoUse: false,
		Description:  "最大保留的已读取文件数(reserve_file_number)",
		Advance:      true,
		ToolTip:      `解析失败的数据会默认出现在"pandora_stash"字段，该选项可以禁止记录解析失败的数据`,
	}

	OptionReserveFileSize = Option{
		KeyName:      KeyReserveFileSize,
		ChooseOnly:   false,
		Default:      "2048",
		DefaultNoUse: false,
		Description:  "最大保留已读文件总大小,单位为MB(reserve_file_size)",
		Advance:      true,
		ToolTip:      `当已读文件的总大小超过这个值时，会把最老的那部分删掉，默认保留2GB，单位为MB`,
	}
)

var ModeKeyOptions = []Option{
	OptionDeleteEnable,
	OptionCleanInterval,
	OptionReserveFileNumber,
	OptionReserveFileSize,
}
