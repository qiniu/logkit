package apps

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestParser(t *testing.T) {
	cases := []struct {
		log string
		exp Data
	}{
		{
			log: "[00:00:07 413]INFO:vm-vmw2057-tod:bbl_hb:55322:[BBLHbCtrl.cpp:1108]:  *** server editodb: Shutdown.",
			exp: Data{
				"timestamp_tode": "00:00:07 413",
				"level_tode":     "INFO",
				"host_tode":      "vm-vmw2057-tod",
				"server_tode":    "bbl_hb",
				"pid_tode":       "55322",
				"source_tode":    "BBLHbCtrl.cpp",
				"line_tode":      "1108",
				"message_tode":   "*** server editodb: Shutdown.",
			},
		},
	}
	for _, v := range cases {
		data, err := parser(v.log)
		assert.NoError(t, err)
		assert.Equal(t, v.exp, data)
	}
}
