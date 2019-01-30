package cisco

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/grok"
	. "github.com/qiniu/logkit/utils/models"
)

func TestParse(t *testing.T) {
	tests := []struct {
		s          []string
		expectData []Data
	}{
		{
			expectData: []Data{},
		},
		{
			s: []string{`Teardown dynamic TCP translation from Inside:100.20.50.100/70000 to Outside:8.8.8.8/40000 duration 0:00:00`},
			expectData: []Data{
				{
					"src_xlated_interface": "Outside",
					"src_xlated_ip":        "8.8.8.8",
					"action":               "Teardown",
					"xlate_type":           "dynamic",
					"protocol":             "TCP",
					"src_interface":        "Inside",
					"src_ip":               "100.20.50.100",
					"src_port":             "70000",
				},
			},
		},
	}

	c := conf.MapConf{config.KeyGrokPatterns: CiscoPatterns}
	grokParser, _ := grok.NewParser(c)
	l := Parser{
		name:       config.TypeCisco,
		grokParser: grokParser,
	}
	for _, tt := range tests {
		got, err := l.Parse(tt.s)
		if c, ok := err.(*StatsError); ok {
			err = errors.New(c.LastError)
			assert.Equal(t, int64(0), c.Errors)
		}

		assert.EqualValues(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}
}
