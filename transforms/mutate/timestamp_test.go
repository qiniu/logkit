package mutate

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestTimestampTransformer(t *testing.T) {
	tests := []struct {
		Key          string
		New          string
		LayoutBefore string
		Precision    string
		Offset       int
		Override     bool
		data         Data
		exp          Data
	}{
		{
			LayoutBefore: "",
			Key:          "key",
			data:         Data{"key": "2017/03/28 15:41:53 -0000"},
			exp:          Data{"key": int64(1490715713)},
		},
		{
			LayoutBefore: "",
			Key:          "key",
			New:          "new",
			data:         Data{"key": "2017/03/28 15:41:53 -0000"},
			exp:          Data{"key": "2017/03/28 15:41:53 -0000", "new": int64(1490715713)},
		},
		{
			Offset:       -8,
			LayoutBefore: "2006/01/02/03/04/05",
			Key:          "key",
			New:          "new.key1",
			data:         Data{"key": "2017/09/22/11/07/12"},
			exp:          Data{"key": "2017/09/22/11/07/12", "new": map[string]interface{}{"key1": int64(1506049632)}},
		},
		{
			Offset:       -8,
			LayoutBefore: "2006/01/02/03/04/05",
			Key:          "key",
			New:          "new.key1",
			Precision:    NanoSeconds,
			Override:     true,
			data:         Data{"key": "2017/09/22/11/07/12", "new": map[string]interface{}{"key1": "value1"}},
			exp:          Data{"key": "2017/09/22/11/07/12", "new": map[string]interface{}{"key1": int64(1506049632000000000)}},
		},
		{
			Offset:       -8,
			LayoutBefore: "【2006/01/02/03/04/05】",
			Key:          "key",
			data:         Data{"key": "【2017/09/22/11/07/12】"},
			exp:          Data{"key": int64(1506049632)},
		},
		{
			Offset:       -9,
			LayoutBefore: "【2006/01/02/03/04/05】",
			Key:          "key",
			data:         Data{"key": "【2017/09/22/11/07/12】"},
			exp:          Data{"key": int64(1506046032)},
		},
	}
	for idx, ti := range tests {
		tis := &Timestamp{
			Key:          ti.Key,
			New:          ti.New,
			Offset:       ti.Offset,
			LayoutBefore: ti.LayoutBefore,
			Precision:    ti.Precision,
			Override:     ti.Override,
		}
		tis.Init()
		data, err := tis.Transform([]Data{ti.data})
		assert.Nil(t, err)
		exp := []Data{ti.exp}
		assert.Equal(t, exp, data, fmt.Sprintf("idx %v", idx))
		assert.Equal(t, transforms.StageAfterParser, tis.Stage())
	}

	fmt.Println(time.Now().Format(time.RFC3339), time.Now().Unix())
}
