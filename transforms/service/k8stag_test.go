package service

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/stretchr/testify/assert"
)

func TestK8sTransformer(t *testing.T) {
	ktag := &K8sTag{
		SourceFileKey: "sourcetag",
	}
	data, err := ktag.Transform([]sender.Data{{"sourcetag": "admin-220-jrhnr_zhujiali_admin-92.log", "abc": "x1 y2"}, {"sourcetag": "atnet-status-29419-1_ava_atnet-status-27.log", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []sender.Data{
		{"sourcetag": "admin-220-jrhnr_zhujiali_admin-92.log", K8sPodName: "admin-220-jrhnr", K8sNamespace: "zhujiali", K8sContainer: "admin-92", "abc": "x1 y2"},
		{"sourcetag": "atnet-status-29419-1_ava_atnet-status-27.log", K8sPodName: "atnet-status-29419-1", K8sNamespace: "ava", K8sContainer: "atnet-status-27", "abc": "x1"}}
	assert.Equal(t, exp, data)

	assert.Equal(t, ktag.Stage(), transforms.StageAfterParser)
}
