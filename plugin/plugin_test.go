package plugin

import (
	"testing"
)

func TestListPlugins(t *testing.T) {
	Conf = &Config{
		Enabled: true,
		Dir:     "E:\\Go\\GOPATH\\src\\github.com\\qiniu\\logkit\\plugins",
		remoteSource:     "",
	}
	SyncPlugins()
}

