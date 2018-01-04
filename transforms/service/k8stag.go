package service

import (
	"errors"
	"strings"

	"fmt"

	"path/filepath"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
)

const (
	K8sTagType       = "k8stag"
	K8sPodName       = "k8s_pod_name"
	K8sNamespace     = "k8s_namespace"
	K8sContainerName = "k8s_container_name"
	K8sContainerId   = "k8s_container_id"
)

type K8sTag struct {
	SourceFileKey string `json:"sourcefilefield"`
	stats         utils.StatsInfo
}

func (g *K8sTag) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("k8stag transformer not support rawTransform")
}

func (g *K8sTag) Transform(datas []sender.Data) ([]sender.Data, error) {
	var err, ferr error
	errnums := 0
	for i := range datas {
		val, ok := datas[i][g.SourceFileKey]
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.SourceFileKey)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", g.SourceFileKey)
			continue
		}
		strval = filepath.Base(strval)
		splits := strings.Split(strval, "_")
		if len(splits) < 3 {
			errnums++
			err = fmt.Errorf("k8stag transform sourcefile must has more than 2 sperated field but now only %v fields of %v", len(splits), strval)
			continue
		}
		datas[i][K8sPodName] = splits[0]
		datas[i][K8sNamespace] = splits[1]
		container_info := strings.TrimSuffix(strings.Join(splits[2:], "_"), ".log")
		container_info_splits := strings.Split(container_info, "-")
		datas[i][K8sContainerId] = container_info_splits[len(container_info_splits)-1]
		datas[i][K8sContainerName] = strings.Join(container_info_splits[0:len(container_info_splits)-1], "-")
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform k8stag, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *K8sTag) Description() string {
	return "k8stag will get kubernetes tags from sourcefile name"
}

func (g *K8sTag) Type() string {
	return K8sTagType
}

func (g *K8sTag) SampleConfig() string {
	return `{
		"type":"k8stag",
		"sourcefilefield":"datasource"
	}`
}

func (g *K8sTag) ConfigOptions() []utils.Option {
	return []utils.Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
	}
}

func (g *K8sTag) Stage() string {
	return transforms.StageAfterParser
}

func (g *K8sTag) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add(K8sTagType, func() transforms.Transformer {
		return &K8sTag{}
	})
}
