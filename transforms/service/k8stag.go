package service

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	K8sTagType       = "k8stag"
	K8sPodName       = "k8s_pod_name"
	K8sNamespace     = "k8s_namespace"
	K8sContainerName = "k8s_container_name"
	K8sContainerId   = "k8s_container_id"
)

var (
	_ transforms.StatsTransformer = &K8sTag{}
	_ transforms.Transformer      = &K8sTag{}
)

type K8sTag struct {
	SourceFileKey string `json:"sourcefilefield"`
	stats         StatsInfo
}

func (g *K8sTag) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("k8stag transformer not support rawTransform")
}

func (g *K8sTag) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	for i := range datas {
		val, ok := datas[i][g.SourceFileKey]
		if !ok {
			errNum, err = transforms.SetError(errNum, nil, transforms.GetErr, g.SourceFileKey)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", g.SourceFileKey)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		strVal = filepath.Base(strVal)
		splits := strings.Split(strVal, "_")
		if len(splits) < 3 {
			splitErr := fmt.Errorf("k8stag transform sourcefile must has more than 2 sperated field but now only %v fields of %v", len(splits), strVal)
			errNum, err = transforms.SetError(errNum, splitErr, transforms.General, "")
			continue
		}
		datas[i][K8sPodName] = splits[0]
		datas[i][K8sNamespace] = splits[1]
		containerInfo := strings.TrimSuffix(strings.Join(splits[2:], "_"), ".log")
		containerInfoSplits := strings.Split(containerInfo, "-")
		datas[i][K8sContainerId] = containerInfoSplits[len(containerInfoSplits)-1]
		datas[i][K8sContainerName] = strings.Join(containerInfoSplits[0:len(containerInfoSplits)-1], "-")
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func (g *K8sTag) Description() string {
	//return "k8stag will get kubernetes tags from sourcefile name"
	return "从kubernetes 存储的文件名称中获取pod、containerID之类的tags信息"
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

func (g *K8sTag) ConfigOptions() []Option {
	return []Option{
		{
			KeyName:      "sourcefilefield",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "my_field_keyname",
			DefaultNoUse: true,
			Description:  "要进行Transform变化的键(sourcefilefield)",
			Type:         transforms.TransformTypeString,
			ToolTip:      "此处填写 File Reader 中 datasource_tag 选项配置的 key，该选项会记录文件路径",
		},
	}
}

func (g *K8sTag) Stage() string {
	return transforms.StageAfterParser
}

func (g *K8sTag) Stats() StatsInfo {
	return g.stats
}

func (g *K8sTag) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add(K8sTagType, func() transforms.Transformer {
		return &K8sTag{}
	})
}
