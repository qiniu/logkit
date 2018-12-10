package service

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

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
	_ transforms.Initializer      = &K8sTag{}
)

type K8sTag struct {
	SourceFileKey string `json:"sourcefilefield"`
	stats         StatsInfo
	numRoutine    int
}

func (g *K8sTag) Init() error {
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *K8sTag) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("k8stag transformer not support rawTransform")
}

func (g *K8sTag) Transform(datas []Data) ([]Data, error) {
	if g.numRoutine == 0 {
		g.Init()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = g.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)
	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go g.transform(dataPipeline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipeline <- transforms.TransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipeline)
	}()

	var transformResultSlice = make(transforms.TransformResultSlice, dataLen)
	for resultInfo := range resultChan {
		transformResultSlice[resultInfo.Index] = resultInfo
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
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

func (g *K8sTag) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, ok := transformInfo.CurData[g.SourceFileKey]
		if !ok {
			errNum, err = transforms.SetError(errNum, nil, transforms.GetErr, g.SourceFileKey)
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + g.SourceFileKey + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		strVal = filepath.Base(strVal)
		splits := strings.Split(strVal, "_")
		if len(splits) < 3 {
			splitErr := fmt.Errorf("k8stag transform sourcefile must has more than 2 sperated field but now only %v fields of %v", len(splits), strVal)
			errNum, err = transforms.SetError(errNum, splitErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				Err:     err,
				ErrNum:  errNum,
			}
			continue
		}
		transformInfo.CurData[K8sPodName] = splits[0]
		transformInfo.CurData[K8sNamespace] = splits[1]
		containerInfo := strings.TrimSuffix(strings.Join(splits[2:], "_"), ".log")
		containerInfoSplits := strings.Split(containerInfo, "-")
		transformInfo.CurData[K8sContainerId] = containerInfoSplits[len(containerInfoSplits)-1]
		transformInfo.CurData[K8sContainerName] = strings.Join(containerInfoSplits[0:len(containerInfoSplits)-1], "-")

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			Err:     err,
			ErrNum:  errNum,
		}
	}
	wg.Done()
}
