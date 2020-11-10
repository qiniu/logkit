package mutate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Script{}
	_ transforms.Transformer      = &Script{}
	_ transforms.Initializer      = &Script{}
)

// Script represents a transform to run a script.
type Script struct {
	Key         string `json:"key"`
	New         string `json:"new"`
	Interpreter string `json:"interprepter"`
	ScriptPath  string `json:"scriptpath"`
	Script      string `json:"script"`
	storePath   string
	stats       StatsInfo

	keysDetail [][]string
	news       []string
	recordErrs []string
	numRoutine int
}

func (g *Script) Init() error {
	script, err := DecodeString(g.Script)
	if err != nil {
		log.Errorf("script transformer decode script string error: %v", err)
	}
	if script != "" {
		scriptsDir := "transformer_scripts"
		realPath, err := getValidDir(scriptsDir)
		if err != nil {
			return err
		}

		g.storePath = filepath.Join(realPath, "script_"+Hash(script))
		scriptFile, openFileErr := os.OpenFile(g.storePath, os.O_RDWR|os.O_CREATE, DefaultFilePerm)
		if openFileErr != nil {
			return openFileErr
		}
		if _, fileWriteErr := io.WriteString(scriptFile, script); fileWriteErr != nil {
			return fileWriteErr
		}
	}
	if g.storePath == "" {
		g.storePath = g.ScriptPath
	}
	if g.storePath != "" {
		g.storePath, err = CheckPath(g.storePath)
		if err != nil {
			return err
		}
	}

	// 获取 keys
	keysArr := strings.Split(g.Key, ",")
	g.keysDetail = make([][]string, 0, 100)
	for _, key := range keysArr {
		key = strings.TrimSpace(key)
		keys := GetKeys(key)
		if len(keys) <= 0 {
			continue
		}
		g.keysDetail = append(g.keysDetail, keys)
	}
	// 获取新建keys
	g.news = GetKeys(g.New)
	// 增加error
	newsErr := g.New + "_error"
	g.recordErrs = GetKeys(newsErr)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Script) Transform(datas []Data) ([]Data, error) {
	if g.keysDetail == nil {
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

func getScriptRes(interpreter string, params []string) (string, error) {
	command := exec.Command(interpreter, params...) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return "", errors.New(interpreter + " " + strings.Join(params, ",") + " - run script err info is: " + err.Error())
	}

	return string(res), nil
}

func getScriptResFromCmd(script string) (string, error) {
	cmdArr := GetCmd(script)
	command := exec.Command(cmdArr[0], cmdArr[1:]...) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s - run script err info is %v", script, err)
	}

	return string(res), nil
}

func (g *Script) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("script transformer not support rawTransform")
}

func (g *Script) Description() string {
	//return "get script result"
	return "执行指定的脚本，并将脚本结果加入到数据中"
}

func (g *Script) Type() string {
	return "script"
}

func (g *Script) SampleConfig() string {
	return `{
       "type":"script",
       "key":"myTransformKey",
       "new":"myNewKey"
    }`
}

func (g *Script) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
		{
			KeyName:      "interprepter",
			ChooseOnly:   false,
			Default:      "bash",
			Required:     true,
			DefaultNoUse: false,
			Description:  "脚本执行解释器(interprepter)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "scriptpath",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "指定脚本路径(scriptpath)",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "script",
			Element:      Text,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "指定脚本内容(script)",
			Advance:      true,
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Script) Stage() string {
	return transforms.StageAfterParser
}

func (g *Script) Stats() StatsInfo {
	return g.stats
}

func (g *Script) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("script", func() transforms.Transformer {
		return &Script{}
	})
}

func getValidDir(dir string) (realPath string, err error) {
	realPath, fi, err := GetRealPath(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(realPath, DefaultDirPerm); err != nil {
			//此处的error需要直接返回，后面会根据error类型是否为path error做判断
			return
		}
		return
	}
	if err != nil {
		return
	}
	if !fi.Mode().IsDir() {
		err = errors.New("file is not directory")
	}
	return
}

func (g *Script) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err       error
		errNum    int
		scriptRes string
		getErr    error
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0
		getErr = nil

		params := make([]string, 1+len(g.keysDetail))
		params[0] = g.storePath
		paramIndex := 1
		for _, keys := range g.keysDetail {
			val, getErr := GetMapValue(transformInfo.CurData, keys...)
			if getErr != nil {
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
				continue
			}

			valStr, ok := val.(string)
			if !ok {
				typeErr := errors.New("transform key " + g.Key + " data type is not string")
				errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
				continue
			}
			params[paramIndex] = valStr
			paramIndex++
		}
		params = params[:paramIndex]

		getErr = nil
		scriptRes, getErr = getScriptRes(g.Interpreter, params)
		if getErr != nil {
			// 设置脚本执行结果的错误信息
			setErr := SetMapValue(transformInfo.CurData, getErr.Error(), false, g.recordErrs...)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
			}
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}

		// 设置脚本执行结果
		setErr := SetMapValue(transformInfo.CurData, scriptRes, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			ErrNum:  errNum,
			Err:     err,
		}
	}
	wg.Done()
}
