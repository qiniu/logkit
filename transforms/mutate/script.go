package mutate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"unicode"

	"path/filepath"

	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

type Script struct {
	Key          string `json:"key"`
	New          string `json:"new"`
	Interprepter string `json:"interprepter"`
	ScriptPath   string `json:"scriptpath"`
	Script       []byte `json:"script"`
	storePath    string
	stats        utils.StatsInfo
}

func (g *Script) Init() error {
	script := string(g.Script)
	if script != "" {
		scriptsDir := "transformer_scripts"
		realPath, err := getValidDir(scriptsDir)
		if err != nil {
			return err
		}

		g.storePath = filepath.Join(realPath, "script_"+utils.Hash(script))
		scriptFile, openFileErr := os.OpenFile(g.storePath, os.O_RDWR|os.O_CREATE, DefaultFilePerm)
		if openFileErr != nil {
			return openFileErr
		}
		if _, fileWriteErr := io.WriteString(scriptFile, script); fileWriteErr != nil {
			return fileWriteErr
		}
	}
	return nil
}

func (g *Script) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errCount := 0
	keys := utils.GetKeys(g.Key)
	news := utils.GetKeys(g.New)
	if g.storePath == "" {
		g.storePath = g.ScriptPath
	}
	scriptPath := g.storePath

	for i := range datas {
		var scriptRes string
		var gerr error
		if scriptPath == "" {
			val, gerr := utils.GetMapValue(datas[i], keys...)
			if gerr != nil {
				errCount++
				err = fmt.Errorf("transform key %v not exist in data", g.Key)
				continue
			}
			var ok bool
			if scriptPath, ok = val.(string); !ok {
				errCount++
				err = fmt.Errorf("transform key %v data type is not string", g.Key)
				continue
			}
			g.storePath = scriptPath
		}

		gerr = nil
		scriptRes, gerr = getScriptRes(g.Interprepter, g.storePath)
		if gerr != nil {
			errCount++
			err = gerr
			continue
		}

		if len(news) == 0 {
			utils.DeleteMapValue(datas[i], keys...)
			news = keys
		}
		seterr := utils.SetMapValue(datas[i], scriptRes, false, news...)
		if seterr != nil {
			errCount++
			err = fmt.Errorf("the new key %v already exists ", g.New)
		}
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform script, last error info is %v", errCount, err)
	}
	g.stats.Errors += int64(errCount)
	g.stats.Success += int64(len(datas) - errCount)
	return datas, ferr
}

func getScriptRes(interpreter string, path string) (string, error) {
	path, err := checkPath(path)
	if err != nil {
		return "", err
	}

	command := exec.Command(interpreter, path) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s - run script err info is %v", err)
	}

	return string(res), nil
}

func getScriptResFromCmd(script string) (string, error) {
	cmdArr := getCmd(script)
	command := exec.Command(cmdArr[0], cmdArr[1:]...) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s - run script err info is %v", err)
	}

	return string(res), nil
}

//根据key字符串,拆分出层级keys数据
func getCmd(keyStr string) []string {
	keys := strings.FieldsFunc(keyStr, isSeparator)
	return keys
}

func isSeparator(separator rune) bool {
	return separator == ' ' || unicode.IsSpace(separator)
}

func checkPath(path string) (string, error) {
	realPath, fileInfo, err := utils.GetRealPath(path)
	if err != nil || fileInfo == nil {
		return "", fmt.Errorf("%s - utils.GetRealPath failed, err:%v", path, err)
	}

	fileMode := fileInfo.Mode()
	if !fileMode.IsRegular() {
		return "", fmt.Errorf("%s - file failed, err: file is not regular", path)
	}
	utils.CheckFileMode(realPath, fileMode)
	return realPath, nil
}

func (g *Script) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("script transformer not support rawTransform")
}

func (g *Script) Description() string {
	//return "get script result"
	return "获取脚本运行结果"
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
		transforms.KeyFieldNew,
		{
			KeyName:      "interprepter",
			ChooseOnly:   false,
			Default:      "bash",
			Required:     true,
			DefaultNoUse: false,
			Description:  "脚本执行解释器",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "scriptpath",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "指定脚本路径",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "script",
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "指定脚本内容",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Script) Stage() string {
	return transforms.StageAfterParser
}

func (g *Script) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("script", func() transforms.Transformer {
		return &Script{}
	})
}

func getValidDir(dir string) (realPath string, err error) {
	realPath, fi, err := utils.GetRealPath(dir)
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
