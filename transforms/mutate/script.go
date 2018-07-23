package mutate

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Script{}
	_ transforms.Transformer      = &Script{}
	_ transforms.Initializer      = &Script{}
)

type Script struct {
	Key         string `json:"key"`
	New         string `json:"new"`
	Interpreter string `json:"interprepter"`
	ScriptPath  string `json:"scriptpath"`
	Script      string `json:"script"`
	storePath   string
	stats       StatsInfo
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
	return nil
}

func (g *Script) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	// 获取 keys
	keysArr := strings.Split(g.Key, ",")
	keysDetail := make([][]string, 0)
	for _, key := range keysArr {
		key = strings.TrimSpace(key)
		keys := GetKeys(key)
		if len(keys) <= 0 {
			continue
		}
		keysDetail = append(keysDetail, keys)
	}
	// 获取新建keys
	news := GetKeys(g.New)
	// 增加error
	newsErr := g.New + "_error"
	recordErrs := GetKeys(newsErr)

	if g.storePath == "" {
		g.storePath = g.ScriptPath
	}
	scriptPath, err := checkPath(g.storePath)
	if err != nil {
		g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
		return datas, fmtErr
	}

	for i := range datas {
		var scriptRes string
		var getErr error
		params := []string{scriptPath}
		for _, keys := range keysDetail {
			val, getErr := GetMapValue(datas[i], keys...)
			if getErr != nil {
				errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
				continue
			}

			valStr, ok := val.(string)
			if !ok {
				typeErr := fmt.Errorf("transform key %v data type is not string", g.Key)
				errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
				continue
			}
			params = append(params, valStr)
		}

		getErr = nil
		scriptRes, getErr = getScriptRes(g.Interpreter, params)
		if getErr != nil {
			if len(getErr.Error()) > 0 {
				// 设置脚本执行结果的错误信息
				setErr := SetMapValue(datas[i], getErr.Error(), false, recordErrs...)
				if setErr != nil {
					errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
				}
			}
			continue
		}

		// 设置脚本执行结果
		setErr := SetMapValue(datas[i], scriptRes, false, news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func getScriptRes(interpreter string, params []string) (string, error) {
	command := exec.Command(interpreter, params...) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s %s - run script err info is: %v", interpreter, params, err)
	}

	return string(res), nil
}

func getScriptResFromCmd(script string) (string, error) {
	cmdArr := getCmd(script)
	command := exec.Command(cmdArr[0], cmdArr[1:]...) //初始化Cmd

	res, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s - run script err info is %v", script, err)
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
	realPath, fileInfo, err := GetRealPath(path)
	if err != nil || fileInfo == nil {
		return "", fmt.Errorf("%s - GetRealPath failed, err:%v", path, err)
	}

	fileMode := fileInfo.Mode()
	if !fileMode.IsRegular() {
		return "", fmt.Errorf("%s - file failed, err: file is not regular", path)
	}
	CheckFileMode(realPath, fileMode)
	return realPath, nil
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
