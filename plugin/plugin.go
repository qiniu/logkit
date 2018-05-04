package plugin

import (
	"github.com/qiniu/log"
	"github.com/toolkits/file"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"

	"os"
	"sync"

	"bytes"
	"github.com/toolkits/sys"
	"os/exec"
	//"syscall"
	"encoding/json"
	"fmt"
	. "github.com/qiniu/logkit/utils/models"
	"time"
)

type Plugin struct {
	Type         string
	Path         string //插件路径
	MTime        int64  //修改时间
	DefaultCycle int    //默认运行周期
	ExecFile     string //可执行文件名称
	ConfDir      string //配置文件名称
}

type Config struct {
	Enabled      bool   `json:"enabled"`      //是否禁用plugin
	Dir          string `json:"dir"`          //plugin路径
	RemoteSource string `json:"remoteSource"` //plugin 远程地址
	//LogDir  		string `json:"logs"`	  //plugin日志输出路径
}

var (
	Plugins map[string]*Plugin
	Conf    *Config
	Lock    = new(sync.RWMutex)
	confDir = "conf"
)

func ListPlugins() map[string]Plugin {
	ps := map[string]Plugin{}
	for k, v := range Plugins {
		ps[k] = *v
	}
	return ps
}

//同步插件
func SyncPlugins() error {
	Lock.Lock()
	if !Conf.Enabled {
		return fmt.Errorf("plugin is not allowed")
	}
	plugins := make(map[string]*Plugin)

	dir := Conf.Dir
	if dir == "" {
		dir = "./plugins"
		log.Debugf("No plugin directory specified , use default plugin directory %v", dir)
	}

	if !file.IsExist(dir) || file.IsFile(dir) {
		return fmt.Errorf("the directory %v is not exist", dir)
	}

	//plugins路径
	pluginFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("can not list files under", dir)
		return fmt.Errorf("can not list files under %v", dir)
	}

	for _, pluginFile := range pluginFiles {
		if !pluginFile.IsDir() {
			continue
		}

		fileName := pluginFile.Name()

		pluginPath := filepath.Join(dir, fileName)
		fs, err := ioutil.ReadDir(pluginPath)
		if err != nil {
			log.Println("can not list files under", pluginPath)
			return fmt.Errorf("can not list files under %v", pluginPath)
		}
		confExist := false
		var fName string
		var cycle int
		var modTime int64
		var exeFile string
		for _, f := range fs {
			fName = f.Name()
			//判断为否文配置文件文件夹
			if f.IsDir() {
				if fName == confDir {
					confExist = true
				}
				continue
			}
			//判断是否为符合规范的可执行文件($cycle_$xx)
			arr := strings.Split(fName, "_")
			if len(arr) < 2 {
				continue
			}
			// filename should be: $cycle_$xx
			cycle, err = strconv.Atoi(arr[0])
			if err != nil {
				continue
			}
			exeFile = fName
			modTime = f.ModTime().Unix()
		}
		if !confExist {
			confFile := filepath.Join(pluginPath, confDir)
			os.Mkdir(confFile, os.ModePerm)
		}
		p := &Plugin{Type: fileName, Path: pluginPath, MTime: modTime, DefaultCycle: cycle, ExecFile: exeFile, ConfDir: confDir}
		plugins[fileName] = p
	}
	Plugins = plugins
	Lock.Unlock()
	return nil
}

func PluginRun(plugin *Plugin, configFile string, logPath string, Cycle int) (metrics []Data, err error) {

	timeout := Cycle*1000 - 500
	exePath := filepath.Join(plugin.Path, plugin.ExecFile)

	if !file.IsExist(exePath) {
		return nil, fmt.Errorf("no executable file error :%v", exePath)
	}

	log.Debugf(exePath, " running...")

	var cmd *exec.Cmd
	if logPath != "" {
		cmd = exec.Command(exePath, "-f", configFile, "-l", logPath)
	} else {
		cmd = exec.Command(exePath, "-f", configFile)
	}

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	//linux
	//cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Start()
	log.Debugf("plugin started: ", exePath)

	err, isTimeout := sys.CmdRunWithTimeout(cmd, time.Duration(timeout)*time.Millisecond)

	if isTimeout {
		// has be killed
		if err == nil {
			err = fmt.Errorf("plugin %v run timeout and has been killed successfully", exePath)
		}

		if err != nil {
			err = fmt.Errorf("plugin %v run timeout with err %v and has been killed successfully", exePath, err.Error())
		}
		return
	}

	if err != nil {
		err = fmt.Errorf("plugin %v run failed with err %v and has been killed successfully", exePath, err.Error())
		return
	}

	// exec successfully
	data := stdout.Bytes()
	if len(data) == 0 {
		err = fmt.Errorf("plugin %v stdout is blank", exePath)
		return
	}

	//jsonStr := string(data)
	//fmt.Println(jsonStr)

	err = json.Unmarshal(data, &metrics)
	if err != nil {
		err = fmt.Errorf("json.Unmarshal stdout of %s fail. error:%s stdout: \n%s\n", exePath, err, stdout.String())
		return nil, err
	}
	return
}

func DeletePlugins(newPlugins map[string]*Plugin) {
	for currKey, currPlugin := range Plugins {
		newPlugin, ok := newPlugins[currKey]
		if !ok || currPlugin.MTime != newPlugin.MTime {
			deletePlugin(currKey)
		}
	}
}

func AddPlugins(newPlugins map[string]*Plugin) {

}

func ClearAllPlugins() {
	for k := range Plugins {
		deletePlugin(k)
	}
}

func deletePlugin(key string) {

}
