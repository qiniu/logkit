package conf

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"

	"github.com/qiniu/log"
)

var (
	confName *string
	NL       = []byte{'\n'}
	ANT      = []byte{'#'}
)

var homeEnvNames = [][]string{
	{"HOME"},
	{"HOMEDRIVE", "HOMEPATH"},
}

var ErrHomeNotFound = errors.New("$HOME not found")

func getEnv(name []string) (v string) {
	if len(name) == 1 {
		return os.Getenv(name[0])
	}
	for _, k := range name {
		v += os.Getenv(k)
	}
	return v
}

func GetConfigDir(app string) (dir string, err error) {
	for _, name := range homeEnvNames {
		home := getEnv(name)
		if home == "" {
			continue
		}
		dir = home + "/." + app
		return dir, os.MkdirAll(dir, 0700)
	}
	return "", ErrHomeNotFound
}

func Init(cflag, app, defaultConf string) {
	confDir, _ := GetConfigDir(app)
	confName = flag.String(cflag, confDir+"/"+defaultConf, "the config file")
}

func Load(conf interface{}) error {
	if !flag.Parsed() {
		flag.Parse()
	}

	log.Info("Use the config file of ", *confName)
	return LoadEx(conf, *confName)
}

func trimComments(data []byte) (data1 []byte) {
	confLines := bytes.Split(data, NL)
	for k, line := range confLines {
		confLines[k] = trimCommentsLine(line)
	}
	return bytes.Join(confLines, NL)
}

func trimCommentsLine(line []byte) []byte {
	var newLine []byte
	var i, quoteCount int
	lastIdx := len(line) - 1
	for i = 0; i <= lastIdx; i++ {
		if line[i] == '\\' {
			if i != lastIdx && (line[i+1] == '\\' || line[i+1] == '"') {
				newLine = append(newLine, line[i], line[i+1])
				i++
				continue
			}
		}
		if line[i] == '"' {
			quoteCount++
		}
		if line[i] == '#' {
			if quoteCount%2 == 0 {
				break
			}
		}
		newLine = append(newLine, line[i])
	}
	return newLine
}

func LoadEx(conf interface{}, confName string) error {
	data, err := ioutil.ReadFile(confName)
	if err != nil {
		return err
	}

	data = trimComments(data)
	if err = json.Unmarshal(data, conf); err != nil {
		log.Errorf("Parse conf %v failed: %v", string(data), err)
		return err
	}
	return nil
}

func LoadFile(conf interface{}, confName string) (err error) {
	data, err := ioutil.ReadFile(confName)
	if err != nil {
		return
	}
	data = trimComments(data)

	return json.Unmarshal(data, conf)
}
